// Package redis provides Redis-backed KV, Blob, and distributed lock
// implementations. Its default key prefix is versioned (nexss:storage:v1),
// so future incompatible key-layout changes can use an explicit migration.
package redis

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xfile"
	goredis "github.com/redis/go-redis/v9"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/internal/blobref"
)

const backendName = "redis"

type Backend struct{ kind storage.Kind }

func NewKV() *Backend   { return &Backend{kind: storage.KindKV} }
func NewBlob() *Backend { return &Backend{kind: storage.KindBlob} }
func NewLock() *Backend { return &Backend{kind: storage.KindLock} }

func (b *Backend) Name() string       { return backendName }
func (b *Backend) Kind() storage.Kind { return b.kind }

func (b *Backend) Open(opts map[string]string) (storage.Instance, error) {
	for key := range opts {
		switch key {
		case "addr", "username", "password", "db", "namespace", "ttl":
		default:
			return nil, storage.BadRequestf("redis: unknown option %q", key)
		}
	}
	if opts["addr"] == "" {
		return nil, storage.BadRequest("redis: \"addr\" is required")
	}
	dbNum := 0
	if opts["db"] != "" {
		parsed, err := strconv.Atoi(opts["db"])
		if err != nil || parsed < 0 {
			return nil, storage.BadRequestf("redis: invalid db %q", opts["db"])
		}
		dbNum = parsed
	}
	ttl, err := storage.ParseDuration(opts["ttl"], 0)
	if err != nil {
		return nil, err
	}
	prefix := strings.Trim(opts["namespace"], ":")
	if prefix == "" {
		prefix = "nexss:storage"
	}
	if !strings.HasSuffix(prefix, ":v1") {
		prefix += ":v1"
	}
	client := goredis.NewClient(&goredis.Options{
		Addr: opts["addr"], Username: opts["username"], Password: opts["password"], DB: dbNum,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, storage.Unavailablef(err, "redis: connect")
	}
	inst := &instance{client: client, kind: b.kind, prefix: prefix, defaultTTL: ttl, owners: make(map[string]string)}
	if b.kind == storage.KindBlob {
		return &blobInstance{instance: inst}, nil
	}
	return inst, nil
}

func Register(reg *storage.Registry) error {
	for _, backend := range []*Backend{NewKV(), NewBlob(), NewLock()} {
		if err := reg.Register(backend); err != nil {
			return err
		}
	}
	return nil
}

type instance struct {
	client     *goredis.Client
	kind       storage.Kind
	prefix     string
	defaultTTL time.Duration
	mu         sync.Mutex
	owners     map[string]string
	closeOnce  sync.Once
	closeErr   error
}

func (i *instance) Kind() storage.Kind { return i.kind }
func (i *instance) Close() error {
	i.closeOnce.Do(func() { i.closeErr = i.client.Close() })
	if i.closeErr != nil {
		return storage.Unavailablef(i.closeErr, "redis: close")
	}
	return nil
}

func (i *instance) key(key string) string {
	return i.prefix + ":" + base64.RawURLEncoding.EncodeToString([]byte(key))
}

func (i *instance) valueKey(key string) string {
	if i.kind == storage.KindBlob {
		return i.key("blob:" + key)
	}
	return i.key("kv:" + key)
}

func (i *instance) Get(ctx context.Context, key string) (value any, hit bool, resultErr error) {
	raw, hit, err := i.getRaw(ctx, key)
	if err != nil || !hit {
		return nil, hit, err
	}
	if decodeErr := json.Unmarshal(raw, &value); decodeErr != nil {
		return nil, false, storage.Internalf("redis: decode key %q: %v", key, decodeErr)
	}
	return value, true, nil
}

func (i *instance) getRaw(ctx context.Context, key string) (value []byte, hit bool, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	value, err := i.client.Get(ctx, i.valueKey(key)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, storage.Unavailablef(err, "redis: get key")
	}
	return append([]byte(nil), value...), true, nil
}

func (i *instance) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return storage.BadRequestf("redis: encode value: %v", err)
	}
	if ttl <= 0 {
		ttl = i.defaultTTL
	}
	if err := i.client.Set(ctx, i.valueKey(key), encoded, ttl).Err(); err != nil {
		return storage.Unavailablef(err, "redis: set key")
	}
	return nil
}

// Delete is an extension to CacheLayer exposed for Flow's kv.del action.
func (i *instance) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := i.client.Del(ctx, i.valueKey(key)).Err(); err != nil {
		return storage.Unavailablef(err, "redis: delete key")
	}
	return nil
}

type blobInstance struct{ *instance }

func (i *blobInstance) metadataKey(key string) string {
	return i.key("blobmeta:" + key)
}

func redisValueBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case []byte:
		return v, true
	case string:
		return []byte(v), true
	default:
		return nil, false
	}
}

func (i *blobInstance) Get(ctx context.Context, key string) (xfile.Ref, bool, error) {
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, false, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return xfile.Ref{}, false, err
	}
	storageKey := blobref.ScopedKey(scope, key)
	raw, err := i.client.Get(ctx, i.metadataKey(storageKey)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return xfile.Ref{}, false, nil
	}
	if err != nil {
		return xfile.Ref{}, false, storage.Unavailablef(err, "redis: lookup blob")
	}
	var meta blobref.Metadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return xfile.Ref{}, false, storage.Internalf("redis: decode blob metadata: %v", err)
	}
	if !blobref.Authorized(scope, meta) {
		return xfile.Ref{}, false, nil
	}
	if err := blobref.ValidateMetadata(meta); err != nil {
		return xfile.Ref{}, false, err
	}
	return blobref.NewRef(backendName, key, meta), true, nil
}

func (i *blobInstance) Put(ctx context.Context, key string, src io.Reader) (xfile.Ref, error) {
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return xfile.Ref{}, err
	}
	if src == nil {
		return xfile.Ref{}, storage.BadRequest("blob: source reader is required")
	}
	data, err := io.ReadAll(src)
	if err != nil {
		return xfile.Ref{}, storage.BadRequestf("blob: read source: %v", err)
	}
	meta := blobref.MetadataFor(scope, data)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return xfile.Ref{}, ctxErr
	}
	storageKey := blobref.ScopedKey(scope, key)
	rawMeta, err := json.Marshal(meta)
	if err != nil {
		return xfile.Ref{}, storage.Internalf("redis: encode blob metadata: %v", err)
	}
	_, err = i.client.TxPipelined(ctx, func(pipe goredis.Pipeliner) error {
		pipe.Set(ctx, i.valueKey(storageKey), data, 0)
		pipe.Set(ctx, i.metadataKey(storageKey), rawMeta, 0)
		return nil
	})
	if err != nil {
		return xfile.Ref{}, storage.Unavailablef(err, "redis: put blob")
	}
	return blobref.NewRef(backendName, key, meta), nil
}

func (i *blobInstance) Open(ctx context.Context, ref xfile.Ref) (io.ReadCloser, xfile.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, xfile.Info{}, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	key, err := blobref.Key(backendName, ref)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	storageKey := blobref.ScopedKey(scope, key)
	result, err := i.client.Eval(ctx, `
		local raw = redis.call('GET', KEYS[2])
		if not raw then return nil end
		local meta = cjson.decode(raw)
		if meta.caller_id ~= ARGV[1] or meta.tenant_id ~= ARGV[2] then return nil end
		local data = redis.call('GET', KEYS[1])
		if not data then return nil end
		return {data, raw}
	`, []string{i.valueKey(storageKey), i.metadataKey(storageKey)}, scope.CallerID, scope.TenantID).Result()
	if err != nil {
		return nil, xfile.Info{}, storage.Unavailablef(err, "redis: atomically resolve blob")
	}
	if result == nil {
		return nil, xfile.Info{}, storage.NotFound("blob: reference not found")
	}
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return nil, xfile.Info{}, storage.Internal("redis: malformed result while opening blob")
	}
	data, ok := redisValueBytes(values[0])
	if !ok {
		return nil, xfile.Info{}, storage.Internal("redis: malformed blob bytes while opening blob")
	}
	rawMeta, ok := redisValueBytes(values[1])
	if !ok {
		return nil, xfile.Info{}, storage.Internal("redis: malformed metadata while opening blob")
	}
	var meta blobref.Metadata
	if decodeErr := json.Unmarshal(rawMeta, &meta); decodeErr != nil {
		return nil, xfile.Info{}, storage.Internalf("redis: decode blob metadata: %v", decodeErr)
	}
	size, digest, err := blobref.ConfirmStored(data, meta)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	if err := blobref.Verify(ref, size, digest); err != nil {
		return nil, xfile.Info{}, err
	}
	return io.NopCloser(bytes.NewReader(data)), blobref.Info(ref, size, digest), nil
}

func (i *blobInstance) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return err
	}
	storageKey := blobref.ScopedKey(scope, key)
	_, err = i.client.Eval(ctx, `
		local raw = redis.call('GET', KEYS[2])
		if not raw then return 0 end
		local ok, meta = pcall(cjson.decode, raw)
		if not ok then return redis.error_reply('invalid blob metadata') end
		if meta.caller_id ~= ARGV[1] or meta.tenant_id ~= ARGV[2] then return 0 end
		redis.call('DEL', KEYS[1], KEYS[2])
		return 1
	`, []string{i.valueKey(storageKey), i.metadataKey(storageKey)}, scope.CallerID, scope.TenantID).Result()
	if err != nil {
		return storage.Unavailablef(err, "redis: delete blob")
	}
	return nil
}

func (i *instance) TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, storage.BadRequest("redis lock: ttl must be positive")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return false, storage.Internalf("redis lock: create owner token: %v", err)
	}
	token := base64.RawURLEncoding.EncodeToString(random[:])
	redisKey := i.key("lock:" + key)
	ok, err := i.client.SetNX(ctx, redisKey, token, ttl).Result()
	if err != nil {
		return false, storage.Unavailablef(err, "redis: acquire lock")
	}
	if ok {
		i.mu.Lock()
		i.owners[key] = token
		i.mu.Unlock()
	}
	return ok, nil
}

func (i *instance) Unlock(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	i.mu.Lock()
	token, ok := i.owners[key]
	delete(i.owners, key)
	i.mu.Unlock()
	if !ok {
		return nil
	}
	const unlock = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`
	if err := i.client.Eval(ctx, unlock, []string{i.key("lock:" + key)}, token).Err(); err != nil {
		return storage.Unavailablef(err, "redis: release lock")
	}
	return nil
}

var (
	_ action.CacheLayer[any] = (*instance)(nil)
	_ storage.BlobStore      = (*blobInstance)(nil)
	_ xfile.Resolver         = (*blobInstance)(nil)
	_ action.Mutex           = (*instance)(nil)
)
