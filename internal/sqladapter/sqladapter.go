// Package sqladapter contains the shared SQL implementation used by the
// SQLite and PostgreSQL adapters. It never creates or upgrades schema.
package sqladapter

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xfile"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/internal/blobref"
)

// Backend adapts one SQL driver and storage kind to the storage contract.
type Backend struct {
	name, driver, option string
	kind                 storage.Kind
}

// NewBackend constructs a backend. option is "path" for SQLite or "dsn"
// for PostgreSQL. SQL schemas must be upgraded separately with Goose.
func NewBackend(name, driver string, kind storage.Kind, option string) *Backend {
	return &Backend{name: name, driver: driver, kind: kind, option: option}
}

func (b *Backend) Name() string       { return b.name }
func (b *Backend) Kind() storage.Kind { return b.kind }

func (b *Backend) Open(opts map[string]string) (storage.Instance, error) {
	for key := range opts {
		if key != b.option && key != "namespace" && key != "ttl" {
			return nil, storage.BadRequestf("%s: unknown option %q", b.name, key)
		}
	}
	addr := opts[b.option]
	if addr == "" {
		return nil, storage.BadRequestf("%s: %q is required", b.name, b.option)
	}
	ttl, err := storage.ParseDuration(opts["ttl"], 0)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(b.driver, addr)
	if err != nil {
		return nil, storage.Unavailablef(err, "%s: open database", b.name)
	}
	if b.driver == "sqlite" && (addr == ":memory:" || addr == "file::memory:") {
		db.SetMaxOpenConns(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, storage.Unavailablef(err, "%s: connect database", b.name)
	}
	namespace := opts["namespace"]
	if namespace == "" {
		namespace = "default"
	}
	inst := &instance{db: db, name: b.name, kind: b.kind, namespace: namespace, defaultTTL: ttl, postgres: b.driver == "pgx"}
	if b.kind == storage.KindBlob {
		return &blobInstance{instance: inst}, nil
	}
	return inst, nil
}

type instance struct {
	db         *sql.DB
	name       string
	kind       storage.Kind
	namespace  string
	defaultTTL time.Duration
	postgres   bool
	closeOnce  sync.Once
	closeErr   error
}

func (i *instance) Kind() storage.Kind { return i.kind }
func (i *instance) Close() error {
	i.closeOnce.Do(func() { i.closeErr = i.db.Close() })
	if i.closeErr != nil {
		return storage.Unavailablef(i.closeErr, "sql storage: close database")
	}
	return nil
}

func (i *instance) placeholder(n int) string {
	if i.postgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func (i *instance) table() string {
	if i.kind == storage.KindBlob {
		return "nexss_storage_blobs"
	}
	return "nexss_storage_kv"
}

func (i *instance) Get(ctx context.Context, key string) (value any, hit bool, resultErr error) {
	raw, hit, err := i.getRaw(ctx, key)
	if err != nil || !hit {
		return nil, hit, err
	}
	if decodeErr := json.Unmarshal(raw, &value); decodeErr != nil {
		return nil, false, storage.Internalf("sql storage: decode key %q: %v", key, decodeErr)
	}
	return value, true, nil
}

func (i *instance) getRaw(ctx context.Context, key string) (raw []byte, hit bool, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	var expires sql.NullInt64
	q := "SELECT value, expires_at FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2)
	err := i.db.QueryRowContext(ctx, q, i.namespace, key).Scan(&raw, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, storage.Unavailablef(err, "sql storage: get key")
	}
	if expires.Valid && expires.Int64 <= time.Now().UnixNano() {
		//nolint:gosec // Table and placeholders come from fixed adapter metadata; all data is bound as parameters.
		if _, err := i.db.ExecContext(ctx, "DELETE FROM "+i.table()+" WHERE namespace="+i.placeholder(1)+" AND key="+i.placeholder(2), i.namespace, key); err != nil {
			return nil, false, storage.Unavailablef(err, "sql storage: delete expired key")
		}
		return nil, false, nil
	}
	return append([]byte(nil), raw...), true, nil
}

func (i *instance) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return storage.BadRequestf("sql storage: encode value: %v", err)
	}
	return i.put(ctx, key, raw, ttl)
}

func (i *instance) put(ctx context.Context, key string, raw []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = i.defaultTTL
	}
	var expires any
	if ttl > 0 {
		expires = time.Now().Add(ttl).UnixNano()
	}
	p1, p2, p3, p4 := i.placeholder(1), i.placeholder(2), i.placeholder(3), i.placeholder(4)
	//nolint:gosec // Table and placeholders come from fixed adapter metadata; all data is bound as parameters.
	q := "INSERT INTO " + i.table() + " (namespace,key,value,expires_at) VALUES (" + p1 + "," + p2 + "," + p3 + "," + p4 + ") ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at"
	if _, err := i.db.ExecContext(ctx, q, i.namespace, key, raw, expires); err != nil {
		return storage.Unavailablef(err, "sql storage: set key")
	}
	return nil
}

// Delete is an extension to CacheLayer exposed for Flow's kv.del action.
func (i *instance) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	//nolint:gosec // Table and placeholders come from fixed adapter metadata; all data is bound as parameters.
	q := "DELETE FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2)
	if _, err := i.db.ExecContext(ctx, q, i.namespace, key); err != nil {
		return storage.Unavailablef(err, "sql storage: delete key")
	}
	return nil
}

func (i *instance) Put(ctx context.Context, key string, data []byte) error {
	return i.put(ctx, key, data, 0)
}

type blobInstance struct{ *instance }

func (i *blobInstance) Get(ctx context.Context, key string) (xfile.Ref, bool, error) {
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, false, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return xfile.Ref{}, false, err
	}
	var meta blobref.Metadata
	q := "SELECT blob_size, digest, owner_user, owner_tenant FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2) + " AND owner_user=" + i.placeholder(3) + " AND owner_tenant=" + i.placeholder(4)
	err = i.db.QueryRowContext(ctx, q, i.namespace, key, scope.CallerID, scope.TenantID).Scan(&meta.Size, &meta.Digest, &meta.CallerID, &meta.TenantID)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !blobref.Authorized(scope, meta)) {
		return xfile.Ref{}, false, nil
	}
	if err != nil {
		return xfile.Ref{}, false, storage.Unavailablef(err, "sql storage: lookup blob")
	}
	if err := blobref.ValidateMetadata(meta); err != nil {
		return xfile.Ref{}, false, err
	}
	return blobref.NewRef(i.name, key, meta), true, nil
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
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, err
	}
	p1, p2, p3, p4 := i.placeholder(1), i.placeholder(2), i.placeholder(3), i.placeholder(4)
	p5, p6, p7, p8 := i.placeholder(5), i.placeholder(6), i.placeholder(7), i.placeholder(8)
	//nolint:gosec // Table and placeholders come from fixed adapter metadata; all data is bound as parameters.
	q := "INSERT INTO " + i.table() + " (namespace,key,value,blob_size,digest,owner_user,owner_tenant,expires_at) VALUES (" + p1 + "," + p2 + "," + p3 + "," + p4 + "," + p5 + "," + p6 + "," + p7 + "," + p8 + ") ON CONFLICT(namespace,owner_tenant,owner_user,key) DO UPDATE SET value=excluded.value, blob_size=excluded.blob_size, digest=excluded.digest, expires_at=excluded.expires_at"
	if _, err := i.db.ExecContext(ctx, q, i.namespace, key, data, meta.Size, meta.Digest, meta.CallerID, meta.TenantID, nil); err != nil {
		return xfile.Ref{}, storage.Unavailablef(err, "sql storage: put blob")
	}
	return blobref.NewRef(i.name, key, meta), nil
}

func (i *blobInstance) Open(ctx context.Context, ref xfile.Ref) (io.ReadCloser, xfile.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, xfile.Info{}, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	key, err := blobref.Key(i.name, ref)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	var data []byte
	var meta blobref.Metadata
	q := "SELECT value, blob_size, digest, owner_user, owner_tenant FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2) + " AND owner_user=" + i.placeholder(3) + " AND owner_tenant=" + i.placeholder(4)
	err = i.db.QueryRowContext(ctx, q, i.namespace, key, scope.CallerID, scope.TenantID).Scan(&data, &meta.Size, &meta.Digest, &meta.CallerID, &meta.TenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, xfile.Info{}, storage.NotFound("blob: reference not found")
	}
	if err != nil {
		return nil, xfile.Info{}, storage.Unavailablef(err, "sql storage: open blob")
	}
	if ownerErr := blobref.CheckOwner(scope, meta); ownerErr != nil {
		return nil, xfile.Info{}, ownerErr
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
	//nolint:gosec // Table and placeholders come from fixed adapter metadata; all data is bound as parameters.
	q := "DELETE FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2) + " AND owner_user=" + i.placeholder(3) + " AND owner_tenant=" + i.placeholder(4)
	if _, err := i.db.ExecContext(ctx, q, i.namespace, key, scope.CallerID, scope.TenantID); err != nil {
		return storage.Unavailablef(err, "sql storage: delete blob")
	}
	return nil
}

func (i *instance) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return storage.BadRequest("sql storage: transaction callback is required")
	}
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.Unavailablef(err, "sql storage: begin transaction")
	}
	txCtx := context.WithValue(ctx, txContextKey{}, tx)
	if err := fn(txCtx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return errors.Join(err, storage.Unavailablef(rbErr, "sql storage: rollback transaction"))
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return storage.Unavailablef(err, "sql storage: commit transaction")
	}
	return nil
}

type txContextKey struct{}

// TxFromContext returns the SQL transaction placed in a RunInTx callback's
// context. It lets application code share the transaction with its own SQL.
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(*sql.Tx)
	return tx, ok
}

var (
	_ action.CacheLayer[any] = (*instance)(nil)
	_ storage.BlobStore      = (*blobInstance)(nil)
	_ xfile.Resolver         = (*blobInstance)(nil)
	_ action.TxRunner        = (*instance)(nil)
)
