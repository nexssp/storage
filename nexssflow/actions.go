package nexssflow

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xfile"

	"github.com/nexssp/storage"
)

// currentStore resolves the per-compilation store from the execution
// context. The WrapPipeline hook installs it before the compiled
// program runs, so by the time any storage.* action executes the
// store is available.
func currentStore(ctx context.Context) (*store, error) {
	s, ok := storeFromContext(ctx)
	if !ok {
		return nil, storage.Internal(
			"storage: no store context; the WrapPipeline hook is missing")
	}
	return s, nil
}

// buildLibrary returns the single action.Library for this bundle. It
// contains the generic storage.* actions. Each action takes a `store`
// argument naming the @storage declaration to use.
//
// Every action records events into the request context (when the
// storage.events hook is attached via @hook:storage.events) so
// observability and test assertions work without additional wiring.
func buildLibrary() action.Library {
	return action.Library{
		Name: "storage",
		Actions: []action.AnyAction{
			// KV
			kvGetAction(),
			kvSetAction(),
			kvDelAction(),
			// Blob
			blobGetAction(),
			blobPutAction(),
			blobDelAction(),
			// Lock
			lockAcquireAction(),
			lockReleaseAction(),
			// Probe (reads accumulated hook events)
			probeAction(),
		},
	}
}

// ---------------------------------------------------------------------------
// KV actions — operate on action.CacheLayer[any]
//
// Each action records "cache_hit" / "cache_miss" / "error" events into
// the request context when the storage.events hook is attached. The
// event recording is a no-op when the hook is absent, so the hot path
// stays clean.
// ---------------------------------------------------------------------------

type kvGetReq struct {
	Store string `json:"store"`
	Key   string `json:"key"`
}

type kvGetRes struct {
	Value any  `json:"value"`
	Hit   bool `json:"hit"`
}

func kvGetAction() action.AnyAction {
	return action.New("storage.kv.get",
		func(ctx context.Context, req kvGetReq) (kvGetRes, error) {
			s, err := currentStore(ctx)
			if err != nil {
				return kvGetRes{}, err
			}
			inst, err := s.instance(req.Store)
			if err != nil {
				recordStorageEvent(ctx, "error", req.Store, "storage.kv.get", req.Key)
				return kvGetRes{}, err
			}
			layer, err := storage.AssertKV(inst)
			if err != nil {
				recordStorageEvent(ctx, "error", req.Store, "storage.kv.get", req.Key)
				return kvGetRes{}, err
			}
			val, hit, err := layer.Get(ctx, req.Key)
			if err != nil {
				recordStorageEvent(ctx, "error", req.Store, "storage.kv.get", req.Key)
				return kvGetRes{}, storage.Unavailable("storage.kv.get failed", err)
			}
			if hit {
				recordStorageEvent(ctx, "cache_hit", req.Store, "storage.kv.get", req.Key)
			} else {
				recordStorageEvent(ctx, "cache_miss", req.Store, "storage.kv.get", req.Key)
			}
			return kvGetRes{Value: val, Hit: hit}, nil
		}).
		Description("Get a value from a KV store; emits cache_hit/cache_miss events").
		Tag("storage", "kv", "get").
		Build()
}

type kvSetReq struct {
	Store string `json:"store"`
	Key   string `json:"key"`
	Value any    `json:"value"`
	TTL   string `json:"ttl,omitempty"`
}

type kvSetRes struct {
	OK bool `json:"ok"`
}

func kvSetAction() action.AnyAction {
	return action.New("storage.kv.set",
		func(ctx context.Context, req kvSetReq) (kvSetRes, error) {
			s, err := currentStore(ctx)
			if err != nil {
				return kvSetRes{}, err
			}
			inst, err := s.instance(req.Store)
			if err != nil {
				return kvSetRes{}, err
			}
			layer, err := storage.AssertKV(inst)
			if err != nil {
				return kvSetRes{}, err
			}
			ttl, err := storage.ParseDuration(req.TTL, 0)
			if err != nil {
				return kvSetRes{}, err
			}
			if err := layer.Set(ctx, req.Key, req.Value, ttl); err != nil {
				recordStorageEvent(ctx, "error", req.Store, "storage.kv.set", req.Key)
				return kvSetRes{}, storage.Unavailable("storage.kv.set failed", err)
			}
			return kvSetRes{OK: true}, nil
		}).
		Description("Set a value in a KV store").
		Tag("storage", "kv", "set").
		Build()
}

type kvDelReq struct {
	Store string `json:"store"`
	Key   string `json:"key"`
}

type kvDelRes struct {
	OK bool `json:"ok"`
}

func kvDelAction() action.AnyAction {
	return action.New("storage.kv.del",
		func(ctx context.Context, req kvDelReq) (kvDelRes, error) {
			s, err := currentStore(ctx)
			if err != nil {
				return kvDelRes{}, err
			}
			inst, err := s.instance(req.Store)
			if err != nil {
				return kvDelRes{}, err
			}
			layer, err := storage.AssertKV(inst)
			if err != nil {
				return kvDelRes{}, err
			}
			type deleter interface {
				Delete(ctx context.Context, key string) error
			}
			d, ok := layer.(deleter)
			if !ok {
				return kvDelRes{}, storage.BadRequestf(
					"storage.kv.del: backend %q does not support Delete", inst.Kind())
			}
			if err := d.Delete(ctx, req.Key); err != nil {
				recordStorageEvent(ctx, "error", req.Store, "storage.kv.del", req.Key)
				return kvDelRes{}, storage.Unavailable("storage.kv.del failed", err)
			}
			return kvDelRes{OK: true}, nil
		}).
		Description("Delete a key from a KV store").
		Tag("storage", "kv", "del").
		Build()
}

// ---------------------------------------------------------------------------
// Blob actions — operate on storage.BlobStore
// ---------------------------------------------------------------------------

type blobGetReq struct {
	Store string `json:"store"`
	Key   string `json:"key"`
}

type blobGetRes struct {
	Ref xfile.Ref `json:"ref"`
	Hit bool      `json:"hit"`
}

func blobGetAction() action.AnyAction {
	return action.New("storage.blob.get",
		func(ctx context.Context, req blobGetReq) (blobGetRes, error) {
			s, err := currentStore(ctx)
			if err != nil {
				return blobGetRes{}, err
			}
			inst, err := s.instance(req.Store)
			if err != nil {
				return blobGetRes{}, err
			}
			store, err := storage.AssertBlob(inst)
			if err != nil {
				return blobGetRes{}, err
			}
			ref, hit, err := store.Get(ctx, req.Key)
			if err != nil {
				return blobGetRes{}, err
			}
			return blobGetRes{Ref: ref, Hit: hit}, nil
		}).
		Description("Look up a blob and return its compact xfile.Ref handle, never its bytes").
		Tag("storage", "blob", "get").
		Build()
}

type blobPutReq struct {
	Store string `json:"store"`
	Key   string `json:"key"`
	Data  any    `json:"data"`
}

type blobPutRes struct {
	Ref xfile.Ref `json:"ref"`
	OK  bool      `json:"ok"`
}

func blobPutAction() action.AnyAction {
	return action.New("storage.blob.put",
		func(ctx context.Context, req blobPutReq) (blobPutRes, error) {
			s, err := currentStore(ctx)
			if err != nil {
				return blobPutRes{}, err
			}
			inst, err := s.instance(req.Store)
			if err != nil {
				return blobPutRes{}, err
			}
			store, err := storage.AssertBlob(inst)
			if err != nil {
				return blobPutRes{}, err
			}
			src, err := toReader(req.Data)
			if err != nil {
				return blobPutRes{}, err
			}
			ref, err := store.Put(ctx, req.Key, src)
			if err != nil {
				return blobPutRes{}, err
			}
			return blobPutRes{Ref: ref, OK: true}, nil
		}).
		Description("Stream a blob into storage and return its compact xfile.Ref handle").
		Tag("storage", "blob", "put").
		Build()
}

type blobDelReq struct {
	Store string `json:"store"`
	Key   string `json:"key"`
}

type blobDelRes struct {
	OK bool `json:"ok"`
}

// different Kind (Blob) and calls a different method (Delete).
func blobDelAction() action.AnyAction {
	return action.New("storage.blob.del",
		func(ctx context.Context, req blobDelReq) (blobDelRes, error) {
			s, err := currentStore(ctx)
			if err != nil {
				return blobDelRes{}, err
			}
			inst, err := s.instance(req.Store)
			if err != nil {
				return blobDelRes{}, err
			}
			store, err := storage.AssertBlob(inst)
			if err != nil {
				return blobDelRes{}, err
			}
			if err := store.Delete(ctx, req.Key); err != nil {
				return blobDelRes{}, err
			}
			return blobDelRes{OK: true}, nil
		}).
		Description("Delete a blob from a blob store").
		Tag("storage", "blob", "del").
		Build()
}

// ---------------------------------------------------------------------------
// Lock actions — operate on action.Mutex
// ---------------------------------------------------------------------------

type lockAcquireReq struct {
	Store string `json:"store"`
	Key   string `json:"key"`
	TTL   string `json:"ttl"`
}

type lockAcquireRes struct {
	Acquired bool `json:"acquired"`
}

func lockAcquireAction() action.AnyAction {
	return action.New("storage.lock.acquire",
		func(ctx context.Context, req lockAcquireReq) (lockAcquireRes, error) {
			s, err := currentStore(ctx)
			if err != nil {
				return lockAcquireRes{}, err
			}
			inst, err := s.instance(req.Store)
			if err != nil {
				return lockAcquireRes{}, err
			}
			m, err := storage.AssertMutex(inst)
			if err != nil {
				return lockAcquireRes{}, err
			}
			ttl, err := storage.ParseDuration(req.TTL, 30*time.Second)
			if err != nil {
				return lockAcquireRes{}, err
			}
			ok, err := m.TryLock(ctx, req.Key, ttl)
			if err != nil {
				return lockAcquireRes{}, storage.Unavailable("storage.lock.acquire failed", err)
			}
			return lockAcquireRes{Acquired: ok}, nil
		}).
		Description("Acquire a distributed lock (best-effort, non-fenced)").
		Tag("storage", "lock", "acquire").
		Build()
}

type lockReleaseReq struct {
	Store string `json:"store"`
	Key   string `json:"key"`
}

type lockReleaseRes struct {
	OK bool `json:"ok"`
}

// different Kind (Mutex) and calls a different method (Unlock).
func lockReleaseAction() action.AnyAction {
	return action.New("storage.lock.release",
		func(ctx context.Context, req lockReleaseReq) (lockReleaseRes, error) {
			s, err := currentStore(ctx)
			if err != nil {
				return lockReleaseRes{}, err
			}
			inst, err := s.instance(req.Store)
			if err != nil {
				return lockReleaseRes{}, err
			}
			m, err := storage.AssertMutex(inst)
			if err != nil {
				return lockReleaseRes{}, err
			}
			if err := m.Unlock(ctx, req.Key); err != nil {
				return lockReleaseRes{}, storage.Unavailable("storage.lock.release failed", err)
			}
			return lockReleaseRes{OK: true}, nil
		}).
		Description("Release a distributed lock").
		Tag("storage", "lock", "release").
		Build()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// toReader coerces a runtime value to an input stream. Existing readers stay
// streams; simple Flow values are converted to a byte reader.
func toReader(v any) (io.Reader, error) {
	switch x := v.(type) {
	case io.Reader:
		return x, nil
	case nil:
		return bytes.NewReader(nil), nil
	case []byte:
		return bytes.NewReader(x), nil
	case string:
		return bytes.NewReader([]byte(x)), nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, storage.BadRequestf("storage.blob: cannot encode value as bytes: %v", err)
		}
		return bytes.NewReader(data), nil
	}
}

// toBytes remains for byte-oriented journal actions; Blob actions use toReader.
func toBytes(v any) ([]byte, error) {
	reader, err := toReader(v)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, storage.BadRequestf("storage: read value: %v", err)
	}
	return data, nil
}
