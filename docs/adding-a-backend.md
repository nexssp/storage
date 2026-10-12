# Adding a storage backend

This guide walks through writing a new adapter for `nexssp/storage`.
A backend is a small Go package that:

1. Implements one Kernel storage interface (e.g. `action.CacheLayer[V]`).
2. Wraps it in a `storage.Backend` so the Registry can Open it.
3. Registers itself in `init()` so a Flow `@require` brings it in.

## 1. Pick the Kind

Every Backend declares exactly one `storage.Kind`. If your adapter
can serve multiple Kinds (e.g. Redis can do both KV and Lock),
register separate Backend instances for each Kind.

| Kind | Implements | Required methods |
|---|---|---|
| `kv` | `action.CacheLayer[any]` | `Get(ctx, key) (any, bool, error)`, `Set(ctx, key, val, ttl) error` |
| `blob` | `storage.BlobStore` | `Get`, `Put`, `Delete` |
| `lock` | `action.Mutex` | `TryLock(ctx, key, ttl) (bool, error)`, `Unlock(ctx, key) error` |
| `fenced_lock` | `action.FencedMutex` | `Acquire`, `Renew`, `Release` |
| `idem` | `action.IdempotencyStore` | `Get(ctx, key) (IdempotencyEntry, bool)`, `Set(ctx, key, entry, ttl)` |
| `journal` | `action.Journal` | `GetStep(ctx, execID, step) ([]byte, bool, error)`, `RecordStep(ctx, execID, step, []byte) error` |
| `tx` | `action.TxRunner` | `RunInTx(ctx, fn) error` |
| `rate_limit` | `action.RateLimiter` | `Allow(ctx, key) (bool, error)` |

For `idem`, you SHOULD also implement `action.IdempotencyCoordinator`
and optionally `action.IdempotencyLeaseRenewer` — the memory backend
does this; see `memory/memory.go` for the reference.

## 2. Create the package

```
nexssp-storage-redis/
├── go.mod                          # module github.com/nexssp/storage-redis
├── kv.go                           # CacheLayer[any] impl + Backend
├── lock.go                         # Mutex impl + Backend
├── fenced_lock.go                  # FencedMutex impl + Backend
├── idem.go                         # IdempotencyStore + Coordinator
└── redis_test.go
```

## 3. Write the Backend

```go
// kv.go
package redis

import (
    "context"
    "time"

    "github.com/nexssp/storage"
    "github.com/redis/go-redis/v9"
)

type kvBackend struct{}

func (kvBackend) Name() string       { return "redis" }
func (kvBackend) Kind() storage.Kind  { return storage.KindKV }

func (kvBackend) Open(opts map[string]string) (storage.Instance, error) {
    addr := opts["addr"]
    if addr == "" {
        return nil, storage.BadRequest("redis: addr is required")
    }
    client := redis.NewClient(&redis.Options{Addr: addr})
    return &kvInstance{client: client}, nil
}

type kvInstance struct {
    client *redis.Client
}

func (k *kvInstance) Kind() storage.Kind { return storage.KindKV }
func (k *kvInstance) Close() error       { return k.client.Close() }

func (k *kvInstance) Get(ctx context.Context, key string) (any, bool, error) {
    val, err := k.client.Get(ctx, key).Result()
    if err == redis.Nil {
        return nil, false, nil
    }
    if err != nil {
        return nil, false, storage.Unavailable("redis get failed", err)
    }
    return val, true, nil
}

func (k *kvInstance) Set(ctx context.Context, key string, val any, ttl time.Duration) error {
    if err := k.client.Set(ctx, key, val, ttl).Err(); err != nil {
        return storage.Unavailable("redis set failed", err)
    }
    return nil
}
```

## 4. Register in init()

Two options for registration:

**Option A: Process-wide registration** (simple, matches Flow's
`@require` model where the bundle's `init()` registers):

```go
// kv.go
func init() {
    storage.Default.Register(kvBackend{})
}
```

You'll need to add `storage.Default` (a package-level default
Registry) to nexssp/storage — currently the Registry is a struct
created per-Bundle. To support this pattern, the Flow extension's
Bundle can probe the default Registry for any Backends registered
after Bundle() returns.

**Option B: Bundle factory** (preferred — keeps Registry scoped to
the compilation, per AGENTS.md §7):

```go
// bundle.go
package redis

import (
    "github.com/nexssp/storage"
)

func Register(reg *storage.Registry) error {
    if err := reg.Register(kvBackend{}); err != nil { return err }
    if err := reg.Register(lockBackend{}); err != nil { return err }
    return reg.Register(fencedLockBackend{})
}
```

The Flow extension's Bundle() then calls `redis.Register(reg)` after
registering the memory backends. This requires the Flow extension to
import the adapter package, which happens via `@require`.

For v1 of nexssp/storage (this prototype), Option A is not yet
implemented. Use Option B by importing the adapter package from the
Flow extension's Bundle — the prototype's `library.go` documents
where to add the import.

## 5. Test conformance

Every backend MUST pass the conformance suite. The memory backend's
tests (`memory/memory_test.go`) are the reference — copy them and
swap `memory.KV{}` for your backend.

Conformance tests cover:
- Happy path (get/set, acquire/release, claim/complete)
- Missing key returns `(nil, false, nil)` — not an error
- TTL expiry
- Context cancellation propagates
- Concurrent use is safe (run with `-race`)
- Wrong-type assertion fails with a classified error

## 6. Document options

List every option key your backend accepts in a doc comment on the
Backend type. Unknown keys MUST be rejected at Open time with
`storage.BadRequest`:

```go
func (kvBackend) Open(opts map[string]string) (storage.Instance, error) {
    addr := opts["addr"]
    if addr == "" {
        return nil, storage.BadRequest("redis: 'addr' is required")
    }
    for k := range opts {
        switch k {
        case "addr", "ttl", "namespace", "password":
            // ok
        default:
            return nil, storage.BadRequestf("redis: unknown option %q", k)
        }
    }
    // ...
}
```

## 7. Ship

- `go.mod` declares `module github.com/nexssp/storage-redis`.
- `go.mod` requires `github.com/nexssp/storage v0.1.0`.
- The package is importable as `github.com/nexssp/storage-redis`.
- A user flow writes `@require github.com/nexssp/storage-redis v1.0.0`.
- The Flow extension's `@storage kv:redis { addr: ... }` resolves.

## Checklist before publishing

- [ ] Backend implements the Kernel interface (compile-time check with `var _ action.X = (*Y)(nil)`)
- [ ] Open validates opts and rejects unknown keys
- [ ] Open returns classified errors (use `storage.BadRequest`, `storage.Unavailable`, etc.)
- [ ] Close is idempotent
- [ ] All methods honor `ctx.Err()` on entry
- [ ] Conformance tests pass with `-race`
- [ ] No `fmt.Errorf` or `errors.New` escapes package boundaries
- [ ] README lists all accepted options
