# nexssp/storage

A universal adapter contract for pluggable storage backends, with a
Flow bundle that makes every store available as compile-time-checked
actions and middleware.

## What this is

One Go module (`github.com/nexssp/storage`) with three layers:

1. **Adapter contract** (`backend.go`, `blob.go`, `errors.go`) —
   `Kind`, `Backend`, `Instance`, `Registry`, `Declaration`.
2. **Reference memory backends** (`memory/`) — 8 backends, one per Kind.
3. **Flow bundle** (`nexssflow/`) — the `@storage` directive, two
   action forms (generic `storage.*` + aliased `<alias>.*`), the
   `storage.events` hook, the `storage.probe` action, and three
   middleware-wiring modifiers.

One `@require` loads everything:

```nflow
@require github.com/nexssp/storage v0.1.0
```

## The 8 Kinds

| Kind | Kernel interface | Use case |
|---|---|---|
| `kv` | `action.CacheLayer[V]` | Typed values with TTL |
| `blob` | `storage.BlobStore` | Opaque bytes — files, snapshots |
| `lock` | `action.Mutex` | Best-effort distributed lock |
| `fenced_lock` | `action.FencedMutex` | Monotonic-fence lock |
| `idem` | `action.IdempotencyStore` (+`Coordinator`) | Idempotent replay + claims |
| `journal` | `action.Journal` | Step memoization for `action.Durable` |
| `tx` | `action.TxRunner` | Relational / document transactions |
| `rate_limit` | `action.RateLimiter` | Distributed token-bucket |

## Four ways to use a declared store

### 1. Aliased actions (compile-time-checked, no `store:` argument)

```nflow
@storage kv:memory as cache { ttl: "5m" }

# The alias "cache" becomes the action namespace.
# If you typo "cache" or forget the @storage line, nflow lint fails.
cache.kv.set @{ key: "user:1", value: "alice" }
cache.kv.get @{ key: "user:1" }
cache.kv.del @{ key: "user:1" }
```

Each `@storage` declaration materializes as a set of actions named
`<alias>.<kind>.<op>`. The store name is baked into a closure — no
runtime `store:` argument, no string lookup.

### 2. Generic actions (dynamic store selection)

```nflow
@storage kv:memory as cache { }

# The generic form takes a store: argument. Useful when the store
# name is dynamic (e.g. selected by a routing expression).
storage.kv.get @{ store: "cache", key: "k" }
```

### 3. Middleware wiring (the practical path — bare identifiers)

```nflow
@storage kv:memory as cache { ttl: "5m" }
@storage idem:memory as idem { ttl: "1h" }
@storage lock:memory as locks { }

# No quotes, no strings — bare identifiers, compile-time-checked.
# :cache_store=cache resolves "cache" to the @storage declaration at
# advisor time. If the store doesn't exist, compilation fails.
http.request:cache_store=cache:cache_ttl=30s @{ url: "..." }
payment.charge:idempotent_store=idem @{ amount: 100 }
api.call:dedup_store=locks @{ ... }
```

Three modifiers, all unquoted:

| Modifier | Wires | Value |
|---|---|---|
| `:cache_store=<name>` + `:cache_ttl=<duration>` | `CacheMiddleware` | store name + TTL |
| `:idempotent_store=<name>` | `IdempotencyMiddleware` | store name |
| `:dedup_store=<name>` | `Dedup` | store name |

### 4. Hook observation

```nflow
@storage kv:memory as cache { }
@hook:storage.events

# Every cache.kv.get records cache_hit / cache_miss / error into the
# request context. storage.probe reads the accumulator.
cache.kv.get @{ key: "missing" }
-> storage.probe
# Result: { attached: true, hits: 0, misses: 1, errors: 0, events: [...] }
```

The `storage.events` hook is registered by `nexssflow`'s `init()`.
Attach it with `@hook:storage.events`. The `storage.probe` action
returns a summary `{ hits, misses, errors, events }` — assert on it
in fixtures:

```nflow
@assert: result.misses == 1
@assert: result.hits == 0
```

## Architecture

```
.nflow source
    │
    │ @require github.com/nexssp/storage v0.1.0
    │ @storage kv:redis as cache { addr: "..." }
    │ cache.kv.set @{ key: "k", value: "v" }
    │ http.request:cache_store=cache:cache_ttl=30s @{ url: "..." }
    │ @hook:storage.events
    │
    ▼
nexssp/storage/nexssflow/         ← the Flow bundle
    ├── library.go                Bundle() registers everything
    ├── directive.go              @storage parser
    ├── materialize.go            mounts <alias>.<kind>.<op> actions
    ├── aliased_actions.go        the aliased action builders
    ├── actions.go                generic storage.* actions
    ├── hooks.go                  storage.events hook + accumulator
    ├── probe.go                  storage.probe action
    ├── advise.go                 :cache_store= / :idempotent_store= / :dedup_store=
    └── nflows/                   conformance + showcase fixtures
    │
    ▼
nexssp/storage                    ← the adapter contract
    ├── backend.go                Kind, Backend, Instance, Registry
    ├── blob.go                   BlobStore (proposed Kernel promotion)
    ├── errors.go
    └── memory/memory.go          8 reference backends
    │
    ▼
nexssp/kernel                     ← unchanged
```
