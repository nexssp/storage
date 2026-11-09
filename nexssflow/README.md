# nexssp/storage — Flow bundle

This package (`nexssp/storage/nexssflow/`) is the Flow bundle that
bridges the `nexssp/storage` adapter registry to `.nflow` programs.
It is loaded via `@require` — no Flow checkout changes are needed.

## Install

```nflow
@require github.com/nexssp/storage v0.6.0
```

`@require` resolves to `github.com/nexssp/storage/nexssflow` (the
default `nexssflow` subdir convention). The bundle's `init()` calls
`core.Register("storage", Bundle)` and registers the `storage.events`
named hook.

## Three layers of API

### Layer 1 — Direct actions

```nflow
@storage kv:memory as cache { ttl: "5m" }

noop -> storage.kv.set @{ store: "cache", key: "k", value: "v" }
noop -> storage.kv.get @{ store: "cache", key: "k" }

# Optional: keep the current request available to the next action.
cache.kv.get @{ key: .tenant_id, request: . }
```

Eight actions: `storage.kv.{get,set,del}`, `storage.blob.{get,put,del}`,
`storage.lock.{acquire,release}`. Each takes a `store:` argument
naming the `@storage` declaration. Blob put accepts streamed input and
returns an `xfile.Ref`; Blob get is a metadata-only lookup returning
`{ ref, hit }`, never the payload bytes. All Blob operations require an
authenticated caller-and-tenant scope in the request context.

KV `get` accepts an optional `request` field. Pass `request: .` to echo the
current input in the result's `request` field alongside `value` and `hit`; this
lets downstream actions use both the loaded KV record and the original business
payload. The field is omitted unless explicitly supplied, preserving the
existing result shape for current callers.

### Layer 2 — Middleware wiring (the practical path)

```nflow
@storage kv:memory as cache { ttl: "5m" }

# Wire Kernel's CacheMiddleware to use the declared store.
# First call misses → handler runs → back-fills.
# Second call hits → handler does not run.
http.request:cache_store=cache:cache_ttl=30s @{ url: "https://api.example/users/1" }
```

Storage modifiers require a matching `@storage` declaration and are wired by the `AtomAdvise` hook. `:local_dedup` is process-local concurrent request coalescing and does not require a store:

| Modifier | Wires | Value |
|---|---|---|
| `:cache_store="store"` | `CacheMiddleware` | KV store alias |
| `:cache_ttl="duration"` | Cache TTL | Duration; requires `:cache_store` |
| `:idempotent_store="store"` | `IdempotencyMiddleware` | `<store-name>` |
| `:local_dedup` | `Dedup` | No value; same-process in-flight calls only |

If `:cache_ttl` is omitted, the declared KV store's `ttl` option is used, or five minutes when not set. A cache key is namespaced by action name and derived from the JSON representation of the request. Requests that cannot be JSON-encoded bypass caching/dedup rather than sharing a fallback key.

The aliased `idem.claim` action returns the stored `entry` when the claim state is `completed`, so a Flow can replay its status, body, and headers. `idem.complete` requires the original `request_hash`; its TTL can be set on the request, inherited from the store's `ttl` option, or defaults to Kernel's idempotency TTL.

`:idempotent_store` replays only when the host puts an explicit key in the execution context with Kernel's `xctx.WithIdempotencyKey`. `RequestID` is for correlation and does not enable replay by itself; plain Flow CLI fixtures therefore demonstrate wiring, not keyed replay.

### Transactional pipeline execution

Declare exactly one `tx` store to run the **whole Flow pipeline** in a transaction. SQL-backed KV and Blob operations participate when they use the same driver and `path`/`dsn` as the transaction store:

```nflow
@storage tx:sqlite as transaction { path: "./data/app.db" }
@storage kv:sqlite as customers { path: "./data/app.db" }

noop
  -> customers.kv.set @{ key: "customer-42", value: "Maks" }
  -> transaction.tx.run @{ step: "save-customer" }
  -> customers.kv.get @{ key: "customer-42" }
```

`tx.run` is an optional assertion that the transaction scope is active; it does not open a nested transaction. A pipeline error rolls back and success commits. A mismatched SQL database is rejected rather than used outside the transaction. Apply the adapter's embedded Goose migration explicitly before running a SQLite/PostgreSQL example. The in-memory `tx` backend is only a test double and does not provide atomicity or rollback. A pipeline can declare only one transaction store.

### Layer 3 — Hook observation

```nflow
@storage kv:memory as cache { ttl: "5m" }
@hook:storage.events

noop -> storage.kv.get @{ store: "cache", key: "k" }
```

The `storage.events` hook stamps a `*StorageEvents` accumulator into
the request context. Every `storage.*` action records
`cache_hit` / `cache_miss` / `error` events into it. Test fixtures
and observability probes read it via `HookProbeKey`.

## Directive syntax

```
@storage <kind>:<backend> [as <alias>] { options }
```

- `<kind>` — one of `kv`, `blob`, `lock`, `fenced_lock`, `idem`,
  `journal`, `tx`, `rate_limit`. Unknown kinds fail at compile time.
- `<backend>` — the adapter name (e.g. `memory`, `redis`, `s3`).
- `as <alias>` — optional. Renames the store for this compilation.
- `{ options }` — optional. Backend-specific key/value pairs.

For KV stores, `tenant_scope: "tenant"` enables per-tenant key isolation on supported backends. The host must place a tenant ID from trusted authentication/tenant resolution into the request context before the Flow runs; never derive it directly from user-controlled input. See [tenant-scoped KV](../docs/tenant-scoped-kv.md).

If a KV action runs without a tenant ID in the context, it fails with `xerr.KindUnauthorized` via `storage.Unauthorized`; there is no fallback to shared keys.

## Fixtures

```sh
nflow run nflows/basic.nflow          # KV set + get roundtrip
nflow run nflows/kv_miss.nflow        # Missing key returns hit=false
nflow run nflows/blob.nflow           # Blob put + get roundtrip
nflow run nflows/lock.nflow           # Lock acquire + release
nflow run nflows/alias.nflow          # `as` alias
nflow run nflows/read_through.nflow   # :cache_store= read-through pattern
nflow run nflows/idempotent.nflow     # :idempotent_store= pattern
nflow run nflows/dedup.nflow          # local :local_dedup pattern
```

## Tests

```sh
task test
task test -- ./nexssflow
task ci
```

The advisor tests (`advise_test.go`) prove the middleware wiring
works end-to-end: a `:cache_store=` modifier causes the second call to be
served from cache with zero handler invocations. Transaction integration
tests run a Flow pipeline against SQLite and verify both commit and rollback.

## Why this lives in nexssp/storage/nexssflow/, not in flow/extensions/

Three reasons:

1. **Adapter packages depend on nexssp/storage, not on Flow.** The
   SQLite, PostgreSQL, and Redis packages in this module import
   `github.com/nexssp/storage` for the `Backend` interface. If the
   Flow bundle lived inside `flow/extensions/storage/`, the Redis
   adapter would have to import Flow to get the bundle — backwards.

2. **One `@require` loads the built-ins.** The user writes one line,
   not two. The bundle registers memory, SQLite, PostgreSQL, and Redis
   backends. Additional third-party adapters are integrated by adding
   their registration to this bundle; no Flow repository change is needed.

3. **Flow stays focused on language features.** `flow/extensions/loop`,
   `flow/extensions/on_error`, `flow/extensions/macros` are language
   features. Storage is an adapter — it belongs with the adapter
   contract, not with the grammar.

This matches the pattern documented in
[flow/extensions/README.md §External repositories](https://github.com/nexssp/flow/blob/main/extensions/README.md):
"Any repo can become a Flow extension by adding a `nexssflow/` package."

## License

Apache License 2.0.
