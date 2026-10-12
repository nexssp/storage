# nexssp/storage — Flow bundle

This package (`nexssp/storage/nexssflow/`) is the Flow bundle that
bridges the `nexssp/storage` adapter registry to `.nflow` programs.
It is loaded via `@require` — no Flow checkout changes are needed.

## Install

```nflow
@require github.com/nexssp/storage v0.1.0
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
```

Eight actions: `storage.kv.{get,set,del}`, `storage.blob.{get,put,del}`,
`storage.lock.{acquire,release}`. Each takes a `store:` argument
naming the `@storage` declaration.

### Layer 2 — Middleware wiring (the practical path)

```nflow
@storage kv:memory as cache { ttl: "5m" }

# Wire Kernel's CacheMiddleware to use the declared store.
# First call misses → handler runs → back-fills.
# Second call hits → handler does not run.
http.request:cached="cache:30s" @{ url: "https://api.example/users/1" }
```

Three modifiers, all wired by the `AtomAdvise` hook:

| Modifier | Wires | Value |
|---|---|---|
| `:cached="store:ttl"` | `CacheMiddleware` | `<store-name>:<duration>` |
| `:idempotent_store="store"` | `IdempotencyMiddleware` | `<store-name>` |
| `:dedup_store="store"` | `Dedup` | `<store-name>` |

The store name may contain colons (e.g. `kv:redis`); the TTL is
parsed from the segment after the last colon.

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

## Fixtures

```sh
nflow run nflows/basic.nflow          # KV set + get roundtrip
nflow run nflows/kv_miss.nflow        # Missing key returns hit=false
nflow run nflows/blob.nflow           # Blob put + get roundtrip
nflow run nflows/lock.nflow           # Lock acquire + release
nflow run nflows/alias.nflow          # `as` alias
nflow run nflows/read_through.nflow   # :cached= read-through pattern
nflow run nflows/idempotent.nflow     # :idempotent_store= pattern
nflow run nflows/dedup.nflow          # :dedup_store= pattern
```

## Tests

```sh
go test -race ./...
```

The advisor tests (`advise_test.go`) prove the middleware wiring
works end-to-end: a `:cached=` modifier causes the second call to be
served from cache with zero handler invocations.

## Why this lives in nexssp/storage/nexssflow/, not in flow/extensions/

Three reasons:

1. **Adapter packages depend on nexssp/storage, not on Flow.** A
   Redis adapter repo (`nexssp/storage-redis`) imports
   `github.com/nexssp/storage` for the `Backend` interface. If the
   Flow bundle lived inside `flow/extensions/storage/`, the Redis
   adapter would have to import Flow to get the bundle — backwards.

2. **One `@require` loads everything.** The user writes one line,
   not two. The bundle ships with the adapter contract; adding a new
   backend (Redis) is a separate `@require` of `nexssp/storage-redis`,
   not a Flow PR.

3. **Flow stays focused on language features.** `flow/extensions/loop`,
   `flow/extensions/on_error`, `flow/extensions/macros` are language
   features. Storage is an adapter — it belongs with the adapter
   contract, not with the grammar.

This matches the pattern documented in
[flow/extensions/README.md §External repositories](https://github.com/nexssp/flow/blob/main/extensions/README.md):
"Any repo can become a Flow extension by adding a `nexssflow/` package."

## License

Apache License 2.0.
