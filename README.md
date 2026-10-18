# Nexss Storage

<small>Nexss Flow 🌀 ready</small>

<!-- project-status -->

[![CI](https://github.com/nexssp/storage/actions/workflows/ci.yml/badge.svg)](https://github.com/nexssp/storage/actions/workflows/ci.yml)
[![Latest tag](https://img.shields.io/github/v/tag/nexssp/storage?sort=semver&label=latest%20tag)](https://github.com/nexssp/storage/tags)
[![Go Reference](https://pkg.go.dev/badge/github.com/nexssp/storage.svg)](https://pkg.go.dev/github.com/nexssp/storage)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

<!-- /project-status -->

Nexss Storage defines a small adapter contract for storage capabilities used by [Nexss Kernel](https://github.com/nexssp/kernel), with memory, SQLite, PostgreSQL, and Redis implementations. Its [Nexss Flow](https://github.com/nexssp/flow) bundle declares stores in `.nflow` source and exposes them as checked actions and middleware.

> **Scope:** This module is not a general SQL query layer and does not create or upgrade database schemas on `Open`, Flow startup, or ordinary requests. Apply SQLite/PostgreSQL adapter migrations explicitly with Goose v3 before using those backends.

## Architecture

```
.nflow program / Go application
              │
              ▼
       storage.Registry
       ├── memory reference stores
       ├── SQLite: kv, blob, tx
       ├── PostgreSQL: kv, blob, tx
       └── Redis: kv, blob, lock
              │
              ▼
       Nexss Kernel interfaces
       Cache · Blob · Lock · Idempotency · Journal · Tx · Rate limit
```

## Install

Requires **Go 1.26.9 or newer**.

```
go get github.com/nexssp/storage@latest
```

## Storage kinds

Each `Kind` maps to a Kernel interface, so consumers can use the capability appropriate to a declaration:

| Kind | Contract | Typical use |
| --- | --- | --- |
| `kv` | `action.CacheLayer[V]` | Typed values with TTL |
| `blob` | `storage.BlobStore` | Objects and files addressed by typed `xfile.Ref` handles |
| `lock` | `action.Mutex` | Basic distributed lock |
| `fenced_lock` | `action.FencedMutex` | Lock lease with a monotonic fencing token |
| `idem` | `action.IdempotencyStore` | Idempotent replay and claims |
| `journal` | `action.Journal` | Durable-action step memoization |
| `tx` | `action.TxRunner` | Transaction boundaries |
| `rate_limit` | `action.RateLimiter` | Shared token-bucket admission control |

Memory provides a reference implementation for all eight kinds. Persistent backend support is:

| Package | Kinds | Main options |
| --- | --- | --- |
| [`sqlite`](sqlite/) | `kv`, `blob`, `tx` | `path`, optional `namespace`, KV `ttl` |
| [`postgres`](postgres/) | `kv`, `blob`, `tx` | `dsn`, optional `namespace`, KV `ttl` |
| [`redis`](redis/) | `kv`, `blob`, `lock` | `addr`, optional credentials/database/namespace, KV `ttl` |

## Optional atomic KV updates

KV instances may additionally implement `storage.AtomicKV` (`storage.AssertAtomicKV`). `Update` serializes a single namespace/key read-modify-write and invokes its callback under the backend transaction/lock; callback errors do not persist a value. Expired values are misses. The callback TTL follows `Set` semantics (non-positive uses the backend default; no default means no expiry). SQLite and PostgreSQL SQL adapters support this capability; ordinary `CacheLayer` behavior is unchanged. In an ambient SQL transaction, a callback error rolls back only the update's savepoint and the surrounding transaction remains usable. SQLite defaults to a 5-second busy timeout for lock contention, so concurrent writers wait for the short transaction instead of failing immediately; an explicit `_busy_timeout`/`_timeout` in the driver DSN is respected.

## Nexss Flow 🌀 integration

The Flow bundle registers the built-in memory, SQLite, PostgreSQL, and Redis backends. Declare a store with `@storage <kind>:<backend> as <alias>`; the alias becomes a statically checked action namespace.

```
@require github.com/nexssp/storage
@storage kv:memory as cache { ttl: "5m" }
@assert: result.hit == true
@assert: result.value == "hello"

noop
  -> cache.kv.set @{ key: "greeting", value: "hello" }
  -> cache.kv.get @{ key: "greeting" }
```

The aliased action surfaces are `kv.{get,set,del}`, `blob.{put,get,del}`, `lock.{acquire,release}`, `fenced.{acquire,release}`, `idem.{claim,complete,release}`, `journal.{get,record}`, `tx.run`, and `rate.check`. The generic `storage.*` actions support KV, Blob, and Lock when a store is selected dynamically. The bundle also wires Kernel middleware through `:cache_store=<alias>` (optionally `:cache_ttl=<duration>`), `:idempotent_store=<alias>`, and `:dedup_store=<alias>`. Attach `@hook:storage.events` to collect cache hit/miss/error events; `storage.probe` is available to inspect the hook from Flow.

Blob reads return a compact `{ ref, hit }` result, not payload bytes. Blob operations require authenticated caller-and-tenant scope in the Flow execution context. Basic locks are best-effort and non-fenced.

### Transactional Flow pipelines

Exactly one `@storage tx:<backend>` declaration wraps the entire compiled Flow pipeline in `TxRunner.RunInTx`. SQL-backed KV and Blob operations in that pipeline participate in the transaction when their driver and `path`/`dsn` match the transaction declaration. A different SQL database is rejected rather than silently written outside the transaction. A Flow error rolls back the transaction; success commits it. `tx.run` is an optional in-pipeline assertion that the declared transaction is active; it does not start a nested transaction.

```nflow
@storage tx:sqlite as transaction { path: "./data/app.db" }
@storage kv:sqlite as customers { path: "./data/app.db" }

noop
  -> customers.kv.set @{ key: "customer-42", value: "Maks" }
  -> transaction.tx.run @{ step: "save-customer" }
  -> customers.kv.get @{ key: "customer-42" }
```

Apply the adapter's embedded Goose migration before executing this example; opening a backend never creates the schema. Only one transaction store may be declared per pipeline. The in-memory `tx` backend is a test double and does not provide rollback or atomicity. The Flow fenced-lock namespace still does not expose lease renewal, and the generic `storage.*` namespace is limited to KV, Blob, and Lock.

Runnable ecosystem examples now live in the [Nexss examples repository](https://github.com/nexssp/examples).

## Go registry API

Register a backend, open an instance from a `Declaration`, and use the kind-specific assertion to access its Kernel interface. The registry tracks opened instances; close it during host shutdown with a context deadline.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/memory"
)

func run() (err error) {
	ctx := context.Background()
	reg := storage.NewRegistry()
	reg.MustRegister(memory.NewKV())
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = errors.Join(err, reg.Close(closeCtx))
	}()

	inst, err := (storage.Declaration{
		Name: "cache", Backend: "memory", Kind: storage.KindKV,
		Options: map[string]string{"ttl": "5m"},
	}).Open(reg)
	if err != nil {
		return err
	}
	cache, err := storage.AssertKV(inst)
	if err != nil {
		return err
	}
	if err := cache.Set(ctx, "greeting", "hello", 0); err != nil {
		return err
	}
	value, hit, err := cache.Get(ctx, "greeting")
	if err != nil {
		return err
	}
	if !hit {
		return fmt.Errorf("cache miss")
	}
	fmt.Printf("%v\n", value)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

## Blob references and SQL migrations

Blob APIs return compact typed references rather than payload bytes. `Put` and `Open` use reader-based interfaces, while current memory, Redis, and SQL implementations buffer payloads internally. A reference is an opaque selector, not a filesystem path or authorization token.

SQLite and PostgreSQL adapter tables are created only by their embedded migrations. Run the migrations as an explicit deployment/setup step with Goose v3 and the dedicated `nexss_storage_goose_db_version` table; application-owned tables belong in a separate migration stream. `Backend.Open` only connects and never runs DDL. Redis uses a versioned keyspace (default prefix `nexss:storage:v1`).

- [Blob reference and stream semantics](docs/blob-references.md)

- [Migration standard](docs/migrations.md)

## Packages

| Package | Purpose |
| --- | --- |
| [`storage`](.) | `Kind`, `Backend`, `Instance`, `Declaration`, `Registry`, and capability assertions. |
| [`memory`](memory/) | In-memory reference backends for all storage kinds. |
| [`sqlite`](sqlite/), [`postgres`](postgres/), [`redis`](redis/) | Persistent backend adapters. |
| [`nexssflow`](nexssflow/) | `@storage`, aliased and generic actions, middleware wiring, events, and probes. |

## Development

Install [Task](https://taskfile.dev), then run the repository's test task or CI gate:

```
task test
task test -- ./memory
task ci
```

`task test` runs the short test suite; pass a package after `--` to limit it. `task ci` runs lint and uncached race tests, as defined in `Taskfile.yml`.

## Related projects

- [Nexss Kernel](https://github.com/nexssp/kernel) — storage interfaces and typed actions.

- [Nexss Flow](https://github.com/nexssp/flow) — Flow language and runtime.
