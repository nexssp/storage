# Database and sandbox conventions

**Status: approved conventions for this storage module.** The storage/migration rules below are normative. The generic SQL action API and sandbox runtime integration remain unimplemented; this document does not claim otherwise.

## Current storage surface

- `sqlite` and `postgres` implement the storage module's KV, blob, and transaction contracts. Their adapter-owned tables are described by embedded Goose migration files.
- `redis` implements KV, blob, and lock contracts with a versioned keyspace prefix.
- SQL `Backend.Open` opens a connection only. It must not create or upgrade tables.
- The Flow bundle currently has no generic SQL query/exec API, application-model-to-DDL generator, or startup migration hook. An `@storage` declaration does not create application tables.

## PostgreSQL local example and defaults

The runnable [`examples/postgres/`](../examples/postgres/) quickstart includes a PostgreSQL 17 Compose service, bound to `127.0.0.1:54329`, with database/user `storage_dev` and a clearly marked demo-only password. Its matching DSN is `postgres://storage_dev:storage_dev_only_change_me@127.0.0.1:54329/storage_dev?sslmode=disable`. The port is loopback-only, and the password/TLS settings are **not suitable for shared or production databases**. Override `DATABASE_URL` in the Go example for other databases; use secret injection outside checked-in Flow source.

The example demonstrates the necessary order: start the local database, apply the adapter's Goose migration explicitly using `nexss_storage_goose_db_version`, then run the Go or Flow storage example. No adapter or Flow startup step performs migrations. See the example README for complete commands and cleanup guidance.

## SQL and migration ownership

Follow the [Nexss storage migration standard](migrations.md):

1. Use **Goose v3 SQL migrations only** for SQLite and PostgreSQL. Do not add a storage-specific runner or mix migration tools/formats.
2. Keep adapter-owned tables in the adapter migration stream and application-owned tables in the application's own stream.
3. Use different Goose version tables for independent streams: `nexss_storage_goose_db_version` for this module's adapter tables and an application-specific name such as `app_goose_db_version` for app tables.
4. Apply upgrades explicitly with a one-off deployment/setup command, before application rollout. `Backend.Open`, Flow startup, and ordinary requests must never run migrations implicitly.
5. Keep released migrations immutable; add forward migrations and use expand/contract changes for rolling deployments.

Example PostgreSQL commands (the Goose v3 CLI and matching driver must be available in the migration environment):

```sh
# Adapter-owned storage tables; omit this step if they are not used.
goose -dir ./postgres/migrations/postgres \
  -table nexss_storage_goose_db_version \
  postgres "$DATABASE_URL" up

# Application-owned schema; use the app's separate migration directory/table.
goose -dir ./db/migrations/postgres \
  -table app_goose_db_version \
  postgres "$DATABASE_URL" up
```

The embedded `Migrations()` filesystems are available to migration tooling; this module does not ship or invoke a Goose runner. For SQLite, use the corresponding SQLite adapter directory/dialect and an explicit local upgrade step. Never point these commands at a production database while testing this module.

There is **no generic SQL query/exec API yet**. Any future API needs parameter binding, dialect-specific behavior, a stable JSON-safe row shape, cancellation/limits, and transaction-scoped composition. It must not imply that migrations happen on startup. SQL DDL remains reviewed Goose files; a generic `db` schema tag/code generator is out of scope.

## Redis keyspace versioning

Redis does not use Goose or SQL migration files. Keep keys namespaced and version their serialized/key layout. The default storage prefix is `nexss:storage:v1`; custom namespaces must also retain an explicit version suffix. For an incompatible layout change, define an explicit backward-compatible rollout or idempotent backfill/upgrade (for example, a deliberate `v1` to `v2` transition). Never flush a shared Redis database as a migration strategy. Add a separate version key only when an actual incompatible format change requires one.

## Sandbox engine and migration presets

This section records the contract for a Flow sandbox that invokes explicit setup/deployment work; **the runtime integration is not implemented in this repository or in its pinned Flow dependency (`github.com/nexssp/flow v0.26.1`)**. The inspected dependency has no `sandbox.exec` action or container dispatch integration. Its `external.exec` action runs a host process and is not an isolated sandbox; do not use or describe it as one. Do not treat the design below as existing Flow CLI commands.

The storage bundle can open a PostgreSQL backend when the host provides a reachable DSN, but the current `@storage` declaration receives literal options at compile/preprocess time. It does not expand `${DATABASE_URL}` or receive credentials from a secret manager. A production Flow sandbox integration therefore also needs Flow-side runtime support and a trusted secret-injection/configuration path; this storage-only change does not add that integration. The exact local PostgreSQL example is in [`examples/postgres/`](../examples/postgres/).

### Engine selection and readiness

- Use **rootless Podman by default**. Docker is an explicit opt-in engine, not an automatic alternative.
- Keep engine selection independent of image preset and security profile. Never silently switch engines, run on the host, or fall back to in-process execution if container setup or launch fails.
- Before dispatch, check that the selected engine executable is present, its runtime is reachable, and the requested pinned image is available. Fail closed with an actionable setup error if any check fails.
- A local Podman setup check can be performed without Flow using:

  ```sh
  command -v podman
  podman --version
  podman info --format '{{.Host.Security.Rootless}}'  # must print true for the default
  podman image inspect "$IMAGE_REF"                 # IMAGE_REF must be digest-pinned
  ```

  For Docker, only after explicit opt-in, check `command -v docker`, `docker --version`, `docker info`, and the selected image. A check must never trigger an implicit fallback to the other engine.
- Setup pulls only the explicitly selected, immutable image reference. Runtime repeats the selected-engine and image-availability checks; a missing engine/image is an error, not a reason to bypass isolation.

### Safe named image presets

The runtime should expose a small allowlist of semantic preset names, each resolved by maintained configuration to an immutable `image@sha256:<digest>` reference. Never resolve a preset to mutable `latest`, accept arbitrary caller image strings by default, or install tools from the network at container startup.

| Preset | Contents and intended use |
|---|---|
| `minimal` | Small non-root base for offline tasks; no database client or compiler unless required. |
| `go-test` | Pinned Go toolchain/test dependencies for repository checks; no runtime package installation. |
| `goose-sqlite` | Pinned Goose v3 SQLite-capable migration tool for explicitly requested local SQLite upgrades. |
| `goose-postgres` | Pinned Goose v3 PostgreSQL-capable migration tool for explicitly requested PostgreSQL upgrades. |

The actual image registry and digests must be supplied by the Flow runtime maintainers; this repository does not invent or publish image references.

### Safe execution profiles

Profiles are policy bundles, separate from images. Baseline profiles run rootless, as a non-root UID, with a read-only image filesystem, dropped capabilities, no privileged mode, no host container socket, bounded CPU/memory/processes, and only explicitly mounted workspace paths. Network is disabled unless a narrowly scoped profile says otherwise.

| Profile | Allowed access |
|---|---|
| `offline` | Default: no network; only declared workspace mounts. |
| `go-test` | Offline test execution; source is read-only unless a specific output/cache mount is declared. |
| `sqlite-migration` | No network; only the explicitly selected SQLite file and migration/workspace paths are writable. |
| `postgres-migration` | Explicit target only: database egress restricted to the configured host and port; no general internet egress. Inject credentials at runtime, never into the image or logs. |

A PostgreSQL upgrade is still an explicit deployment action requiring a migration-capable role and the exact target to be visible before execution. The default offline profile must not be weakened globally to support it.

## Non-goals

- Do not change the Flow engine or add SQL actions as part of this storage conventions work.
- Do not run migrations during application startup or against a real database as part of tests.
- Do not add automatic schema generation or a generic SQL API in this scope.
- Do not claim a sandbox runtime check exists until the Flow-side engine/image/profile implementation provides it.
- Do not adopt the separate proposal's `CREATE TABLE IF NOT EXISTS` inside SQLite `Open`; that would make schema changes implicit. Stream-oriented BlobStore methods, metadata/URL support, and dynamic `@require` driver selection are useful future API questions, but require a separate compatibility and Flow-integration design.
