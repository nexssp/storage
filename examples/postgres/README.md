# PostgreSQL quickstart

This example connects the storage module's PostgreSQL KV backend to a **local, disposable development database**. It includes working defaults so you can try the adapter without first designing a config file.

## Defaults

| Setting | Value |
|---|---|
| PostgreSQL image | `postgres:17` (PostgreSQL 17 series) |
| Host and port | `127.0.0.1:54329` (loopback only) |
| Database | `storage_dev` |
| User | `storage_dev` |
| Password | `storage_dev_only_change_me` (**local demo only**) |
| DSN | `postgres://storage_dev:storage_dev_only_change_me@127.0.0.1:54329/storage_dev?sslmode=disable` |
| Storage namespace | `demo` in the Go example; `flow-demo` in the Flow example |
| Default KV TTL | `5m` |

The Compose password and connection string are intentionally identical. **Do not reuse these credentials or `sslmode=disable` outside this loopback-only development example.** Set `DATABASE_URL` explicitly before running the Go example, and supply the appropriate DSN in Flow configuration. Keep real credentials out of source control.

## 1. Start PostgreSQL

From the repository root, use rootless Podman by default:

```sh
podman compose -f examples/postgres/compose.yaml up -d postgres
```

If Podman or its Compose provider is unavailable, Docker Compose is an explicit local alternative:

```sh
docker compose -f examples/postgres/compose.yaml up -d postgres
```

Wait for it to become healthy:

```sh
podman compose -f examples/postgres/compose.yaml ps
# or: docker compose -f examples/postgres/compose.yaml ps
```

The Compose service binds only to `127.0.0.1`; it is not exposed to other machines. The named volume persists local data across `compose down`. The default password is not safe for a shared or internet-reachable database.

## 2. Apply the adapter migration explicitly

The adapter never creates or upgrades tables from `Open`, Flow startup, or an ordinary storage call. With a Goose v3 CLI that includes the PostgreSQL driver, run this once before the examples:

```sh
export DATABASE_URL='postgres://storage_dev:storage_dev_only_change_me@127.0.0.1:54329/storage_dev?sslmode=disable'
goose -dir ./postgres/migrations/postgres \
  -table nexss_storage_goose_db_version \
  postgres "$DATABASE_URL" up
```

This creates only the module-owned KV and blob tables. It does **not** create application tables. Keep application migrations in the application's own directory and use a separate Goose version table, as described in [`docs/migrations.md`](../../docs/migrations.md).

This is an operator-invoked migration against the disposable local database. **Do not point this example command at production or another real database.** Review the target and pending migration before any non-demo use.

## 3. Run the Go example

From the repository root:

```sh
go run ./examples/postgres
```

It opens the `postgres` KV backend with namespace `demo` and a five-minute default TTL, writes a small value, reads it back, and prints the hit/result. Export the local demo DSN (or another connection string) first:

```sh
export DATABASE_URL='postgres://USER:PASSWORD@HOST:5432/DBNAME?sslmode=verify-full'
go run ./examples/postgres
```

The adapter options are `dsn` (required), `namespace` (default `default`), and `ttl` for KV. The adapter pings the database on open, so the database must be reachable and its adapter migration must already be applied.

## 4. Use it from Flow

[`postgres.nflow`](postgres.nflow) is a local-development example that declares the same default DSN and performs a KV write/read:

```sh
nflow run examples/postgres/postgres.nflow
```

It requires the published storage Flow bundle and the adapter migration from step 2. The current `v0.1.0` tag predates the PostgreSQL adapter, and these repository changes are intentionally uncommitted; the Flow sample becomes runnable via `@require` only after a release containing PostgreSQL support is published. Until then, use the Go example against this checkout. **The DSN in the Flow sample is a literal for the local demo only.** In the inspected Flow version, `@storage` options are read while the program is compiled; the storage bundle does not expand `${DATABASE_URL}` or inject secret-manager values into `dsn`. Do not put production credentials in a checked-in `.nflow` file. Supplying secrets to Flow storage declarations safely requires a host/runtime configuration path; that is not implemented by this repository today.

## Sandbox status

The PostgreSQL adapter can be used by a process that can reach its configured database. That is not the same as having a sandbox runner. The Flow version used by this module (`github.com/nexssp/flow v0.26.1`) has no `sandbox.exec` action or Podman/Docker dispatch integration. Its `external.exec` action invokes a host process and **must not be treated as isolated container execution**.

The documented future sandbox design uses rootless Podman by default, digest-pinned image presets, explicit mounts and network policy, and a narrow `postgres-migration` profile. Those are design requirements only—not available commands or runtime features here. A real integration needs Flow-side runner support plus a trusted way to inject DB credentials and restrict egress to the intended host/port. The example does not pretend to provide those missing capabilities.

If you run a process inside a container, `127.0.0.1` refers to that container, not the host or this Compose database. Configure an explicit reachable service/network address and allow only the required database port; do not weaken the default offline sandbox profile globally.

## Stop the local database

```sh
podman compose -f examples/postgres/compose.yaml down
# or: docker compose -f examples/postgres/compose.yaml down
```

This stops the service but retains its named data volume. Removing the volume destroys the local demo database.
