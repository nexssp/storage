# Nexss storage migration standard

This document defines the persistent schema and serialized-data conventions for storage adapters and applications using them.

## SQL migrations: SQLite and PostgreSQL only

- Use **Goose v3 SQL migrations** as the only supported SQL migration runner/file format for SQLite and PostgreSQL. Do not introduce Goose v2, a second migration tool, a custom runner, or ORM auto-DDL for these streams.
- SQL migration files are dialect-specific and live with the schema owner. Adapter-owned files live under that adapter's `migrations/<dialect>/` directory and may be embedded as an `fs.FS`; application-owned tables belong in the application's own migration directory.
- Use zero-padded, increasing versions and descriptive names, e.g. `00001_create_records.sql`. Include `-- +goose Up` and, when safely reversible, `-- +goose Down`. Prefer SQL; use Go migrations only when SQL cannot reasonably express the change.
- Keep PostgreSQL and SQLite changes semantically aligned where both are supported, while allowing dialect-specific SQL. Test using isolated test databases only; tests and local checks must not target a real/production database.
- Once released, never edit a migration. Add a forward migration and prefer expand/contract changes so old and new app versions can overlap during rollout.
- Keep an independent Goose version history for every independently owned migration stream. Goose's default `goose_db_version` table is shared within a database; if two streams both use version `00001` and share that history, one stream can make the other appear already applied. Use `nexss_storage_goose_db_version` for adapter-owned storage tables and a separate app-specific table such as `app_goose_db_version` for application tables.
- Run upgrades explicitly with Goose v3 in a one-off setup/deployment step, before application rollout, using a migration-capable role. `Backend.Open`, application startup, Flow startup, and ordinary requests **MUST NOT** create or upgrade schema. Storage core must not import Goose or invoke migrations.

Example application migration:

```sql
-- +goose Up
CREATE TABLE records (
    id TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- +goose Down
DROP TABLE records;
```

Run independent streams with distinct directories and version tables:

```sh
# Adapter-owned KV/blob tables (run only if the adapter is used).
goose -dir ./postgres/migrations/postgres \
  -table nexss_storage_goose_db_version \
  postgres "$DATABASE_URL" up

# Application-owned tables.
goose -dir ./db/migrations/postgres \
  -table app_goose_db_version \
  postgres "$DATABASE_URL" up
```

Use the corresponding SQLite adapter/app directories and Goose SQLite dialect for SQLite. `goose status` may be used as an explicit preflight; it does not replace reviewing the target, pending migrations, and permissions. The commands above are operator-invoked examples, not startup hooks.

## Adapter migrations in this module

The `sqlite` and `postgres` packages expose adapter-owned migration files through `Migrations()`. The embedded migrations create only this storage module's KV/blob tables. Blob rows include confirmed byte size and digest plus caller/tenant ownership metadata; ownership is also part of the blob key so equal logical keys in separate scopes remain isolated. Their initial table-creation migrations intentionally omit `Down` sections: dropping those tables later would destroy persisted data and is not a safe rollback. A migration command may use the embedded filesystem, or the source directory shown above; it must use Goose v3 and the adapter's dedicated `nexss_storage_goose_db_version` table. Application tables remain in the application's stream.

This module does **not** bundle or run Goose. Adapter `Open` methods only connect to the configured database. SQL storage operations fail until the required adapter tables have been created by an explicit migration step. Keep migration setup in a separate command/process; do not auto-upgrade at application startup.

## Redis keyspace versioning

Redis has no relational schema, so Goose and SQL migration files do not apply. Keep keys namespaced and version the key format, e.g. `nexss:records:v1:`. This module's default keyspace prefix is `nexss:storage:v1`; custom namespaces must keep a visible version suffix as well.

When a persistent key layout or serialization format changes incompatibly, use a compatible rollout or an explicit, idempotent backfill/upgrade step, then retire old keys only after old readers/writers are gone. Never flush a shared Redis database as a migration strategy. Do not add a separate version key unless a real incompatible data-format change requires one.

## Scope

This standard covers persistent SQL schemas and Redis key layouts. It does not replace Go module semantic-versioning, storage API compatibility rules, or backend-specific operations runbooks. A Flow `@storage` declaration opens a backend; it does not create application tables. The module does not currently provide a generic SQL query/exec API, a schema generator, or automatic migrations.

For the approved database and sandbox conventions, including Podman as the documented default and Docker as explicit opt-in, see [database and sandbox conventions](database-plan.md).
