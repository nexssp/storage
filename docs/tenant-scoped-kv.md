# Tenant-scoped KV

KV backends can optionally isolate keys by tenant. Configure `tenant_scope: "tenant"` on a KV backend; supported adapters are Memory, SQLite, PostgreSQL, and Redis. When omitted, existing key behavior is unchanged.

```go
package example

import (
	"context"
	"time"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/storage"
	"github.com/nexssp/storage/memory"
)

func cacheInvoice(ctx context.Context, trustedTenantID string) error {
	backend := memory.NewKV()
	instance, err := backend.Open(map[string]string{"tenant_scope": "tenant"})
	if err != nil {
		return err
	}
	defer instance.Close()

	kv, err := storage.AssertKV(instance)
	if err != nil {
		return err
	}
	tenantCtx := xctx.WithTenantID(ctx, trustedTenantID)
	return kv.Set(tenantCtx, "invoice:42", "cached", time.Minute)
}
```

**Security:** The host must set `xctx.WithTenantID` from an authenticated, trusted tenant-resolution source. Do not populate the tenant ID from user-controlled headers, query parameters, or request bodies. If a caller can choose the tenant ID arbitrarily, tenant scoping does not provide isolation.

Every operation on a tenant-scoped store requires a tenant ID in the trusted request context. Missing tenant context returns an `*xerr.AppError` with kind `xerr.KindUnauthorized` via `storage.Unauthorized`; it never falls back to a shared key. This lets callers distinguish a missing tenant from infrastructure errors such as `Unavailable`. The tenant ID is supplied by the host's authentication/tenant-resolution layer. Storage does not resolve a tenant from user input, choose a tenant database, or provision databases.

Keys are isolated by tenant for `Get`, `Set`, `Delete`, and optional `AtomicKV.Update`. Different tenants may use the same logical key without seeing or overwriting one another's values. Callers inside the same tenant share the same KV entry; unlike BlobStore, tenant-scoped KV does not split entries by caller ID. BlobStore continues to require both authenticated caller and tenant scope.

This is key-level isolation for Storage KV, not a substitute for database authorization or tenant filtering in application-owned GORM repositories. Do not mix tenant-scoped and unscoped declarations over the same backend namespace. Existing unscoped KV values are not migrated or deleted when tenant scoping is enabled; they remain accessible only through an unscoped instance. Review namespace configuration and any data migration before enabling the option on a populated store.

The option is backend configuration, not a change to the Kernel `CacheLayer` interface. No SQL migration is required because it changes the derived KV key, not the table schema.
