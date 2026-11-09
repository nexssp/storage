// Package tenantkey derives isolated KV keys from trusted tenant context.
package tenantkey

import (
	"context"
	"fmt"

	"github.com/nexssp/kernel/xctx"

	"github.com/nexssp/storage"
)

// Key returns a key scoped to the tenant ID in ctx, or an error when no tenant is present.
func Key(ctx context.Context, key string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	tenantID := xctx.TenantIDFrom(ctx)
	if tenantID == "" {
		return "", storage.Unauthorized("tenant context required")
	}
	return ScopedKey(tenantID, key), nil
}

// ScopedKey combines a tenant ID and logical key without delimiter ambiguity.
func ScopedKey(tenantID, key string) string {
	return fmt.Sprintf("tenant:v1:%d:%s%d:%s", len(tenantID), tenantID, len(key), key)
}
