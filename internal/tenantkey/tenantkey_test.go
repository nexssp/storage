package tenantkey

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
)

func TestKeyRequiresTenant(t *testing.T) {
	_, err := Key(context.Background(), "record")
	if err == nil {
		t.Fatal("expected missing tenant context to fail")
	}
	if appErr := xerr.From(err); appErr == nil || appErr.Kind != xerr.KindUnauthorized {
		t.Fatalf("missing tenant error = %v, want %v", appErr, xerr.KindUnauthorized)
	}
}

func TestKeyScopesTenantsAndPreservesKeyBoundaries(t *testing.T) {
	ctxA := xctx.WithTenantID(context.Background(), "tenant-a")
	ctxB := xctx.WithTenantID(context.Background(), "tenant-b")
	keyA, err := Key(ctxA, "record")
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := Key(ctxB, "record")
	if err != nil {
		t.Fatal(err)
	}
	if keyA == keyB {
		t.Fatal("different tenants received the same scoped key")
	}
	if key, want := ScopedKey("żółć", "klucz"), "tenant:v1:8:żółć5:klucz"; key != want {
		t.Fatalf("UTF-8 key length must use bytes: got %q, want %q", key, want)
	}
	if ScopedKey("ab", "c") == ScopedKey("a", "bc") {
		t.Fatal("length-prefixed keys must preserve tenant/key boundaries")
	}
}
