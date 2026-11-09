package blobref

import (
	"context"
	"testing"

	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xfile"
)

func TestScopeRequiresCallerAndTenant(t *testing.T) {
	if _, err := Scope(context.Background()); err == nil {
		t.Fatal("scope without authenticated identity was accepted")
	}

	ctx := xctx.WithTenantID(xctx.WithUserID(context.Background(), "caller-1"), "tenant-1")
	scope, err := Scope(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if scope != (xfile.Scope{CallerID: "caller-1", TenantID: "tenant-1"}) {
		t.Fatalf("scope = %+v", scope)
	}

	meta := MetadataFor(scope, []byte("payload"))
	if !Authorized(scope, meta) {
		t.Fatal("owner scope was not authorized")
	}
	if Authorized(xfile.Scope{CallerID: "caller-2", TenantID: "tenant-1"}, meta) {
		t.Fatal("different caller was authorized")
	}
	if Authorized(xfile.Scope{CallerID: "caller-1", TenantID: "tenant-2"}, meta) {
		t.Fatal("different tenant was authorized")
	}
}
