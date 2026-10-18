package nexssflow

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/xctx"
	"github.com/nexssp/kernel/xerr"
	"github.com/nexssp/kernel/xfile"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/memory"
)

func TestWithBlobStoreFileResolverIsExplicitAndScoped(t *testing.T) {
	defaultBundle := Bundle(nil)
	if len(defaultBundle.FileResolvers) != 0 {
		t.Fatal("the standard Storage bundle unexpectedly enables file resolution")
	}

	instance, err := memory.NewBlob().Open(nil)
	if err != nil {
		t.Fatalf("open memory BlobStore: %v", err)
	}
	defer func() {
		if err := instance.Close(); err != nil {
			t.Errorf("close memory BlobStore: %v", err)
		}
	}()

	blobs, err := storage.AssertBlob(instance)
	if err != nil {
		t.Fatalf("assert BlobStore: %v", err)
	}

	ownerCtx := blobResolverTestContext("caller-a", "tenant-a")
	ref, err := blobs.Put(ownerCtx, "documents/report.txt", strings.NewReader("approved report\n"))
	if err != nil {
		t.Fatalf("put blob: %v", err)
	}

	base := compiler.Bundle{ID: "storage-file-resolver-test"}
	bound := WithBlobStoreFileResolver(base, ref.Provider, blobs)
	if len(base.FileResolvers) != 0 {
		t.Fatal("helper mutated the caller's original bundle")
	}
	if len(bound.FileResolvers) != 1 {
		t.Fatalf("resolver bindings = %d, want 1", len(bound.FileResolvers))
	}
	binding := bound.FileResolvers[0]
	if binding.Provider != ref.Provider {
		t.Fatalf("resolver provider = %q, want %q", binding.Provider, ref.Provider)
	}

	reader, info, err := binding.Resolver.Open(ownerCtx, ref)
	if err != nil {
		t.Fatalf("open authorized reference: %v", err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		t.Fatalf("read authorized reference: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close authorized reference: %v", closeErr)
	}
	if string(data) != "approved report\n" || info.Provider != ref.Provider || info.ID != ref.ID {
		t.Fatalf("resolved blob = %q, info=%+v; want stored payload and matching identity", data, info)
	}

	for _, scope := range []struct {
		name   string
		caller string
		tenant string
	}{
		{name: "different caller", caller: "caller-b", tenant: "tenant-a"},
		{name: "different tenant", caller: "caller-a", tenant: "tenant-b"},
	} {
		t.Run(scope.name, func(t *testing.T) {
			_, _, err := binding.Resolver.Open(blobResolverTestContext(scope.caller, scope.tenant), ref)
			if got := xerr.KindFrom(err); got != xerr.KindNotFound {
				t.Fatalf("cross-scope open kind = %q, want %q (err %v)", got, xerr.KindNotFound, err)
			}
		})
	}
}

func blobResolverTestContext(caller, tenant string) context.Context {
	ctx := xctx.WithUserID(context.Background(), caller)
	return xctx.WithTenantID(ctx, tenant)
}

var _ xfile.Resolver = storage.BlobStore(nil)
