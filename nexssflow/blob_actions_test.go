package nexssflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/nexssp/kernel/xctx"

	"github.com/nexssp/storage"
)

func TestBlobActionsReturnTypedReferenceHandles(t *testing.T) {
	b := newBundle()
	defer b.Close(context.Background())
	s := b.newStore([]storage.Declaration{{Name: "files", Backend: "memory", Kind: storage.KindBlob}})
	ctx := xctx.WithTenantID(xctx.WithUserID(context.Background(), "user-1"), "tenant-1")
	ctx = storeKey.With(ctx, s)

	putValue, err := blobPutAction().DoAny(ctx, map[string]any{
		"store": "files", "key": "reports/q4.json", "data": strings.NewReader(`{"ok":true}`),
	})
	if err != nil {
		t.Fatalf("put action: %v", err)
	}
	putResult, ok := putValue.(blobPutRes)
	if !ok || !putResult.OK {
		t.Fatalf("put result: %#v", putValue)
	}
	if putResult.Ref.Provider != "memory" || putResult.Ref.ID == "" || putResult.Ref.ExpectedSize == 0 {
		t.Fatalf("put reference: %+v", putResult.Ref)
	}

	getValue, err := blobGetAction().DoAny(ctx, map[string]any{"store": "files", "key": "reports/q4.json"})
	if err != nil {
		t.Fatalf("get action: %v", err)
	}
	getResult, ok := getValue.(blobGetRes)
	if !ok || !getResult.Hit {
		t.Fatalf("get result: %#v", getValue)
	}
	typed := getResult.Ref
	if typed.ID != putResult.Ref.ID || typed.ExpectedDigest == "" {
		t.Fatalf("lookup did not return the typed reference: %+v", typed)
	}
	encoded, err := json.Marshal(getResult)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"data"`) || strings.Contains(string(encoded), `"payload"`) {
		t.Fatalf("Flow Blob get leaked bytes instead of returning a small handle: %s", encoded)
	}

	aliasPutValue, err := aliasedBlobPut(s, "files").DoAny(ctx, map[string]any{
		"key": "aliased/report.json", "data": `{"alias":true}`,
	})
	if err != nil {
		t.Fatalf("aliased put action: %v", err)
	}
	aliasPut, ok := aliasPutValue.(blobPutRes)
	if !ok || !aliasPut.OK || aliasPut.Ref.ExpectedSize == 0 {
		t.Fatalf("aliased put result: %#v", aliasPutValue)
	}
	aliasGetValue, err := aliasedBlobGet(s, "files").DoAny(ctx, map[string]any{"key": "aliased/report.json"})
	if err != nil {
		t.Fatalf("aliased get action: %v", err)
	}
	aliasGet, ok := aliasGetValue.(blobGetRes)
	if !ok || !aliasGet.Hit || aliasGet.Ref.ID != aliasPut.Ref.ID {
		t.Fatalf("aliased get result: %#v", aliasGetValue)
	}
	aliasEncoded, err := json.Marshal(aliasGet)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(aliasEncoded), `"data"`) || strings.Contains(string(aliasEncoded), `"alias":true`) {
		t.Fatalf("aliased Blob get leaked bytes instead of returning a small handle: %s", aliasEncoded)
	}
}
