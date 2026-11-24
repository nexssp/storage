package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/nexssp/storage"
)

func TestRedisPagedKVReadsOrderedPagesWithoutHiddenCaps(t *testing.T) {
	server := miniredis.RunT(t)
	instance, err := NewKV().Open(map[string]string{"addr": server.Addr(), "namespace": "paged-test"})
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	pages, err := storage.AssertPagedKV(instance)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for index := range 251 {
		cursor := fmt.Sprintf("cursor-%03d", index)
		if err := pages.Append(ctx, "activity", cursor, map[string]int{"index": index}); err != nil {
			t.Fatalf("append %s: %v", cursor, err)
		}
	}
	first, err := pages.Page(ctx, "activity", "", 150)
	if err != nil {
		t.Fatal(err)
	}
	if !first.CursorFound || !first.HasMore || len(first.Items) != 150 || first.Items[0].Cursor != "cursor-000" || first.Items[149].Cursor != "cursor-149" {
		t.Fatalf("first page = found %v, more %v, items %d", first.CursorFound, first.HasMore, len(first.Items))
	}
	second, err := pages.Page(ctx, "activity", first.Items[149].Cursor, 150)
	if err != nil {
		t.Fatal(err)
	}
	if !second.CursorFound || second.HasMore || len(second.Items) != 101 || second.Items[0].Cursor != "cursor-150" || second.Items[100].Cursor != "cursor-250" {
		t.Fatalf("second page = found %v, more %v, items %d", second.CursorFound, second.HasMore, len(second.Items))
	}
	missing, err := pages.Page(ctx, "activity", "unknown", 25)
	if err != nil || missing.CursorFound || len(missing.Items) != 0 {
		t.Fatalf("missing cursor page = %+v, err=%v", missing, err)
	}
	if err := pages.Append(ctx, "activity", "cursor-000", "duplicate"); err == nil {
		t.Fatal("expected duplicate cursor to be rejected")
	}
}
