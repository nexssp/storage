package memory

import (
	"context"
	"fmt"
	"testing"

	"github.com/nexssp/storage"
)

func TestPagedKVHasNoHiddenRetentionOrPageLimit(t *testing.T) {
	instance, err := NewKV().Open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	pages, err := storage.AssertPagedKV(instance)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for index := range 8193 {
		cursor := fmt.Sprintf("event-%05d", index)
		if err := pages.Append(ctx, "activity", cursor, index); err != nil {
			t.Fatalf("append %s: %v", cursor, err)
		}
	}
	page, err := pages.Page(ctx, "activity", "", 8193)
	if err != nil {
		t.Fatal(err)
	}
	if !page.CursorFound || page.HasMore || len(page.Items) != 8193 {
		t.Fatalf("page = found %v, more %v, items %d", page.CursorFound, page.HasMore, len(page.Items))
	}
	if page.Items[0].Cursor != "event-00000" || page.Items[len(page.Items)-1].Cursor != "event-08192" {
		t.Fatalf("unexpected first/last cursor: %q / %q", page.Items[0].Cursor, page.Items[len(page.Items)-1].Cursor)
	}
	if err := pages.Append(ctx, "activity", "event-00000", "duplicate"); err == nil {
		t.Fatal("expected duplicate cursor to be rejected")
	}
	if _, err := pages.Page(ctx, "activity", "", 0); err == nil {
		t.Fatal("expected non-positive page limit to be rejected")
	}
}
