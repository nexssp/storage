package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/nexssp/storage"
)

func TestSQLitePagedKVReadsOrderedPagesWithoutHiddenCaps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.db")
	migrator, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	applyEmbeddedMigrations(t, migrator)
	_ = migrator.Close()

	instance, err := NewKV().Open(map[string]string{"path": path, "namespace": "test"})
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
		t.Fatalf("first page = found %v, more %v, items %d, cursors %q..%q", first.CursorFound, first.HasMore, len(first.Items), first.Items[0].Cursor, first.Items[len(first.Items)-1].Cursor)
	}
	second, err := pages.Page(ctx, "activity", first.Items[len(first.Items)-1].Cursor, 150)
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
	if _, err := pages.Page(ctx, "activity", "", 0); err == nil {
		t.Fatal("expected a non-positive page size to be rejected")
	}
}

func TestSQLitePagedKVAppendRollsBackWithAmbientTransaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.db")
	migrator, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	applyEmbeddedMigrations(t, migrator)
	_ = migrator.Close()

	kvInstance, err := NewKV().Open(map[string]string{"path": path, "namespace": "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer kvInstance.Close()
	txInstance, err := NewTx().Open(map[string]string{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer txInstance.Close()

	atomicKV, err := storage.AssertAtomicKV(kvInstance)
	if err != nil {
		t.Fatal(err)
	}
	pages, err := storage.AssertPagedKV(kvInstance)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := storage.AssertTxRunner(txInstance)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("rollback")
	err = runner.RunInTx(context.Background(), func(txCtx context.Context) error {
		if err := atomicKV.Update(txCtx, "state", func(any, bool) (any, time.Duration, error) {
			return "updated", 0, nil
		}); err != nil {
			return err
		}
		if err := pages.Append(txCtx, "activity", "event-1", map[string]string{"type": "created"}); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("transaction error = %v, want rollback sentinel", err)
	}
	layer, err := storage.AssertKV(kvInstance)
	if err != nil {
		t.Fatal(err)
	}
	if _, hit, err := layer.Get(context.Background(), "state"); err != nil || hit {
		t.Fatalf("state after rollback: hit=%v err=%v", hit, err)
	}
	page, err := pages.Page(context.Background(), "activity", "", 1)
	if err != nil || len(page.Items) != 0 || page.HasMore {
		t.Fatalf("page after rollback: %+v err=%v", page, err)
	}
}
