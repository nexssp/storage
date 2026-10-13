package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nexssp/kernel/xctx"

	"github.com/nexssp/storage"
)

func applyEmbeddedMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := fs.ReadDir(Migrations(), ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "00001_create_storage_tables.sql" {
		t.Fatalf("unexpected migration files: %v", entries)
	}
	body, err := fs.ReadFile(Migrations(), entries[0].Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(body), "-- +goose Up") {
		t.Fatalf("migration must use a Goose Up section: %q", body)
	}
	var sqlText strings.Builder
	inUp := false
	for line := range strings.SplitSeq(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "-- +goose Up") {
			inUp = true
			continue
		}
		if strings.HasPrefix(trimmed, "-- +goose ") {
			inUp = false
			continue
		}
		if inUp && !strings.HasPrefix(trimmed, "--") {
			sqlText.WriteString(line)
			sqlText.WriteByte('\n')
		}
	}
	for statement := range strings.SplitSeq(sqlText.String(), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatalf("apply migration: %v\nSQL: %s", err, statement)
		}
	}
}

func TestSQLiteBackendsRequireExplicitMigrationAndSupportStorage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.db")
	ctx := context.Background()

	kvInst, err := NewKV().Open(map[string]string{"path": path, "namespace": "tests", "ttl": "1m"})
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	defer kvInst.Close()
	kv, err := storage.AssertKV(kvInst)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := kv.Get(ctx, "before-migration"); err == nil {
		t.Fatal("expected operation to fail until explicit migration is applied")
	}
	checkDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var tableCount int
	if err := checkDB.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='nexss_storage_kv'").Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	_ = checkDB.Close()
	if tableCount != 0 {
		t.Fatalf("Open created adapter schema before explicit migration (count=%d)", tableCount)
	}

	migrator, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	applyEmbeddedMigrations(t, migrator)
	_ = migrator.Close()

	if err := kv.Set(ctx, "hello", "world", 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	value, hit, err := kv.Get(ctx, "hello")
	if err != nil || !hit || value != "world" {
		t.Fatalf("get: value=%#v hit=%v err=%v", value, hit, err)
	}
	if err := kv.Set(ctx, "short", "lived", 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(35 * time.Millisecond)
	if _, expiredHit, expirationErr := kv.Get(ctx, "short"); expirationErr != nil || expiredHit {
		t.Fatalf("expired value: hit=%v err=%v", expiredHit, expirationErr)
	}
	deleter, ok := kvInst.(interface {
		Delete(context.Context, string) error
	})
	if !ok {
		t.Fatal("KV instance does not implement Delete")
	}
	if err := deleter.Delete(ctx, "hello"); err != nil {
		t.Fatal(err)
	}

	blobInst, err := NewBlob().Open(map[string]string{"path": path, "namespace": "tests"})
	if err != nil {
		t.Fatal(err)
	}
	defer blobInst.Close()
	blob, err := storage.AssertBlob(blobInst)
	if err != nil {
		t.Fatal(err)
	}
	blobCtx := xctx.WithTenantID(xctx.WithUserID(ctx, "user-1"), "tenant-1")
	putRef, err := blob.Put(blobCtx, "file", bytes.NewBufferString("payload"))
	if err != nil {
		t.Fatal(err)
	}
	ref, hit, err := blob.Get(blobCtx, "file")
	if err != nil || !hit || ref.ID != putRef.ID || ref.ExpectedSize != 7 {
		t.Fatalf("blob lookup: ref=%+v hit=%v err=%v", ref, hit, err)
	}
	reader, info, err := blob.Open(blobCtx, ref)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || string(data) != "payload" || info.Size != 7 || info.Digest != string(ref.ExpectedDigest) {
		t.Fatalf("blob open: data=%q info=%+v err=%v", data, info, err)
	}
	otherCtx := xctx.WithTenantID(xctx.WithUserID(ctx, "user-2"), "tenant-1")
	if _, otherHit, otherErr := blob.Get(otherCtx, "file"); otherErr != nil || otherHit {
		t.Fatalf("cross-user lookup must be concealed: hit=%v err=%v", otherHit, otherErr)
	}
	if _, err := blob.Put(otherCtx, "file", bytes.NewBufferString("other")); err != nil {
		t.Fatalf("other user same-key put: %v", err)
	}
	ownerRef, hit, err := blob.Get(blobCtx, "file")
	if err != nil || !hit || ownerRef.ExpectedDigest != ref.ExpectedDigest {
		t.Fatalf("other user must not overwrite owner's blob: ref=%+v hit=%v err=%v", ownerRef, hit, err)
	}

	txInst, err := NewTx().Open(map[string]string{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer txInst.Close()
	runner, err := storage.AssertTxRunner(txInst)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.RunInTx(ctx, func(txCtx context.Context) error {
		tx, ok := TxFromContext(txCtx)
		if !ok {
			return storage.Internal("transaction missing from callback context")
		}
		_, err := tx.ExecContext(txCtx, "INSERT INTO nexss_storage_kv(namespace,key,value) VALUES(?,?,?)", "default", "committed", []byte(`"yes"`))
		return err
	}); err != nil {
		t.Fatalf("transaction: %v", err)
	}
	checkInst, err := NewKV().Open(map[string]string{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer checkInst.Close()
	check, _ := storage.AssertKV(checkInst)
	if value, hit, err := check.Get(ctx, "committed"); err != nil || !hit || value != "yes" {
		t.Fatalf("committed value=%#v hit=%v err=%v", value, hit, err)
	}
}
