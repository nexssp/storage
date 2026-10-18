package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
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

func TestSQLiteKVAndBlobOperationsJoinTransactionAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.db")
	ctx := xctx.WithTenantID(xctx.WithUserID(context.Background(), "tx-user"), "tx-tenant")
	setup, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	applyEmbeddedMigrations(t, setup)
	if err := setup.Close(); err != nil {
		t.Fatal(err)
	}

	kvInst, err := NewKV().Open(map[string]string{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer kvInst.Close()
	kv, err := storage.AssertKV(kvInst)
	if err != nil {
		t.Fatal(err)
	}
	blobInst, err := NewBlob().Open(map[string]string{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer blobInst.Close()
	blob, err := storage.AssertBlob(blobInst)
	if err != nil {
		t.Fatal(err)
	}
	blobCtx := ctx
	txInst, err := NewTx().Open(map[string]string{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer txInst.Close()
	runner, err := storage.AssertTxRunner(txInst)
	if err != nil {
		t.Fatal(err)
	}

	abort := errors.New("abort transaction")
	err = runner.RunInTx(ctx, func(txCtx context.Context) error {
		if err := kv.Set(txCtx, "rolled-back", "no", 0); err != nil {
			return err
		}
		if _, err := blob.Put(txCtx, "rolled-back-blob", bytes.NewBufferString("payload")); err != nil {
			return err
		}
		value, hit, err := kv.Get(txCtx, "rolled-back")
		if err != nil {
			return fmt.Errorf("read inside transaction: %w", err)
		}
		if !hit || value != "no" {
			return fmt.Errorf("read inside transaction: value=%#v hit=%v", value, hit)
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatalf("transaction error = %v, want abort", err)
	}
	if value, hit, err := kv.Get(ctx, "rolled-back"); err != nil || hit {
		t.Fatalf("rolled-back key: value=%#v hit=%v err=%v", value, hit, err)
	}
	if _, hit, err := blob.Get(blobCtx, "rolled-back-blob"); err != nil || hit {
		t.Fatalf("rolled-back blob: hit=%v err=%v", hit, err)
	}

	if err := runner.RunInTx(ctx, func(txCtx context.Context) error {
		return kv.Set(txCtx, "committed", "yes", 0)
	}); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}
	if value, hit, err := kv.Get(ctx, "committed"); err != nil || !hit || value != "yes" {
		t.Fatalf("committed key: value=%#v hit=%v err=%v", value, hit, err)
	}
}

func TestSQLiteStorageRejectsDifferentTransactionDatabase(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	txInst, err := NewTx().Open(map[string]string{"path": filepath.Join(base, "tx.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer txInst.Close()
	kvInst, err := NewKV().Open(map[string]string{"path": filepath.Join(base, "other.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer kvInst.Close()
	runner, err := storage.AssertTxRunner(txInst)
	if err != nil {
		t.Fatal(err)
	}
	kv, err := storage.AssertKV(kvInst)
	if err != nil {
		t.Fatal(err)
	}
	err = runner.RunInTx(ctx, func(txCtx context.Context) error {
		return kv.Set(txCtx, "outside", "must not be written", 0)
	})
	if err == nil || !strings.Contains(err.Error(), "transaction database does not match") {
		t.Fatalf("different database error = %v, want transaction/database mismatch", err)
	}
}

func TestSQLiteAtomicKVConcurrentInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "atomic.db")
	setup, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	applyEmbeddedMigrations(t, setup)
	_ = setup.Close()
	open := func() storage.AtomicKV {
		t.Helper()
		inst, err := NewKV().Open(map[string]string{"path": path, "namespace": "atomic"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = inst.Close() })
		atomicKV, err := storage.AssertAtomicKV(inst)
		if err != nil {
			t.Fatal(err)
		}
		return atomicKV
	}
	a, b := open(), open()
	if err := a.Update(t.Context(), "counter", func(_ any, hit bool) (any, time.Duration, error) {
		if hit {
			t.Fatal("first update was not a miss")
		}
		return float64(1), time.Hour, nil
	}); err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("reject update")
	if err := a.Update(t.Context(), "counter", func(any, bool) (any, time.Duration, error) { return float64(999), 0, sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("callback error = %v", err)
	}
	const workers, iterations = 16, 40
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := range workers {
		wg.Go(func() {
			store := a
			if w%2 == 1 {
				store = b
			}
			for range iterations {
				if err := store.Update(t.Context(), "counter", func(current any, hit bool) (any, time.Duration, error) {
					if !hit {
						return nil, 0, errors.New("unexpected miss")
					}
					value, ok := current.(float64)
					if !ok {
						return nil, 0, fmt.Errorf("counter value has type %T, want float64", current)
					}
					return value + 1, time.Hour, nil
				}); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	ctx := t.Context()
	instance, err := NewKV().Open(map[string]string{"path": path, "namespace": "atomic"})
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close()
	layer, _ := storage.AssertKV(instance)
	got, hit, err := layer.Get(ctx, "counter")
	if err != nil || !hit {
		t.Fatalf("get: hit=%v err=%v", hit, err)
	}
	counter, ok := got.(float64)
	if !ok {
		t.Fatalf("counter has type %T, want float64", got)
	}
	if counter != 1+workers*iterations {
		t.Fatalf("counter = %v", got)
	}
	if err := a.Update(ctx, "short", func(_ any, hit bool) (any, time.Duration, error) {
		if hit {
			t.Fatal("short key should miss")
		}
		return "ephemeral", time.Millisecond, nil
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := a.Update(ctx, "short", func(_ any, hit bool) (any, time.Duration, error) {
		if hit {
			t.Fatal("expired item must be a miss")
		}
		return "ok", time.Hour, nil
	}); err != nil {
		t.Fatal(err)
	}
}
