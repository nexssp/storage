package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nexssp/kernel/xctx"

	"github.com/nexssp/storage"
)

func TestPostgresStorageIntegration(t *testing.T) {
	baseDSN := os.Getenv("NEXSS_STORAGE_POSTGRES_DSN")
	if baseDSN == "" {
		t.Skip("set NEXSS_STORAGE_POSTGRES_DSN to run PostgreSQL integration coverage")
	}
	ctx := context.Background()
	admin, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("storage_it_%d", time.Now().UnixNano())
	// The test role must be allowed to CREATE SCHEMA; the CI postgres user has this privilege.
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop integration schema: %v", err)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("close admin database: %v", err)
		}
	})

	parsed, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	dsn := parsed.String()

	migrationDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	migrationFiles, err := fs.ReadDir(Migrations(), ".")
	if err != nil {
		migrationDB.Close()
		t.Fatal(err)
	}
	for _, file := range migrationFiles {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}
		body, err := readMigration(file.Name())
		if err != nil {
			migrationDB.Close()
			t.Fatal(err)
		}
		for statement := range strings.SplitSeq(body, ";") {
			statement = strings.TrimSpace(statement)
			if statement == "" {
				continue
			}
			if _, err := migrationDB.ExecContext(ctx, statement); err != nil {
				migrationDB.Close()
				t.Fatalf("apply migration %s: %v", file.Name(), err)
			}
		}
	}
	if err := migrationDB.Close(); err != nil {
		t.Fatal(err)
	}

	kvInstance, err := NewKV().Open(map[string]string{"dsn": dsn, "namespace": "integration", "ttl": "1m"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := kvInstance.Close(); err != nil {
			t.Errorf("close KV: %v", err)
		}
	})
	kv, err := storage.AssertKV(kvInstance)
	if err != nil {
		t.Fatal(err)
	}
	atomicKV, err := storage.AssertAtomicKV(kvInstance)
	if err != nil {
		t.Fatal(err)
	}

	if err := kv.Set(ctx, "roundtrip", "value", time.Minute); err != nil {
		t.Fatal(err)
	}
	value, hit, err := kv.Get(ctx, "roundtrip")
	if err != nil || !hit || value != "value" {
		t.Fatalf("KV roundtrip: value=%#v hit=%v err=%v", value, hit, err)
	}

	const workers = 8
	var wait sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wait.Go(func() {
			errs <- atomicKV.Update(ctx, "atomic-counter", func(current any, hit bool) (any, time.Duration, error) {
				value := float64(0)
				if hit {
					var ok bool
					value, ok = current.(float64)
					if !ok {
						return nil, 0, fmt.Errorf("atomic KV value has type %T, want float64", current)
					}
				}
				return value + 1, time.Minute, nil
			})
		})
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	value, hit, err = kv.Get(ctx, "atomic-counter")
	if err != nil || !hit || value != float64(workers) {
		t.Fatalf("atomic update result: value=%#v hit=%v err=%v", value, hit, err)
	}

	scopedInstance, err := NewKV().Open(map[string]string{"dsn": dsn, "namespace": "tenant-scope", "tenant_scope": "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scopedInstance.Close(); err != nil {
			t.Errorf("close tenant-scoped KV: %v", err)
		}
	})
	scopedKV, err := storage.AssertKV(scopedInstance)
	if err != nil {
		t.Fatal(err)
	}
	scopedAtomicKV, err := storage.AssertAtomicKV(scopedInstance)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := scopedKV.Get(ctx, "key"); err == nil {
		t.Fatal("expected missing tenant context to fail")
	}
	tenantA := xctx.WithTenantID(ctx, "tenant-a")
	tenantB := xctx.WithTenantID(ctx, "tenant-b")
	if err := scopedKV.Set(tenantA, "key", "a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := scopedAtomicKV.Update(tenantB, "key", func(_ any, hit bool) (any, time.Duration, error) {
		if hit {
			return nil, 0, errors.New("tenant B unexpectedly saw tenant A's value")
		}
		return "b", time.Minute, nil
	}); err != nil {
		t.Fatal(err)
	}
	value, hit, err = scopedKV.Get(tenantA, "key")
	if err != nil || !hit || value != "a" {
		t.Fatalf("tenant B's atomic update affected tenant A: value=%#v hit=%v err=%v", value, hit, err)
	}
	value, hit, err = scopedKV.Get(tenantB, "key")
	if err != nil || !hit || value != "b" {
		t.Fatalf("tenant B's atomic update did not persist independently: value=%#v hit=%v err=%v", value, hit, err)
	}

	txInstance, err := NewTx().Open(map[string]string{"dsn": dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := txInstance.Close(); err != nil {
			t.Errorf("close transaction backend: %v", err)
		}
	})
	txRunner, err := storage.AssertTxRunner(txInstance)
	if err != nil {
		t.Fatal(err)
	}
	if err := txRunner.RunInTx(ctx, func(txCtx context.Context) error {
		return kv.Set(txCtx, "transactional", "committed", time.Minute)
	}); err != nil {
		t.Fatal(err)
	}
	value, hit, err = kv.Get(ctx, "transactional")
	if err != nil || !hit || value != "committed" {
		t.Fatalf("transactional write: value=%#v hit=%v err=%v", value, hit, err)
	}

	blobInstance, err := NewBlob().Open(map[string]string{"dsn": dsn, "namespace": "integration"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := blobInstance.Close(); err != nil {
			t.Errorf("close Blob: %v", err)
		}
	})
	blob, err := storage.AssertBlob(blobInstance)
	if err != nil {
		t.Fatal(err)
	}
	blobCtx := xctx.WithTenantID(xctx.WithUserID(ctx, "integration-user"), "integration-tenant")
	ref, err := blob.Put(blobCtx, "object", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	reader, info, err := blob.Open(blobCtx, ref)
	if err != nil {
		t.Fatal(err)
	}
	payload, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || string(payload) != "payload" || info.Size != int64(len(payload)) {
		t.Fatalf("Blob roundtrip: payload=%q info=%+v readErr=%v closeErr=%v", payload, info, readErr, closeErr)
	}
}

func readMigration(name string) (string, error) {
	data, err := fs.ReadFile(Migrations(), name)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
