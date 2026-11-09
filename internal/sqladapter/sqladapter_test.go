package sqladapter

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/nexssp/storage"
)

func TestSQLiteDefaultBusyTimeout(t *testing.T) {
	backend := NewBackend("sqlite", "sqlite", "kv", "path")
	inst, err := backend.Open(map[string]string{"path": filepath.Join(t.TempDir(), "timeout.db")})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()

	sqlInst, ok := inst.(*instance)
	if !ok {
		t.Fatalf("opened instance has type %T, want *instance", inst)
	}
	sqlInst.db.SetMaxOpenConns(1)
	var timeout int
	if err := sqlInst.db.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&timeout); err != nil {
		t.Fatal(err)
	}
	if timeout != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", timeout)
	}
}

func TestSQLiteWithoutTimeoutReturnsBusy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-timeout.db")
	first, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	first.SetMaxOpenConns(1)
	second.SetMaxOpenConns(1)

	if _, err := first.ExecContext(t.Context(), "CREATE TABLE busy_test (value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	tx, err := first.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), "INSERT INTO busy_test(value) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	_, err = second.ExecContext(t.Context(), "INSERT INTO busy_test(value) VALUES (2)")
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "locked") {
		t.Fatalf("write without a busy timeout error = %v, want SQLITE_BUSY", err)
	}
}

func TestSQLiteMemoryDSNRemainsInMemoryWithBusyTimeout(t *testing.T) {
	backend := NewBackend("sqlite", "sqlite", "kv", "path")
	inst, err := backend.Open(map[string]string{"path": ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()

	sqlInst, ok := inst.(*instance)
	if !ok {
		t.Fatalf("opened instance has type %T, want *instance", inst)
	}
	sqlInst.db.SetMaxOpenConns(1)
	var sequence int
	var name, filename string
	if err := sqlInst.db.QueryRowContext(t.Context(), "PRAGMA database_list").Scan(&sequence, &name, &filename); err != nil {
		t.Fatal(err)
	}
	if name != "main" || filename != "" {
		t.Fatalf("database_list = (%d, %q, %q), want in-memory main database", sequence, name, filename)
	}
	var timeout int
	if err := sqlInst.db.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&timeout); err != nil {
		t.Fatal(err)
	}
	if timeout != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", timeout)
	}
}

func TestSQLiteDatabaseIdentityCanonicalizesPathsAndBusyTimeout(t *testing.T) {
	base := databaseIdentity("sqlite", "data/app.db")
	withDotPath := databaseIdentity("sqlite", "./data/../data/app.db?_pragma=busy_timeout(1500)")
	withTimeoutOption := databaseIdentity("sqlite", "data/app.db?_timeout=1500")
	if base != withDotPath || base != withTimeoutOption {
		t.Fatal("SQLite busy timeout options or relative path aliases changed database identity")
	}
}

func TestSQLiteMemoryDatabaseIdentitiesAreDistinct(t *testing.T) {
	first := databaseIdentity("sqlite", ":memory:")
	second := databaseIdentity("sqlite", ":memory:")
	if first == second {
		t.Fatal("separate :memory: opens must not share a transaction identity")
	}
}

func TestSQLiteFileMemoryDatabaseIdentitiesRespectSharedCache(t *testing.T) {
	first := databaseIdentity("sqlite", "file::memory:")
	second := databaseIdentity("sqlite", "file::memory:")
	if first == second {
		t.Fatal("separate file::memory: opens without shared cache must have distinct identities")
	}

	shared := databaseIdentity("sqlite", "file::memory:?cache=shared")
	sharedWithBusyTimeout := databaseIdentity("sqlite", "file::memory:?cache=shared&_pragma=busy_timeout(5000)")
	if shared != sharedWithBusyTimeout {
		t.Fatal("shared file::memory: opens must share identity regardless of busy timeout")
	}
}

func TestSQLBackendRejectsOptionsUnusedByKind(t *testing.T) {
	cases := []struct {
		name    string
		kind    storage.Kind
		options map[string]string
	}{
		{name: "blob ttl", kind: storage.KindBlob, options: map[string]string{"path": ":memory:", "ttl": "1m"}},
		{name: "blob tenant scope", kind: storage.KindBlob, options: map[string]string{"path": ":memory:", "tenant_scope": "tenant"}},
		{name: "invalid tenant scope", kind: storage.KindKV, options: map[string]string{"path": ":memory:", "tenant_scope": "caller"}},
		{name: "tx namespace", kind: storage.KindTx, options: map[string]string{"path": ":memory:", "namespace": "other"}},
		{name: "tx ttl", kind: storage.KindTx, options: map[string]string{"path": ":memory:", "ttl": "1m"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backend := NewBackend("sqlite", "sqlite", tc.kind, "path")
			if _, err := backend.Open(tc.options); err == nil {
				t.Fatal("expected unused option to be rejected")
			}
		})
	}
}
