package postgres

import (
	"io/fs"
	"strings"
	"testing"

	"github.com/nexssp/storage"
)

func TestPostgresBackendRegistrationAndMigrationFiles(t *testing.T) {
	reg := storage.NewRegistry()
	if err := Register(reg); err != nil {
		t.Fatal(err)
	}
	if got := reg.Names(); len(got) != 1 || got[0] != "postgres" {
		t.Fatalf("registered names: %v", got)
	}
	files, err := fs.ReadDir(Migrations(), ".")
	if err != nil || len(files) != 1 || files[0].Name() != "00001_create_storage_tables.sql" {
		t.Fatalf("migration files=%v err=%v", files, err)
	}
	body, err := fs.ReadFile(Migrations(), files[0].Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(body), "-- +goose Up") {
		t.Fatalf("migration must use a Goose Up section: %q", body)
	}
}

func TestPostgresOptionsAreValidatedBeforeConnecting(t *testing.T) {
	if _, err := NewKV().Open(nil); err == nil {
		t.Fatal("expected required dsn error")
	}
	if _, err := NewKV().Open(map[string]string{"dsn": "postgres://localhost/db", "oops": "x"}); err == nil {
		t.Fatal("expected unknown option error before network access")
	}
}
