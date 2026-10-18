package nexssflow

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/flow/extensions/modifiers_meta"
	"github.com/nexssp/flow/extensions/pipeline"
	"github.com/nexssp/flow/extensions/projection"
	"github.com/nexssp/flow/extensions/runtime"
	"github.com/nexssp/flow/extensions/scope"
	"github.com/nexssp/flow/extensions/syntax"
	"github.com/nexssp/flow/runner"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/sqlite"
)

func buildTransactionTestConfig(t *testing.T) (cfg runner.Config, cleanup func()) {
	t.Helper()
	storageBundle := Bundle(nil)
	bundles := []compiler.Bundle{
		syntax.Bundle(nil),
		runtime.Bundle(nil),
		projection.Bundle(nil),
		modifiers_meta.Bundle(nil),
		pipeline.Bundle(nil),
		scope.Bundle(nil),
		storageBundle,
	}
	cfg, err := runner.BuildConfig(bundles)
	if err != nil {
		t.Fatalf("build Flow config: %v", err)
	}
	return cfg, func() {
		for _, shutdown := range storageBundle.Shutdowns {
			if err := shutdown(context.Background()); err != nil {
				t.Errorf("shutdown storage bundle: %v", err)
			}
		}
	}
}

func applyStorageMigrations(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	entries, err := fs.ReadDir(sqlite.Migrations(), ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		body, err := fs.ReadFile(sqlite.Migrations(), entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		var up strings.Builder
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
				up.WriteString(line)
				up.WriteByte('\n')
			}
		}
		for statement := range strings.SplitSeq(up.String(), ";") {
			if strings.TrimSpace(statement) == "" {
				continue
			}
			if _, err := db.ExecContext(t.Context(), statement); err != nil {
				t.Fatalf("apply migration %s: %v\nSQL: %s", entry.Name(), err, statement)
			}
		}
	}
}

func TestFlowPipelineTransactionCommitsAndRollsBackStorageKV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "storage.db")
	applyStorageMigrations(t, path)
	cfg, shutdown := buildTransactionTestConfig(t)
	defer shutdown()

	declaration := fmt.Sprintf("@storage tx:sqlite as tx { path: %q }\n@storage kv:sqlite as cache { path: %q }\n", path, path)
	successSource := declaration + `
noop
-> cache.kv.set @{ key: "committed", value: "visible" }
-> tx.tx.run @{ step: "whole-flow" }
-> cache.kv.get @{ key: "committed" }
`
	execution, err := runner.Execute(context.Background(), cfg, successSource, "storage-transaction-success.nflow", nil)
	if err != nil {
		t.Fatalf("execute committing Flow: %v", err)
	}
	result, ok := execution.Output.(kvGetRes)
	if !ok || !result.Hit || result.Value != "visible" {
		t.Fatalf("committed Flow result = %#v, want hit with value visible", execution.Output)
	}

	failureSource := declaration + `
noop
-> cache.kv.set @{ key: "rolled-back", value: "hidden" }
-> runtime.fail @{ message: "force transaction rollback" }
`
	if _, err := runner.Execute(context.Background(), cfg, failureSource, "storage-transaction-rollback.nflow", nil); err == nil {
		t.Fatal("failing Flow pipeline unexpectedly succeeded")
	}

	checkInst, err := sqlite.NewKV().Open(map[string]string{"path": path})
	if err != nil {
		t.Fatal(err)
	}
	defer checkInst.Close()
	check, err := storage.AssertKV(checkInst)
	if err != nil {
		t.Fatal(err)
	}
	if value, hit, err := check.Get(context.Background(), "committed"); err != nil || !hit || value != "visible" {
		t.Fatalf("committed key = %#v hit=%v err=%v", value, hit, err)
	}
	if value, hit, err := check.Get(context.Background(), "rolled-back"); err != nil || hit {
		t.Fatalf("rolled-back key = %#v hit=%v err=%v, want missing", value, hit, err)
	}
}
