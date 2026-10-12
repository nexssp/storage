package nexssflow

import (
	"context"
	"strings"
	"testing"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/storage"
)

// TestBundleShape verifies the Bundle returns what it claims.
func TestBundleShape(t *testing.T) {
	b := Bundle(nil)
	if b.ID != ID {
		t.Fatalf("ID: got %q, want %q", b.ID, ID)
	}
	if len(b.Directives) != 1 || b.Directives[0].Name != "storage" {
		t.Fatalf("directive: %+v", b.Directives)
	}
	if len(b.Libraries) != 1 {
		t.Fatalf("libraries: got %d, want 1", len(b.Libraries))
	}
	if len(b.Shutdowns) != 1 {
		t.Fatalf("shutdowns: got %d, want 1", len(b.Shutdowns))
	}
	if b.Materialize == nil {
		t.Fatal("Materialize hook is nil")
	}
	if b.OnPreprocess == nil {
		t.Fatal("OnPreprocess hook is nil")
	}
	if b.WrapPipeline == nil {
		t.Fatal("WrapPipeline hook is nil")
	}
	// Modifiers moved into storageAdvisor; the bundle declares zero.
	if len(b.Modifiers) != 4 {
		t.Fatalf("Modifiers: got %d, want 4 (handled by advisor)", len(b.Modifiers))
	}

	wantActions := map[string]bool{
		"storage.kv.get":       false,
		"storage.kv.set":       false,
		"storage.kv.del":       false,
		"storage.blob.get":     false,
		"storage.blob.put":     false,
		"storage.blob.del":     false,
		"storage.lock.acquire": false,
		"storage.lock.release": false,
		"storage.probe":        false,
	}
	for _, act := range b.Libraries[0].Actions {
		meta := act.Describe()
		wantActions[meta.Name] = true
	}
	for name, saw := range wantActions {
		if !saw {
			t.Errorf("missing generic action %q", name)
		}
	}
}

// TestDirectiveParsesKindBackend verifies the colon form parses.
func TestDirectiveParsesKindBackend(t *testing.T) {
	directive := storageDirective()

	out := map[string]any{}
	lines := []string{`@storage kv:memory { ttl: "5m" }`, `{ ttl: "5m" }`}

	res, err := directive.Handler(context.Background(), compiler.DirectiveReq{
		Lines: lines, I: 0, Out: out, File: "test.nflow",
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if res.Next == 0 {
		t.Fatal("expected Next > 0")
	}

	decls, _ := out["storage"].([]storage.Declaration)
	if len(decls) != 1 {
		t.Fatalf("decls: got %d, want 1", len(decls))
	}
	d := decls[0]
	if d.Name != "kv:memory" || d.Backend != "memory" || d.Kind != storage.KindKV {
		t.Fatalf("decl: %+v", d)
	}
}

func TestDirectiveRejectsUnknownKind(t *testing.T) {
	lines := []string{`@storage bogus:memory { }`, `{ }`}
	_, err := storageDirective().Handler(context.Background(), compiler.DirectiveReq{
		Lines: lines, I: 0, Out: map[string]any{}, File: "test.nflow",
	})
	if err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("expected unknown-kind error, got: %v", err)
	}
}

func TestDirectiveRejectsDuplicate(t *testing.T) {
	directive := storageDirective()
	lines := []string{`@storage kv:memory { }`, `{ }`}
	req := compiler.DirectiveReq{Lines: lines, I: 0, Out: map[string]any{}, File: "test.nflow"}

	if _, err := directive.Handler(context.Background(), req); err != nil {
		t.Fatalf("first handler: %v", err)
	}
	if _, err := directive.Handler(context.Background(), req); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestDirectiveAlias(t *testing.T) {
	lines := []string{`@storage kv:memory as cache { ttl: "1m" }`, `{ ttl: "1m" }`}
	out := map[string]any{}
	_, err := storageDirective().Handler(context.Background(), compiler.DirectiveReq{
		Lines: lines, I: 0, Out: out, File: "test.nflow",
	})
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	decls, _ := out["storage"].([]storage.Declaration)
	if len(decls) != 1 || decls[0].Name != "cache" {
		t.Fatalf("decl: %+v", decls)
	}
}

// TestMaterialize_MountsAliasedActions verifies that the Materialize
// hook mounts <alias>.kv.set, <alias>.kv.get, etc. for every
// declaration. The materializer reads declarations from meta and
// creates the per-compilation store from the bundle's registry.
func TestMaterialize_MountsAliasedActions(t *testing.T) {
	b := newBundle()
	meta := map[string]any{
		"storage": []storage.Declaration{
			{Name: "cache", Backend: "memory", Kind: storage.KindKV},
			{Name: "files", Backend: "memory", Kind: storage.KindBlob},
			{Name: "locks", Backend: "memory", Kind: storage.KindLock},
			{Name: "idem", Backend: "memory", Kind: storage.KindIdem},
			{Name: "journal", Backend: "memory", Kind: storage.KindJournal},
		},
	}

	mock := newMockResolver()
	err := materialize(b)(compiler.MaterializeReq{
		Ctx:      context.Background(),
		Meta:     meta,
		Resolver: mock,
	})
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}

	wantAliased := []string{
		"cache.kv.set", "cache.kv.get", "cache.kv.del",
		"files.blob.put", "files.blob.get", "files.blob.del",
		"locks.acquire", "locks.release",
		"idem.idem.claim", "idem.idem.complete", "idem.idem.release",
		"journal.journal.get", "journal.journal.record",
	}
	for _, name := range wantAliased {
		if _, ok := mock.actions[name]; !ok {
			t.Errorf("materialize did not mount %q", name)
		}
	}
}

// TestMaterializedActions_AreCallable proves a materialized aliased
// action (e.g. cache.kv.set) actually works when invoked.
func TestMaterializedActions_AreCallable(t *testing.T) {
	b := newBundle()
	meta := map[string]any{
		"storage": []storage.Declaration{
			{Name: "cache", Backend: "memory", Kind: storage.KindKV},
		},
	}

	mock := newMockResolver()
	if err := materialize(b)(compiler.MaterializeReq{
		Ctx:      context.Background(),
		Meta:     meta,
		Resolver: mock,
	}); err != nil {
		t.Fatalf("materialize: %v", err)
	}

	setAct, ok := mock.actions["cache.kv.set"]
	if !ok {
		t.Fatal("cache.kv.set not mounted")
	}

	res, err := setAct.DoAny(context.Background(), map[string]any{
		"key": "k", "value": "v",
	})
	if err != nil {
		t.Fatalf("DoAny: %v", err)
	}
	if m, ok := res.(kvSetRes); !ok || !m.OK {
		t.Fatalf("set result: %+v", res)
	}
}

// TestOnPreprocess_NoDeclarationsIsNoOp proves OnPreprocess returns an
// empty contribution for flows that do not declare @storage, so the
// compiled program is not wrapped and no advisor runs.
func TestOnPreprocess_NoDeclarationsIsNoOp(t *testing.T) {
	b := newBundle()
	contrib := onPreprocess(b)(map[string]any{})
	if len(contrib.CompileOpts) != 0 || len(contrib.Primaries) != 0 {
		t.Fatalf("expected empty contribution, got %+v", contrib)
	}
}

// TestOnPreprocess_CreatesStoreForDeclarations proves OnPreprocess
// creates the per-compilation store and installs an advisor.
func TestOnPreprocess_CreatesStoreForDeclarations(t *testing.T) {
	b := newBundle()
	meta := map[string]any{
		"storage": []storage.Declaration{
			{Name: "cache", Backend: "memory", Kind: storage.KindKV},
		},
	}
	contrib := onPreprocess(b)(meta)
	if len(contrib.CompileOpts) != 1 {
		t.Fatalf("expected one CompileOption, got %d", len(contrib.CompileOpts))
	}
	if storeFromMeta(meta) == nil {
		t.Fatal("OnPreprocess did not publish the store to meta")
	}
}

// TestWrapPipeline_NoStoreIsNoOp proves the wrapper is a pass-through
// when there are no @storage declarations.
func TestWrapPipeline_NoStoreIsNoOp(t *testing.T) {
	b := newBundle()
	inner := action.New("inner",
		func(_ context.Context, _ any) (any, error) { return "ok", nil }).Build()

	wrapped, err := wrapPipeline(b)(map[string]any{}, inner)
	if err != nil {
		t.Fatalf("wrapPipeline: %v", err)
	}
	if wrapped != inner {
		t.Fatal("expected pass-through when no store is present")
	}
}

// mockResolver implements compiler.CapabilityResolver for testing.
type mockResolver struct {
	actions   map[string]action.AnyAction
	streams   map[string]action.AnyStreamAction
	operators map[string]action.NamedOperator
}

func newMockResolver() *mockResolver {
	return &mockResolver{
		actions:   make(map[string]action.AnyAction),
		streams:   make(map[string]action.AnyStreamAction),
		operators: make(map[string]action.NamedOperator),
	}
}

func (m *mockResolver) Action(name string) (action.AnyAction, bool) {
	a, ok := m.actions[name]
	return a, ok
}

func (m *mockResolver) Stream(name string) (action.AnyStreamAction, bool) {
	s, ok := m.streams[name]
	return s, ok
}

func (m *mockResolver) Operator(name string) (action.NamedOperator, bool) {
	o, ok := m.operators[name]
	return o, ok
}

func (m *mockResolver) Mount(lib action.Library) error {
	for _, act := range lib.Actions {
		meta := act.Describe()
		m.actions[meta.Name] = act
	}
	return nil
}

func (m *mockResolver) MountWithAlias(lib action.Library, _ string) error {
	return m.Mount(lib)
}
