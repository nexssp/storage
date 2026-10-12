package nexssflow

import (
	"context"
	"testing"
	"time"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"

	"github.com/nexssp/storage"
)

// newAdviseStore builds a bundle-backed store carrying the given
// declarations. Every test in this file uses the same construction so
// the advisor sees exactly the declarations the test intends.
func newAdviseStore(t *testing.T, decls ...storage.Declaration) *store {
	t.Helper()
	return newBundle().newStore(decls)
}

// runAdvisor applies the storage advisor to a fresh builder carrying
// the given raw modifiers on a synthetic atom. It returns the built
// action, mirroring what the compiler will do at build time.
func runAdvisor(
	t *testing.T,
	s *store,
	handler func(context.Context, any) (any, error),
	rawModifiers ...string,
) action.AnyAction {
	t.Helper()
	adv := storageAdvisor(s)
	atom := &compiler.Atom{
		Name:      "test.action",
		Modifiers: append([]string(nil), rawModifiers...),
	}
	builder := action.New("test.action", handler)
	if err := adv(atom, builder); err != nil {
		t.Fatalf("advisor: %v", err)
	}
	return builder.Build()
}

// runAdvisorErr applies the advisor and expects an error, returning it
// for further inspection. Used by the negative tests.
func runAdvisorErr(
	t *testing.T,
	s *store,
	rawModifiers ...string,
) error {
	t.Helper()
	adv := storageAdvisor(s)
	atom := &compiler.Atom{
		Name:      "test.action",
		Modifiers: append([]string(nil), rawModifiers...),
	}
	builder := action.New("test.action",
		func(_ context.Context, _ any) (any, error) { return nil, nil })
	return adv(atom, builder)
}

// ---------------------------------------------------------------------------
// :cache_store=
// ---------------------------------------------------------------------------

// TestAdvisor_CacheStore_WiresCacheMiddleware proves that the
// :cache_store= modifier wires Kernel's CacheMiddleware to a declared
// KV store. The second call is served from cache and the handler runs
// exactly once.
func TestAdvisor_CacheStore_WiresCacheMiddleware(t *testing.T) {
	s := newAdviseStore(t, storage.Declaration{
		Name:    "cache",
		Backend: "memory",
		Kind:    storage.KindKV,
		Options: map[string]string{"ttl": "5m"},
	})

	var calls int
	act := runAdvisor(t, s,
		func(_ context.Context, _ any) (any, error) {
			calls++
			return "result", nil
		},
		"cache_store=cache",
	)

	ctx := context.Background()
	req := map[string]any{"id": "user-1"}

	if _, err := act.DoAny(ctx, req); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if calls != 1 {
		t.Fatalf("after first call: handler calls = %d, want 1", calls)
	}

	if _, err := act.DoAny(ctx, req); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if calls != 1 {
		t.Fatalf("after second call: handler calls = %d, want 1 (cached)", calls)
	}
}

func TestAdvisor_CacheStore_ConsumesModifier(t *testing.T) {
	s := newAdviseStore(t, storage.Declaration{
		Name:    "cache",
		Backend: "memory",
		Kind:    storage.KindKV,
	})

	adv := storageAdvisor(s)
	atom := &compiler.Atom{
		Name:      "test.action",
		Modifiers: []string{"cache_store=cache", "tag=foo"},
	}
	builder := action.New("test.action",
		func(_ context.Context, _ any) (any, error) { return nil, nil })

	if err := adv(atom, builder); err != nil {
		t.Fatalf("advisor: %v", err)
	}
	// :cache_store= consumed; :tag= untouched.
	if len(atom.Modifiers) != 1 || atom.Modifiers[0] != "tag=foo" {
		t.Fatalf("remaining modifiers = %v, want [tag=foo]", atom.Modifiers)
	}
}

func TestAdvisor_CacheStore_RejectsMissingValue(t *testing.T) {
	s := newAdviseStore(t)
	err := runAdvisorErr(t, s, "cache_store")
	if err == nil {
		t.Fatal("expected error for :cache_store= without value")
	}
}

func TestAdvisor_CacheStore_RejectsUnknownStore(t *testing.T) {
	s := newAdviseStore(t)
	err := runAdvisorErr(t, s, "cache_store=nosuchstore")
	if err == nil {
		t.Fatal("expected error for unknown store")
	}
}

func TestAdvisor_CacheStore_RejectsWrongKind(t *testing.T) {
	// A blob declaration exists under the name "files"; wiring
	// :cache_store=files must fail because AssertKV rejects a non-KV
	// instance.
	s := newAdviseStore(t, storage.Declaration{
		Name:    "files",
		Backend: "memory",
		Kind:    storage.KindBlob,
	})

	err := runAdvisorErr(t, s, "cache_store=files")
	if err == nil {
		t.Fatal("expected error for wrong-kind store")
	}
}

// ---------------------------------------------------------------------------
// :idempotent_store=
// ---------------------------------------------------------------------------

// TestAdvisor_IdempotentStore_WiresIdempotency proves that the
// :idempotent_store= modifier wires Kernel's IdempotencyMiddleware to
// a declared idem store. Two calls with the same RequestID replay the
// first result; the handler runs once.
func TestAdvisor_IdempotentStore_WiresIdempotency(t *testing.T) {
	s := newAdviseStore(t, storage.Declaration{
		Name:    "idem",
		Backend: "memory",
		Kind:    storage.KindIdem,
	})

	var calls int
	act := runAdvisor(t, s,
		func(_ context.Context, _ any) (any, error) {
			calls++
			return "result", nil
		},
		"idempotent_store=idem",
	)

	ctx := xctx.WithRequestID(context.Background(), "test-key-1")

	if _, err := act.DoAny(ctx, nil); err != nil {
		t.Fatalf("first call: %v", err)
	}
	if calls != 1 {
		t.Fatalf("after first call: handler calls = %d, want 1", calls)
	}

	if _, err := act.DoAny(ctx, nil); err != nil {
		t.Fatalf("second call: %v", err)
	}
	if calls != 1 {
		t.Fatalf("after second call: handler calls = %d, want 1 (replayed)", calls)
	}
}

func TestAdvisor_IdempotentStore_RejectsUnknownStore(t *testing.T) {
	s := newAdviseStore(t)
	err := runAdvisorErr(t, s, "idempotent_store=nosuchstore")
	if err == nil {
		t.Fatal("expected error for unknown store")
	}
}

func TestAdvisor_IdempotentStore_RejectsWrongKind(t *testing.T) {
	s := newAdviseStore(t, storage.Declaration{
		Name:    "cache",
		Backend: "memory",
		Kind:    storage.KindKV,
	})
	err := runAdvisorErr(t, s, "idempotent_store=cache")
	if err == nil {
		t.Fatal("expected error for wrong-kind store")
	}
}

// ---------------------------------------------------------------------------
// :dedup_store=
// ---------------------------------------------------------------------------

// TestAdvisor_DedupStore_WiresDedup proves that the :dedup_store=
// modifier wires Kernel's in-flight deduplication: two concurrent
// invocations with the same key share a single handler execution.
func TestAdvisor_DedupStore_WiresDedup(t *testing.T) {
	s := newAdviseStore(t, storage.Declaration{
		Name:    "locks",
		Backend: "memory",
		Kind:    storage.KindLock,
	})

	var calls int
	act := runAdvisor(t, s,
		func(_ context.Context, _ any) (any, error) {
			calls++
			time.Sleep(20 * time.Millisecond)
			return "result", nil
		},
		"dedup_store=locks",
	)

	ctx := context.Background()
	req := map[string]any{"id": "same-key"}
	done := make(chan struct{}, 2)
	go func() { _, _ = act.DoAny(ctx, req); done <- struct{}{} }()
	go func() { _, _ = act.DoAny(ctx, req); done <- struct{}{} }()
	<-done
	<-done

	if calls != 1 {
		t.Fatalf("after concurrent calls: handler calls = %d, want 1 (deduped)", calls)
	}
}

func TestAdvisor_DedupStore_RejectsUnknownStore(t *testing.T) {
	s := newAdviseStore(t)
	err := runAdvisorErr(t, s, "dedup_store=nosuchstore")
	if err == nil {
		t.Fatal("expected error for unknown store")
	}
}

func TestAdvisor_DedupStore_RejectsWrongKind(t *testing.T) {
	s := newAdviseStore(t, storage.Declaration{
		Name:    "cache",
		Backend: "memory",
		Kind:    storage.KindKV,
	})
	err := runAdvisorErr(t, s, "dedup_store=cache")
	if err == nil {
		t.Fatal("expected error for wrong-kind store")
	}
}

// ---------------------------------------------------------------------------
// Unrelated modifiers pass through untouched
// ---------------------------------------------------------------------------

// TestAdvisor_PassesUnrelatedModifiers confirms that the advisor only
// consumes the three storage-specific modifiers and leaves everything
// else for the compiler's modifier table.
func TestAdvisor_PassesUnrelatedModifiers(t *testing.T) {
	s := newAdviseStore(t, storage.Declaration{
		Name:    "cache",
		Backend: "memory",
		Kind:    storage.KindKV,
	})

	adv := storageAdvisor(s)
	atom := &compiler.Atom{
		Name:      "test.action",
		Modifiers: []string{"timeout=5s", "cache_store=cache", "retry=3"},
	}
	builder := action.New("test.action",
		func(_ context.Context, _ any) (any, error) { return nil, nil })

	if err := adv(atom, builder); err != nil {
		t.Fatalf("advisor: %v", err)
	}

	want := []string{"timeout=5s", "retry=3"}
	if len(atom.Modifiers) != len(want) {
		t.Fatalf("modifiers = %v, want %v", atom.Modifiers, want)
	}
	for i, m := range want {
		if atom.Modifiers[i] != m {
			t.Fatalf("modifiers[%d] = %q, want %q", i, atom.Modifiers[i], m)
		}
	}
}
