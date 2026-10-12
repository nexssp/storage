package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"

	"github.com/nexssp/storage"
)

// KV conformance: implements action.CacheLayer[any], honors TTL,
// returns (nil, false, nil) for missing keys.

func TestKV_GetSet(t *testing.T) {
	inst, err := KV{}.Open(nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	layer, err := storage.AssertKV(inst)
	if err != nil {
		t.Fatalf("assert: %v", err)
	}
	ctx := context.Background()

	if _, hit, err := layer.Get(ctx, "missing"); err != nil || hit {
		t.Fatalf("missing key: got hit=%v err=%v", hit, err)
	}
	if err := layer.Set(ctx, "k", "v", 0); err != nil {
		t.Fatalf("set: %v", err)
	}
	val, hit, err := layer.Get(ctx, "k")
	if err != nil || !hit || val != "v" {
		t.Fatalf("get: val=%v hit=%v err=%v", val, hit, err)
	}
}

func TestKV_TTL(t *testing.T) {
	inst, _ := KV{}.Open(nil)
	layer, _ := storage.AssertKV(inst)
	ctx := context.Background()

	if err := layer.Set(ctx, "k", "v", 50*time.Millisecond); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, hit, _ := layer.Get(ctx, "k"); !hit {
		t.Fatal("expected hit before TTL")
	}
	time.Sleep(80 * time.Millisecond)
	if _, hit, _ := layer.Get(ctx, "k"); hit {
		t.Fatal("expected miss after TTL")
	}
}

func TestKV_ContextCanceled(t *testing.T) {
	inst, _ := KV{}.Open(nil)
	layer, _ := storage.AssertKV(inst)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := layer.Get(ctx, "k"); err == nil {
		t.Fatal("expected ctx.Err() on Get")
	}
	if err := layer.Set(ctx, "k", "v", 0); err == nil {
		t.Fatal("expected ctx.Err() on Set")
	}
}

// Blob conformance.

func TestBlob_GetPutDelete(t *testing.T) {
	inst, err := Blob{}.Open(nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	store, err := storage.AssertBlob(inst)
	if err != nil {
		t.Fatalf("assert: %v", err)
	}
	ctx := context.Background()

	if _, hit, err := store.Get(ctx, "missing"); err != nil || hit {
		t.Fatalf("missing: hit=%v err=%v", hit, err)
	}
	if err := store.Put(ctx, "f", []byte("hello")); err != nil {
		t.Fatalf("put: %v", err)
	}
	data, hit, err := store.Get(ctx, "f")
	if err != nil || !hit || string(data) != "hello" {
		t.Fatalf("get: data=%q hit=%v err=%v", data, hit, err)
	}
	if err := store.Delete(ctx, "f"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, hit, _ := store.Get(ctx, "f"); hit {
		t.Fatal("expected miss after delete")
	}
}

// Blob must return a copy — caller mutation must not affect stored state.
func TestBlob_GetReturnsCopy(t *testing.T) {
	inst, _ := Blob{}.Open(nil)
	store, _ := storage.AssertBlob(inst)
	ctx := context.Background()

	original := []byte("abcdef")
	_ = store.Put(ctx, "k", original)
	got, _, _ := store.Get(ctx, "k")
	got[0] = 'X'
	got2, _, _ := store.Get(ctx, "k")
	if got2[0] != 'a' {
		t.Fatalf("stored data mutated: got %q", got2)
	}
}

// Lock conformance.

func TestLock_TryUnlock(t *testing.T) {
	inst, _ := Lock{}.Open(nil)
	m, _ := storage.AssertMutex(inst)
	ctx := context.Background()

	ok, err := m.TryLock(ctx, "k", time.Minute)
	if err != nil || !ok {
		t.Fatalf("trylock: ok=%v err=%v", ok, err)
	}
	ok, _ = m.TryLock(ctx, "k", time.Minute)
	if ok {
		t.Fatal("expected second TryLock to fail")
	}
	if err := m.Unlock(ctx, "k"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	ok, _ = m.TryLock(ctx, "k", time.Minute)
	if !ok {
		t.Fatal("expected re-lock after unlock")
	}
}

func TestLock_TTLExpiry(t *testing.T) {
	inst, _ := Lock{}.Open(nil)
	m, _ := storage.AssertMutex(inst)
	ctx := context.Background()

	if ok, _ := m.TryLock(ctx, "k", 50*time.Millisecond); !ok {
		t.Fatal("first TryLock failed")
	}
	time.Sleep(80 * time.Millisecond)
	if ok, _ := m.TryLock(ctx, "k", time.Minute); !ok {
		t.Fatal("expected re-lock after TTL expiry")
	}
}

func TestLock_RejectsZeroTTL(t *testing.T) {
	inst, _ := Lock{}.Open(nil)
	m, _ := storage.AssertMutex(inst)
	if _, err := m.TryLock(context.Background(), "k", 0); err == nil {
		t.Fatal("expected error on zero TTL")
	}
}

// FencedLock conformance.

func TestFencedLock_AcquireRenewRelease(t *testing.T) {
	inst, _ := FencedLock{}.Open(nil)
	m, _ := storage.AssertFencedMutex(inst)
	ctx := context.Background()

	lease1, ok, err := m.Acquire(ctx, "k", time.Minute)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if lease1.Fence == 0 {
		t.Fatal("expected non-zero fence")
	}
	lease2, ok, _ := m.Acquire(ctx, "k", time.Minute)
	if ok {
		t.Fatal("expected second Acquire to fail")
	}
	_ = lease2
	if renewed, _ := m.Renew(ctx, lease1, time.Minute); !renewed {
		t.Fatal("renew failed for valid lease")
	}
	if _, _ = m.Release(ctx, lease1); false {
		_ = ""
	}
	lease3, ok, _ := m.Acquire(ctx, "k", time.Minute)
	if !ok {
		t.Fatal("expected re-acquire after release")
	}
	if lease3.Fence <= lease1.Fence {
		t.Fatalf("expected fence to increase: %v -> %v", lease1.Fence, lease3.Fence)
	}
}

func TestFencedLock_RenewWrongOwner(t *testing.T) {
	inst, _ := FencedLock{}.Open(nil)
	m, _ := storage.AssertFencedMutex(inst)
	ctx := context.Background()

	lease, ok, _ := m.Acquire(ctx, "k", time.Minute)
	if !ok {
		t.Fatal("acquire failed")
	}
	fake := lease
	fake.Owner = "other"
	if ok, _ := m.Renew(ctx, fake, time.Minute); ok {
		t.Fatal("expected renew with wrong owner to fail")
	}
}

// Idempotency conformance.

func TestIdem_SetGet(t *testing.T) {
	inst, _ := Idem{}.Open(nil)
	s, _ := storage.AssertIdempotencyStore(inst)
	ctx := context.Background()

	s.Set(ctx, "k", action.IdempotencyEntry{Body: []byte(`{"ok":1}`), RequestHash: "h1"}, time.Minute)
	entry, ok := s.Get(ctx, "k")
	if !ok || string(entry.Body) != `{"ok":1}` {
		t.Fatalf("get: entry=%+v ok=%v", entry, ok)
	}
}

func TestIdem_Coordinator(t *testing.T) {
	inst, _ := Idem{}.Open(nil)
	coord, ok := inst.(action.IdempotencyCoordinator)
	if !ok {
		t.Fatal("expected Idem Instance to also satisfy IdempotencyCoordinator")
	}
	ctx := context.Background()

	claim, err := coord.Claim(ctx, "k", "hash1", time.Minute)
	if err != nil || claim.State != action.IdempotencyClaimAcquired {
		t.Fatalf("claim: %+v err=%v", claim, err)
	}

	// Second claim with same hash -> InProgress.
	claim2, _ := coord.Claim(ctx, "k", "hash1", time.Minute)
	if claim2.State != action.IdempotencyClaimInProgress {
		t.Fatalf("expected InProgress, got %v", claim2.State)
	}

	// Complete.
	err = coord.Complete(ctx, "k", claim.Token, action.IdempotencyEntry{
		Body: []byte(`{"ok":1}`), RequestHash: "hash1",
	}, time.Minute)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}

	// Third claim -> Completed.
	claim3, _ := coord.Claim(ctx, "k", "hash1", time.Minute)
	if claim3.State != action.IdempotencyClaimCompleted {
		t.Fatalf("expected Completed, got %v", claim3.State)
	}

	// Mismatched hash -> Conflict.
	claim4, _ := coord.Claim(ctx, "k", "different", time.Minute)
	if claim4.State != action.IdempotencyClaimConflict {
		t.Fatalf("expected Conflict, got %v", claim4.State)
	}
}

// Journal conformance.

func TestJournal_GetRecordStep(t *testing.T) {
	inst, _ := Journal{}.Open(nil)
	j, _ := storage.AssertJournal(inst)
	ctx := context.Background()

	if _, ok, err := j.GetStep(ctx, "exec1", "step1"); err != nil || ok {
		t.Fatalf("missing: ok=%v err=%v", ok, err)
	}
	if err := j.RecordStep(ctx, "exec1", "step1", []byte("payload")); err != nil {
		t.Fatalf("record: %v", err)
	}
	data, ok, err := j.GetStep(ctx, "exec1", "step1")
	if err != nil || !ok || string(data) != "payload" {
		t.Fatalf("get: data=%q ok=%v err=%v", data, ok, err)
	}
}

// Tx conformance: invokes fn with the same ctx.

func TestTx_RunInTx(t *testing.T) {
	inst, _ := Tx{}.Open(nil)
	runner, _ := storage.AssertTxRunner(inst)
	ctx := context.Background()

	called := false
	err := runner.RunInTx(ctx, func(_ context.Context) error {
		called = true
		return nil
	})
	if err != nil || !called {
		t.Fatalf("RunInTx: called=%v err=%v", called, err)
	}

	expected := errors.New("rolled back")
	err = runner.RunInTx(ctx, func(_ context.Context) error {
		return expected
	})
	if !errors.Is(err, expected) {
		t.Fatalf("expected error passthrough, got %v", err)
	}
}

// RateLimit conformance.

func TestRateLimit_Allow(t *testing.T) {
	inst, err := RateLimit{}.Open(map[string]string{"rps": "10", "burst": "2"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rl, _ := storage.AssertRateLimiter(inst)
	ctx := context.Background()

	if ok, _ := rl.Allow(ctx, "k"); !ok {
		t.Fatal("first allow should pass (burst)")
	}
	if ok, _ := rl.Allow(ctx, "k"); !ok {
		t.Fatal("second allow should pass (burst)")
	}
	// Third call within the same instant must fail (burst exhausted).
	if ok, _ := rl.Allow(ctx, "k"); ok {
		t.Fatal("third allow should fail (bucket empty)")
	}
}
