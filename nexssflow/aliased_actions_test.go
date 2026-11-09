package nexssflow

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/nexssp/kernel/action"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/memory"
)

type recordingIdemInstance struct {
	entry action.IdempotencyEntry
	key   string
	token string
	ttl   time.Duration
}

func (r *recordingIdemInstance) Kind() storage.Kind { return storage.KindIdem }
func (r *recordingIdemInstance) Close() error       { return nil }
func (r *recordingIdemInstance) Get(context.Context, string) (action.IdempotencyEntry, bool) {
	return action.IdempotencyEntry{}, false
}

func (r *recordingIdemInstance) Set(context.Context, string, action.IdempotencyEntry, time.Duration) {
}

func (r *recordingIdemInstance) Claim(context.Context, string, string, time.Duration) (action.IdempotencyClaim, error) {
	return action.IdempotencyClaim{}, nil
}

func (r *recordingIdemInstance) Complete(_ context.Context, key, token string, entry action.IdempotencyEntry, ttl time.Duration) error {
	r.entry = entry
	r.key = key
	r.token = token
	r.ttl = ttl
	return nil
}
func (r *recordingIdemInstance) Release(context.Context, string, string) error { return nil }

func actionStore(inst storage.Instance, decl storage.Declaration) *store {
	return &store{
		decls: map[string]storage.Declaration{decl.Name: decl},
		insts: map[string]storage.Instance{decl.Name: inst},
	}
}

func TestAliasedIdemCompleteUsesRequestHashAndDeclaredOrRequestedTTL(t *testing.T) {
	for _, tc := range []struct {
		name      string
		declared  string
		requested string
		wantTTL   time.Duration
	}{
		{name: "request overrides declaration", declared: "7m", requested: "23s", wantTTL: 23 * time.Second},
		{name: "declaration default", declared: "7m", wantTTL: 7 * time.Minute},
		{name: "kernel default", wantTTL: action.DefaultIdempotencyTTL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &recordingIdemInstance{}
			decl := storage.Declaration{Name: "idem", Kind: storage.KindIdem, Options: map[string]string{}}
			if tc.declared != "" {
				decl.Options["ttl"] = tc.declared
			}
			act := aliasedIdemComplete(actionStore(recorder, decl), "idem")
			request := map[string]any{
				"key": "request-1", "token": "claim-token", "request_hash": "sha256:payload-hash", "body": map[string]any{"ok": true},
			}
			if tc.requested != "" {
				request["ttl"] = tc.requested
			}
			if _, err := act.DoAny(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if recorder.key != "request-1" || recorder.token != "claim-token" {
				t.Fatalf("completion key/token = %q/%q", recorder.key, recorder.token)
			}
			if recorder.entry.RequestHash != "sha256:payload-hash" {
				t.Fatalf("request hash = %q, want caller-supplied request hash", recorder.entry.RequestHash)
			}
			if recorder.entry.RequestHash == recorder.token {
				t.Fatal("claim token was stored as request hash")
			}
			if recorder.ttl != tc.wantTTL {
				t.Fatalf("completion TTL = %s, want %s", recorder.ttl, tc.wantTTL)
			}
		})
	}
}

func TestAliasedIdemCompleteRequiresRequestHash(t *testing.T) {
	recorder := &recordingIdemInstance{}
	decl := storage.Declaration{Name: "idem", Kind: storage.KindIdem}
	act := aliasedIdemComplete(actionStore(recorder, decl), "idem")
	if _, err := act.DoAny(context.Background(), map[string]any{"key": "k", "token": "t", "body": "response"}); err == nil {
		t.Fatal("expected missing request_hash to fail")
	}
	if recorder.key != "" {
		t.Fatal("completion was called without request_hash")
	}
}

func TestAliasedIdemClaimReturnsCompletedEntry(t *testing.T) {
	decl := storage.Declaration{Name: "idem", Kind: storage.KindIdem, Options: map[string]string{"ttl": "1h"}}
	inst, err := memory.NewIdem().Open(decl.Options)
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	s := actionStore(inst, decl)
	claimAction := aliasedIdemClaim(s, "idem")
	completeAction := aliasedIdemComplete(s, "idem")
	ctx := context.Background()
	requestHash := "sha256:request"

	claim, err := claimAction.DoAny(ctx, map[string]any{"key": "request-1", "request_hash": requestHash})
	if err != nil {
		t.Fatal(err)
	}
	claimValue := reflect.ValueOf(claim)
	if got := claimValue.FieldByName("State").String(); got != "acquired" {
		t.Fatalf("initial claim state = %q, want acquired", got)
	}
	token := claimValue.FieldByName("Token").String()
	if token == "" {
		t.Fatal("initial claim returned an empty token")
	}
	if _, err := completeAction.DoAny(ctx, map[string]any{
		"key": "request-1", "token": token, "request_hash": requestHash, "body": map[string]any{"ok": true},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := claimAction.DoAny(ctx, map[string]any{"key": "request-1", "request_hash": requestHash})
	if err != nil {
		t.Fatal(err)
	}
	resultValue := reflect.ValueOf(result)
	if got := resultValue.FieldByName("State").String(); got != "completed" {
		t.Fatalf("replay claim state = %q, want completed", got)
	}
	entryValue := resultValue.FieldByName("Entry")
	if entryValue.IsNil() {
		t.Fatal("completed claim did not return its stored entry")
	}
	entry, ok := reflect.TypeAssert[*action.IdempotencyEntry](entryValue)
	if !ok {
		t.Fatalf("completed entry has type %T, want *action.IdempotencyEntry", entryValue.Interface())
	}
	if string(entry.Body) != `{"ok":true}` || entry.RequestHash != requestHash {
		t.Fatalf("replay entry = %+v, want stored body and request hash", entry)
	}
}

func TestAliasedFencedReleaseAcceptsTypedAndJSONLeases(t *testing.T) {
	inst, err := memory.NewFencedLock().Open(nil)
	if err != nil {
		t.Fatal(err)
	}
	decl := storage.Declaration{Name: "locks", Kind: storage.KindFencedLock}
	s := actionStore(inst, decl)
	acquire := aliasedFencedAcquire(s, "locks")
	release := aliasedFencedRelease(s, "locks")
	ctx := context.Background()

	result, err := acquire.DoAny(ctx, map[string]any{"key": "resource", "ttl": "1s"})
	if err != nil {
		t.Fatal(err)
	}
	value := reflect.ValueOf(result)
	lease, ok := reflect.TypeAssert[action.LockLease](value.FieldByName("Lease"))
	if !ok {
		t.Fatalf("acquire lease has type %T, want action.LockLease", value.FieldByName("Lease").Interface())
	}
	if _, err := release.DoAny(ctx, map[string]any{"lease": lease}); err != nil {
		t.Fatalf("release typed lease: %v", err)
	}
	if _, err := release.DoAny(ctx, map[string]any{"lease": lease}); err == nil {
		t.Fatal("release of stale lease must not report success")
	}

	result, err = acquire.DoAny(ctx, map[string]any{"key": "resource", "ttl": "1s"})
	if err != nil {
		t.Fatal(err)
	}
	value = reflect.ValueOf(result)
	lease, ok = reflect.TypeAssert[action.LockLease](value.FieldByName("Lease"))
	if !ok {
		t.Fatalf("acquire lease has type %T, want action.LockLease", value.FieldByName("Lease").Interface())
	}
	encoded, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	var jsonLease map[string]any
	if err := json.Unmarshal(encoded, &jsonLease); err != nil {
		t.Fatal(err)
	}
	if _, err := release.DoAny(ctx, map[string]any{"lease": jsonLease}); err != nil {
		t.Fatalf("release JSON-shaped lease: %v", err)
	}
}
