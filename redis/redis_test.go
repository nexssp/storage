package redis

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/nexssp/kernel/xctx"

	"github.com/nexssp/storage"
)

func TestRedisKVBlobAndLock(t *testing.T) {
	server := miniredis.RunT(t)
	ctx := context.Background()
	options := map[string]string{"addr": server.Addr(), "namespace": "test", "ttl": "1m"}

	kvInst, err := NewKV().Open(options)
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	defer kvInst.Close()
	kv, err := storage.AssertKV(kvInst)
	if err != nil {
		t.Fatal(err)
	}
	if _, hit, err := kv.Get(ctx, "missing"); err != nil || hit {
		t.Fatalf("missing key: hit=%v err=%v", hit, err)
	}
	if err := kv.Set(ctx, "key", map[string]any{"ok": true}, 0); err != nil {
		t.Fatal(err)
	}
	value, hit, err := kv.Get(ctx, "key")
	valueMap, mapOK := value.(map[string]any)
	if err != nil || !hit || !mapOK || valueMap["ok"] != true {
		t.Fatalf("get: value=%#v hit=%v err=%v", value, hit, err)
	}
	if err := kv.Set(ctx, "expires", "soon", 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	server.FastForward(30 * time.Millisecond)
	if _, expiredHit, expirationErr := kv.Get(ctx, "expires"); expirationErr != nil || expiredHit {
		t.Fatalf("expired key: hit=%v err=%v", expiredHit, expirationErr)
	}
	deleter, ok := kvInst.(interface {
		Delete(context.Context, string) error
	})
	if !ok {
		t.Fatal("KV instance does not implement Delete")
	}
	if err := deleter.Delete(ctx, "key"); err != nil {
		t.Fatal(err)
	}

	blobInst, err := NewBlob().Open(options)
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

	lockInst, err := NewLock().Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer lockInst.Close()
	lock, err := storage.AssertMutex(lockInst)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := lock.TryLock(ctx, "resource", time.Minute); err != nil || !ok {
		t.Fatalf("first lock: ok=%v err=%v", ok, err)
	}
	if ok, err := lock.TryLock(ctx, "resource", time.Minute); err != nil || ok {
		t.Fatalf("second lock: ok=%v err=%v", ok, err)
	}
	if err := lock.Unlock(ctx, "resource"); err != nil {
		t.Fatal(err)
	}
	if ok, err := lock.TryLock(ctx, "resource", time.Minute); err != nil || !ok {
		t.Fatalf("re-lock: ok=%v err=%v", ok, err)
	}
}

func TestRedisOptionsValidation(t *testing.T) {
	if _, err := NewKV().Open(nil); err == nil {
		t.Fatal("expected required addr error")
	}
	if _, err := NewKV().Open(map[string]string{"addr": "localhost:6379", "db": "-1"}); err == nil {
		t.Fatal("expected invalid db error")
	}
	if _, err := NewKV().Open(map[string]string{"addr": "localhost:6379", "unknown": "x"}); err == nil {
		t.Fatal("expected unknown option error")
	}
	if _, err := NewKV().Open(map[string]string{"addr": "localhost:6379", "tenant_scope": "caller"}); err == nil {
		t.Fatal("expected invalid tenant_scope error")
	}
	if _, err := NewLock().Open(map[string]string{"addr": "localhost:6379", "tenant_scope": "tenant"}); err == nil {
		t.Fatal("expected tenant_scope to be rejected for lock backend")
	}
	if _, err := NewBlob().Open(map[string]string{"addr": "localhost:6379", "tenant_scope": "tenant"}); err == nil {
		t.Fatal("expected tenant_scope to be rejected for blob backend")
	}
}

func TestRedisTenantScopedKV(t *testing.T) {
	server := miniredis.RunT(t)
	inst, err := NewKV().Open(map[string]string{"addr": server.Addr(), "namespace": "tenant-test", "tenant_scope": "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	kv, err := storage.AssertKV(inst)
	if err != nil {
		t.Fatal(err)
	}
	tenantA := xctx.WithTenantID(context.Background(), "tenant-a")
	tenantB := xctx.WithTenantID(context.Background(), "tenant-b")
	if _, _, err := kv.Get(context.Background(), "key"); err == nil {
		t.Fatal("expected missing tenant context to fail")
	}
	if err := kv.Set(context.Background(), "key", "value", 0); err == nil {
		t.Fatal("expected Set without tenant context to fail")
	}
	deleter, ok := inst.(interface {
		Delete(context.Context, string) error
	})
	if !ok {
		t.Fatal("KV instance does not implement Delete")
	}
	if err := deleter.Delete(context.Background(), "key"); err == nil {
		t.Fatal("expected Delete without tenant context to fail")
	}
	if err := kv.Set(tenantA, "key", "a", 0); err != nil {
		t.Fatal(err)
	}
	if _, hit, err := kv.Get(tenantB, "key"); err != nil || hit {
		t.Fatalf("tenant B must not see tenant A's key: hit=%v err=%v", hit, err)
	}
	if err := kv.Set(tenantB, "key", "b", 0); err != nil {
		t.Fatal(err)
	}
	value, hit, err := kv.Get(tenantA, "key")
	if err != nil || !hit || value != "a" {
		t.Fatalf("tenant A's value changed: value=%v hit=%v err=%v", value, hit, err)
	}
	if err := deleter.Delete(tenantB, "key"); err != nil {
		t.Fatal(err)
	}
	if value, hit, err := kv.Get(tenantA, "key"); err != nil || !hit || value != "a" {
		t.Fatalf("tenant B's delete affected tenant A: value=%v hit=%v err=%v", value, hit, err)
	}
}

func TestRedisKeyspacePrefixIsVersioned(t *testing.T) {
	server := miniredis.RunT(t)
	for _, tc := range []struct {
		name      string
		namespace string
		want      string
	}{
		{name: "default namespace", want: "nexss:storage:v1:"},
		{name: "custom namespace gets version", namespace: "tenant-a", want: "tenant-a:v1:"},
		{name: "existing version is not duplicated", namespace: "tenant-a:v1", want: "tenant-a:v1:"},
		{name: "leading separator before version is normalized", namespace: ":v1", want: "v1:"},
		{name: "newer version is preserved", namespace: "tenant-a:v2", want: "tenant-a:v2:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := map[string]string{"addr": server.Addr()}
			if tc.namespace != "" {
				opts["namespace"] = tc.namespace
			}
			inst, err := NewKV().Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer inst.Close()
			concrete, ok := inst.(*instance)
			if !ok {
				t.Fatalf("KV instance has unexpected type %T", inst)
			}
			key := concrete.key("sample")
			if !strings.HasPrefix(key, tc.want) {
				t.Fatalf("key %q does not start with versioned prefix %q", key, tc.want)
			}
		})
	}
}

func TestRedisUnlockRequiresLocalOwner(t *testing.T) {
	server := miniredis.RunT(t)
	ctx := context.Background()
	opts := map[string]string{"addr": server.Addr(), "namespace": "unlock-test"}

	ownerInstance, err := NewLock().Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer ownerInstance.Close()
	otherInstance, err := NewLock().Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer otherInstance.Close()
	owner, _ := storage.AssertMutex(ownerInstance)
	other, _ := storage.AssertMutex(otherInstance)

	if ok, err := owner.TryLock(ctx, "resource", time.Minute); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	if err := other.Unlock(ctx, "resource"); err == nil {
		t.Fatal("unlock without a local owner must fail")
	}
	if ok, err := other.TryLock(ctx, "resource", time.Minute); err != nil || ok {
		t.Fatalf("foreign unlock released the lock: ok=%v err=%v", ok, err)
	}
	if err := owner.Unlock(ctx, "resource"); err != nil {
		t.Fatal(err)
	}
}

func TestRedisLockOwnerTablePrunesExpiredEntriesAndIsBounded(t *testing.T) {
	server := miniredis.RunT(t)
	inst, err := NewLock().Open(map[string]string{"addr": server.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	concrete, ok := inst.(*instance)
	if !ok {
		t.Fatalf("opened instance has type %T, want *instance", inst)
	}

	concrete.mu.Lock()
	for index := range maxLockOwners {
		concrete.owners[strings.Repeat("x", index+1)] = lockOwner{token: "expired", expiresAt: time.Now().Add(-time.Second)}
	}
	concrete.mu.Unlock()
	if ok, err := concrete.TryLock(context.Background(), "fresh", time.Minute); err != nil || !ok {
		t.Fatalf("acquire after pruning: ok=%v err=%v", ok, err)
	}
	concrete.mu.Lock()
	got := len(concrete.owners)
	concrete.mu.Unlock()
	if got != 1 {
		t.Fatalf("owner table size = %d, want 1 after stale-entry pruning", got)
	}

	concrete.mu.Lock()
	for index := range maxLockOwners {
		concrete.owners[strings.Repeat("y", index+1)] = lockOwner{token: "active", expiresAt: time.Now().Add(time.Minute)}
	}
	concrete.mu.Unlock()
	if _, err := concrete.TryLock(context.Background(), "over-capacity", time.Minute); err == nil {
		t.Fatal("expected owner table capacity error")
	}
}
