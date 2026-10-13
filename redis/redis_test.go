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
