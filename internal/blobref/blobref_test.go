package blobref

import (
	"strings"
	"testing"

	"github.com/nexssp/kernel/xfile"
)

func TestRefIsOpaqueAndKeepsExpectedMetadataSeparate(t *testing.T) {
	meta := Metadata{Size: 7, Digest: "sha256:" + strings.Repeat("a", 64), CallerID: "user", TenantID: "tenant"}
	ref := NewRef("memory", "reports/2026/q4.json", meta)
	if ref.Provider != xfile.Provider("memory") || strings.Contains(string(ref.ID), "/") || strings.Contains(string(ref.ID), "reports") {
		t.Fatalf("reference does not contain an opaque ID: %+v", ref)
	}
	key, err := Key("memory", ref)
	if err != nil || key != "reports/2026/q4.json" {
		t.Fatalf("opaque ID round-trip: key=%q err=%v", key, err)
	}
	info := Info(ref, 7, meta.Digest)
	if info.Size != 7 || info.Digest != meta.Digest || ref.ExpectedSize != 7 || string(ref.ExpectedDigest) != info.Digest {
		t.Fatalf("expected and confirmed metadata were not retained independently: ref=%+v info=%+v", ref, info)
	}
	if info.Provider != ref.Provider || info.ID != ref.ID {
		t.Fatalf("confirmed info does not identify resolved reference: %+v", info)
	}
}

func TestKeyRejectsUnsupportedOrMalformedReferences(t *testing.T) {
	base := NewRef("memory", "key", Metadata{Size: 1, Digest: "sha256:" + strings.Repeat("b", 64)})
	cases := []struct {
		name string
		ref  xfile.Ref
	}{
		{name: "wrong provider", ref: func() xfile.Ref { r := base; r.Provider = "redis"; return r }()},
		{name: "unsupported version", ref: func() xfile.Ref { r := base; r.Version = "v1"; return r }()},
		{name: "negative size", ref: func() xfile.Ref { r := base; r.ExpectedSize = -1; return r }()},
		{name: "malformed digest", ref: func() xfile.Ref { r := base; r.ExpectedDigest = "sha256:not-hex"; return r }()},
		{name: "malformed ID", ref: func() xfile.Ref { r := base; r.ID = "not-an-opaque-id"; return r }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Key("memory", tc.ref); err == nil {
				t.Fatal("expected reference to be rejected")
			}
		})
	}
}

func TestScopedKeyIncludesBothCallerAndTenant(t *testing.T) {
	base := xfile.Scope{CallerID: "user", TenantID: "tenant"}
	if ScopedKey(base, "same") == ScopedKey(xfile.Scope{CallerID: "other", TenantID: "tenant"}, "same") {
		t.Fatal("different callers share an internal key")
	}
	if ScopedKey(base, "same") == ScopedKey(xfile.Scope{CallerID: "user", TenantID: "other"}, "same") {
		t.Fatal("different tenants share an internal key")
	}
	if ScopedKey(base, "same") == ScopedKey(xfile.Scope{CallerID: "user", TenantID: "tenant"}, "same-other") {
		t.Fatal("different logical keys share an internal key")
	}
}
