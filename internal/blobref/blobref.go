// Package blobref contains shared helpers for storage BlobStore adapters.
package blobref

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/nexssp/kernel/xfile"

	"github.com/nexssp/storage"
)

const idPrefix = "k1_"

// Metadata is information stored beside a blob. Owner fields are always
// populated from authenticated context, never from an xfile.Ref.
type Metadata struct {
	Size     int64  `json:"size"`
	Digest   string `json:"digest"`
	CallerID string `json:"caller_id"`
	TenantID string `json:"tenant_id"`
}

// Scope returns the authenticated caller and tenant for a Blob operation.
func Scope(ctx context.Context) (xfile.Scope, error) {
	return xfile.ScopeFromContext(ctx)
}

// NewRef creates an opaque reference for a provider-scoped storage key.
func NewRef(provider, key string, meta Metadata) xfile.Ref {
	return xfile.Ref{
		Provider:       xfile.Provider(provider),
		ID:             xfile.ID(idPrefix + base64.RawURLEncoding.EncodeToString([]byte(key))),
		ExpectedSize:   xfile.ExpectedSize(meta.Size),
		ExpectedDigest: xfile.ExpectedDigest(meta.Digest),
	}
}

// ScopedKey derives an internal storage key from authenticated scope and the
// caller's logical key. Length prefixes avoid delimiter ambiguity.
func ScopedKey(scope xfile.Scope, key string) string {
	return fmt.Sprintf("%d:%s%d:%s%d:%s", len(scope.TenantID), scope.TenantID, len(scope.CallerID), scope.CallerID, len(key), key)
}

// Key validates a reference for provider and decodes its opaque key. It does
// not authorize access; callers must separately compare stored owner metadata
// with the context-derived scope.
func Key(provider string, ref xfile.Ref) (string, error) {
	if ref.Provider == "" || string(ref.Provider) != provider {
		return "", storage.BadRequestf("blob: reference provider %q does not match %q", ref.Provider, provider)
	}
	if ref.Version != "" {
		return "", storage.BadRequest("blob: versioned references are not supported by this backend")
	}
	if ref.ExpectedSize < 0 {
		return "", storage.BadRequest("blob: expected size cannot be negative")
	}
	if ref.ExpectedDigest != "" {
		if err := ValidateDigest(string(ref.ExpectedDigest)); err != nil {
			return "", err
		}
	}
	encoded := string(ref.ID)
	if !strings.HasPrefix(encoded, idPrefix) {
		return "", storage.BadRequest("blob: malformed opaque reference ID")
	}
	key, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(encoded, idPrefix))
	if err != nil {
		return "", storage.BadRequestf("blob: malformed opaque reference ID: %v", err)
	}
	if idPrefix+base64.RawURLEncoding.EncodeToString(key) != encoded {
		return "", storage.BadRequest("blob: non-canonical opaque reference ID")
	}
	return string(key), nil
}

// Digest returns the byte count and explicit sha256-prefixed digest.
func Digest(data []byte) (size int64, digest string) {
	sum := sha256.Sum256(data)
	return int64(len(data)), "sha256:" + hex.EncodeToString(sum[:])
}

// MetadataFor records confirmed byte properties bound to an authenticated scope.
func MetadataFor(scope xfile.Scope, data []byte) Metadata {
	size, digest := Digest(data)
	return Metadata{Size: size, Digest: digest, CallerID: scope.CallerID, TenantID: scope.TenantID}
}

// ValidateDigest accepts the explicit digest form supported by Storage.
func ValidateDigest(digest string) error {
	algorithm, value, ok := strings.Cut(digest, ":")
	if !ok || algorithm != "sha256" || len(value) != sha256.Size*2 {
		return storage.BadRequest("blob: expected digest must use sha256:<hex>")
	}
	if _, err := hex.DecodeString(value); err != nil {
		return storage.BadRequestf("blob: malformed sha256 digest: %v", err)
	}
	return nil
}

// Verify checks caller-supplied expected metadata against confirmed values.
func Verify(ref xfile.Ref, size int64, digest string) error {
	if ref.ExpectedSize != 0 && int64(ref.ExpectedSize) != size {
		return storage.BadRequestf("blob: expected size %d does not match confirmed size %d", ref.ExpectedSize, size)
	}
	if ref.ExpectedDigest != "" && !strings.EqualFold(string(ref.ExpectedDigest), digest) {
		return storage.BadRequest("blob: expected digest does not match confirmed digest")
	}
	return nil
}

// Authorized reports whether a stored object belongs to scope. Empty owner
// fields never authorize access, including for legacy/unscoped records.
func Authorized(scope xfile.Scope, meta Metadata) bool {
	return meta.CallerID != "" && meta.TenantID != "" &&
		meta.CallerID == scope.CallerID && meta.TenantID == scope.TenantID
}

// Info creates authoritative metadata for an opened object. It deliberately
// does not populate xfile.Ref.Expected* fields.
func Info(ref xfile.Ref, size int64, digest string) xfile.Info {
	return xfile.Info{
		Provider: ref.Provider,
		ID:       ref.ID,
		Version:  ref.Version,
		Size:     size,
		Digest:   digest,
	}
}

// ConfirmStored verifies that bytes still match the metadata stored by the
// backend, detecting corruption independently of any caller expectation.
func ConfirmStored(data []byte, meta Metadata) (size int64, digest string, resultErr error) {
	size, digest = Digest(data)
	if size != meta.Size || !strings.EqualFold(digest, meta.Digest) {
		return 0, "", storage.Internalf("blob: stored metadata mismatch (size %d/%d, digest %s/%s)", meta.Size, size, meta.Digest, digest)
	}
	return size, digest, nil
}

// CheckOwner returns a non-disclosing not-found error for records owned by a
// different authenticated scope.
func CheckOwner(scope xfile.Scope, meta Metadata) error {
	if !Authorized(scope, meta) {
		return storage.NotFound("blob: reference not found")
	}
	return nil
}

// MetadataFromBytes records the confirmed properties of newly accepted data.
func MetadataFromBytes(ctx context.Context, data []byte) (Metadata, error) {
	scope, err := Scope(ctx)
	if err != nil {
		return Metadata{}, err
	}
	return MetadataFor(scope, data), nil
}

// ValidateMetadata rejects incomplete owner or digest state returned by a
// persistent backend, rather than manufacturing a reference from bad data.
func ValidateMetadata(meta Metadata) error {
	if meta.Size < 0 || meta.CallerID == "" || meta.TenantID == "" {
		return storage.Internal("blob: backend returned incomplete metadata")
	}
	if err := ValidateDigest(meta.Digest); err != nil {
		return storage.Internalf("blob: backend returned invalid digest: %v", err)
	}
	return nil
}

// Describe is used in backend diagnostics without exposing payload bytes.
func Describe(ref xfile.Ref) string {
	return fmt.Sprintf("provider=%q id=%q", ref.Provider, ref.ID)
}
