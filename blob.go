package storage

import (
	"context"
	"io"

	"github.com/nexssp/kernel/xfile"
)

// BlobStore stores opaque payloads without TTL semantics. Unlike a KV cache,
// BlobStore is intended for files, documents, snapshots, and other large data.
//
// Put consumes src and returns an opaque, provider-scoped reference. Get is a
// metadata-only lookup: it returns a compact xfile.Ref, never the object bytes.
// Open streams an object identified by a reference and returns confirmed
// metadata separately from the caller-supplied expectations in xfile.Ref.
// Implementations scope operations to the authenticated caller and tenant in
// ctx; references are untrusted selectors, not authorization credentials.
type BlobStore interface {
	// Get looks up key and returns a small reference. A missing or inaccessible
	// key returns (zero, false, nil). The reference carries expected metadata
	// when the backend has it; it never contains the payload.
	Get(ctx context.Context, key string) (ref xfile.Ref, hit bool, err error)

	// Put streams src under key, replacing any existing object. It returns a
	// typed reference with integrity expectations for the bytes consumed.
	Put(ctx context.Context, key string, src io.Reader) (xfile.Ref, error)

	// Open resolves ref and returns a reader owned by the caller plus confirmed
	// metadata for the opened object. Expected metadata in ref is verified.
	Open(ctx context.Context, ref xfile.Ref) (io.ReadCloser, xfile.Info, error)

	// Delete removes key for the authenticated caller and tenant. A missing or
	// inaccessible key is not an error.
	Delete(ctx context.Context, key string) error
}
