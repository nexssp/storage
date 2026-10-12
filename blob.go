package storage

import (
	"context"
)

// BlobStore is the contract for opaque byte-slice storage without TTL
// semantics. It is intentionally separate from action.CacheLayer[V]:
//
//   - CacheLayer is for typed values with per-entry TTL. It powers
//     read-through caching and singleflight stampede protection.
//   - BlobStore is for opaque payloads (files, snapshots, documents)
//     where the caller owns the encoding and lifecycle. A Blob has no
//     TTL; deletion is explicit.
//
// This interface is defined here, in package storage, to keep the
// prototype self-contained. The migration plan (see
// docs/promote-to-kernel.md) is to move it to package action once the
// first non-memory backend (e.g., S3) validates the shape.
//
// Until promotion, both the storage package and Flow extension import
// it from here. After promotion, the storage package will re-export
// action.BlobStore as storage.BlobStore for backwards compatibility.
type BlobStore interface {
	// Get returns the bytes for key. A missing key returns
	// (nil, false, nil) — a missing blob is not an error. Storage
	// failures return (nil, false, err) with a classified xerr kind.
	Get(ctx context.Context, key string) (data []byte, hit bool, err error)

	// Put stores data under key. The implementation MUST be
	// idempotent: re-putting the same key overwrites. Storage
	// failures return a classified xerr.
	Put(ctx context.Context, key string, data []byte) error

	// Delete removes key. A missing key is not an error. Storage
	// failures return a classified xerr.
	Delete(ctx context.Context, key string) error
}
