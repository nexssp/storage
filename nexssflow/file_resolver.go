package nexssflow

import (
	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/xfile"

	"github.com/nexssp/storage"
)

// WithBlobStoreFileResolver returns a bundle with an explicit provider-scoped
// binding to a Storage BlobStore. It does not change Bundle's defaults: hosts
// must opt in by calling this helper and including the returned bundle in
// runner.BuildConfig. The BlobStore still authorizes each Open from the
// request's authenticated caller and tenant context; the Ref is not authority.
//
// A binding applies to every reference for provider, so use the provider from
// a trusted Storage-generated reference and avoid binding multiple stores with
// the same provider unless their resolution order is intentional.
func WithBlobStoreFileResolver(bundle compiler.Bundle, provider xfile.Provider, blobs storage.BlobStore) compiler.Bundle {
	return compiler.BundleWithFileResolver(bundle, provider, blobs)
}
