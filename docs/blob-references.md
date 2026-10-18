# Blob references and streams

Storage Blob APIs use the shared `github.com/nexssp/kernel/xfile` reference contract. Blob payloads are not returned by lookup actions.

## Go API

```go
ref, err := blobs.Put(ctx, "reports/q4.json", strings.NewReader(payload))
if err != nil {
    return err
}

reader, info, err := blobs.Open(ctx, ref)
if err != nil {
    return err
}
defer reader.Close()
// Consume reader as a stream. info is confirmed metadata for the opened object.
```

`BlobStore` exposes:

- `Put(ctx, key, io.Reader) (xfile.Ref, error)` — consumes the input stream and replaces the key.
- `Get(ctx, key) (xfile.Ref, bool, error)` — metadata-only lookup returning a compact typed handle, not the payload.
- `Open(ctx, ref) (io.ReadCloser, xfile.Info, error)` — implements `xfile.Resolver`; callers own and must close the reader.
- `Delete(ctx, key) error` — deletes only an object owned by the current authenticated scope.

The current memory, Redis, and SQL implementations expose stream-based methods but internally buffer payloads because their current backing data layouts and database/Redis clients accept whole values. The API does not promise constant-memory or zero-copy transfer yet. Flow action results remain small references regardless of payload size.

## Reference and metadata semantics

`xfile.Ref` is an opaque provider-scoped selector. Storage encodes the logical key in an opaque ID; it is not an operating-system path and conveys no authority. Unsupported non-empty versions are rejected.

`ExpectedSize` and `ExpectedDigest` in a reference are integrity expectations, not confirmed facts. Storage currently supports explicit `sha256:<hex>` digests. `Put` and metadata-only `Get` populate these expectations from the stored object metadata. `Open` checks those expectations against the object and returns authoritative `xfile.Info` separately, with confirmed `Size` and `Digest`; it never places confirmed facts into the `Ref.Expected*` fields.

## Caller and tenant scope

Every Blob operation requires both authenticated caller and tenant IDs in the request context, as read by `xfile.ScopeFromContext`. Writes bind the object to that caller and tenant. A lookup for another scope is concealed as a miss; opening another scope's reference returns not found. Authorization is based on context and stored ownership metadata, never on `Ref` fields.

For persistent SQL adapters, the adapter-owned Blob table stores byte count, digest, caller, and tenant metadata beside the object. Apply the embedded Goose migration explicitly before use; opening a backend never creates or changes schema. Redis stores the same metadata alongside the existing versioned keyspace entries.

## Passing a Storage reference to Sandbox

Storage's `BlobStore` already implements `xfile.Resolver`. A host can make one chosen BlobStore available to Flow's Sandbox bundle by explicitly adding a provider binding to the bundle list used to build the runner configuration:

```go
storageFiles := storageflow.WithBlobStoreFileResolver(
    compiler.Bundle{ID: "app.storage-files"},
    ref.Provider, // use the provider from a trusted Storage-generated Ref
    blobs,
)
bundles = append(bundles, storageFiles)
cfg, err := runner.BuildConfig(bundles) // bundles also contains Sandbox and the app's normal bundles
```

Then pass the typed `ref` as one entry in Sandbox's `files` map, with a separate workspace-relative destination such as `input.txt`. This is explicit host wiring: importing Storage or using Sandbox's ordinary Flow bundle does not make a BlobStore available automatically. The binding routes references for that provider to the selected BlobStore; it is not a per-object allowlist. Do not bind multiple stores with the same provider unless their resolver order is intentional.

Every Sandbox execution must still carry the authenticated caller and tenant in its context. The selected BlobStore authorizes every `Open` from that context and conceals another caller's or tenant's object as not found; reference fields do not grant access. Sandbox separately validates the destination, verifies returned identity/size/digest, enforces its per-file limit, and stages content before installation. The host should keep the normal static command/profile policy enabled for untrusted inputs and use a real isolated engine; the in-process engine is not an isolation boundary.

## Flow bundle

`<alias>.blob.put` and `storage.blob.put` accept a `data` value; if it is an `io.Reader` it is passed through as a stream, while strings, byte slices, and other values are converted to readers. Put returns `{ref, ok}`. `<alias>.blob.get` and `storage.blob.get` return `{ref, hit}` and never return a byte slice or whole object. Go components that need the contents can resolve the returned typed `xfile.Ref` with the same BlobStore's `Open` method.
