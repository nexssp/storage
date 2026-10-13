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

## Flow bundle

`<alias>.blob.put` and `storage.blob.put` accept a `data` value; if it is an `io.Reader` it is passed through as a stream, while strings, byte slices, and other values are converted to readers. Put returns `{ref, ok}`. `<alias>.blob.get` and `storage.blob.get` return `{ref, hit}` and never return a byte slice or whole object. Go components that need the contents can resolve the returned typed `xfile.Ref` with the same BlobStore's `Open` method.
