// Package memory provides reference in-process implementations of
// every storage.Kind. They are intended for development, testing, and
// single-process flows where durability is not required.
//
// Every Backend here is registered by the Flow storage extension's
// Bundle() so a freshly-installed Flow has working storage without
// any external dependency. Production deployments register
// additional Backends (Redis, S3, Postgres, ...) via @require.
//
// Memory backends are safe for concurrent use. They do not enforce
// size bounds; unbounded growth in production will exhaust memory.
// Use them in production only for short-lived state (request-scoped
// caches, test fixtures).
package memory

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xfile"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/internal/blobref"
)

// backendName is the Name() value shared by every in-process backend
// in this package. All memory backends register under the same Name;
// the (Name, Kind) pair keeps them distinct in the Registry.
const backendName = "memory"

// ---------------------------------------------------------------------------
// KindKV — action.CacheLayer[any]
// ---------------------------------------------------------------------------

// KV is the in-memory CacheLayer backend.
type KV struct{}

// NewKV returns a KV backend.
func NewKV() *KV { return &KV{} }

func (KV) Name() string       { return backendName }
func (KV) Kind() storage.Kind { return storage.KindKV }

func (KV) Open(opts map[string]string) (storage.Instance, error) {
	ttl, err := storage.ParseDuration(opts["ttl"], 0)
	if err != nil {
		return nil, err
	}
	return &kvInstance{
		data: make(map[string]kvItem),
		ttl:  ttl,
	}, nil
}

type kvItem struct {
	val any
	exp time.Time
}

type kvInstance struct {
	mu   sync.RWMutex
	data map[string]kvItem
	ttl  time.Duration
}

func (k *kvInstance) Kind() storage.Kind { return storage.KindKV }
func (k *kvInstance) Close() error       { return nil }

func (k *kvInstance) Get(ctx context.Context, key string) (val any, hit bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	now := time.Now()

	k.mu.RLock()
	item, ok := k.data[key]
	k.mu.RUnlock()

	if !ok {
		return nil, false, nil
	}
	if !item.exp.IsZero() && now.After(item.exp) {
		k.mu.Lock()
		if cur, ok := k.data[key]; ok && cur.exp.Equal(item.exp) {
			delete(k.data, key)
		}
		k.mu.Unlock()
		return nil, false, nil
	}
	return item.val, true, nil
}

func (k *kvInstance) Set(ctx context.Context, key string, val any, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var exp time.Time
	now := time.Now()
	switch {
	case ttl > 0:
		exp = now.Add(ttl)
	case k.ttl > 0:
		exp = now.Add(k.ttl)
	}
	k.mu.Lock()
	k.data[key] = kvItem{val: val, exp: exp}
	k.mu.Unlock()
	return nil
}

// Delete is an extension beyond action.CacheLayer. Callers may type-
// assert to *kvInstance to use it, or wait for the proposed
// action.KVStore interface (see docs/promote-to-kernel.md).
func (k *kvInstance) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	k.mu.Lock()
	delete(k.data, key)
	k.mu.Unlock()
	return nil
}

// Compile-time interface check.
var _ action.CacheLayer[any] = (*kvInstance)(nil)

// ---------------------------------------------------------------------------
// KindBlob — storage.BlobStore
// ---------------------------------------------------------------------------

// Blob is the in-memory BlobStore backend.
type Blob struct{}

func NewBlob() *Blob { return &Blob{} }

func (Blob) Name() string       { return backendName }
func (Blob) Kind() storage.Kind { return storage.KindBlob }

func (Blob) Open(_ map[string]string) (storage.Instance, error) {
	return &blobInstance{data: make(map[string]blobRecord)}, nil
}

type blobRecord struct {
	data []byte
	blobref.Metadata
}

type blobInstance struct {
	mu   sync.RWMutex
	data map[string]blobRecord
}

func (b *blobInstance) Kind() storage.Kind { return storage.KindBlob }
func (b *blobInstance) Close() error       { return nil }

func (b *blobInstance) Get(ctx context.Context, key string) (xfile.Ref, bool, error) {
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, false, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return xfile.Ref{}, false, err
	}
	storageKey := blobref.ScopedKey(scope, key)
	b.mu.RLock()
	record, ok := b.data[storageKey]
	b.mu.RUnlock()
	if !ok || !blobref.Authorized(scope, record.Metadata) {
		return xfile.Ref{}, false, nil
	}
	if err := blobref.ValidateMetadata(record.Metadata); err != nil {
		return xfile.Ref{}, false, err
	}
	return blobref.NewRef(backendName, key, record.Metadata), true, nil
}

func (b *blobInstance) Put(ctx context.Context, key string, src io.Reader) (xfile.Ref, error) {
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return xfile.Ref{}, err
	}
	if src == nil {
		return xfile.Ref{}, storage.BadRequest("blob: source reader is required")
	}
	data, err := io.ReadAll(src)
	if err != nil {
		return xfile.Ref{}, storage.BadRequestf("blob: read source: %v", err)
	}
	meta := blobref.MetadataFor(scope, data)
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, err
	}
	storageKey := blobref.ScopedKey(scope, key)
	b.mu.Lock()
	b.data[storageKey] = blobRecord{data: data, Metadata: meta}
	b.mu.Unlock()
	return blobref.NewRef(backendName, key, meta), nil
}

func (b *blobInstance) Open(ctx context.Context, ref xfile.Ref) (io.ReadCloser, xfile.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, xfile.Info{}, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	key, err := blobref.Key(backendName, ref)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	storageKey := blobref.ScopedKey(scope, key)
	b.mu.RLock()
	record, ok := b.data[storageKey]
	b.mu.RUnlock()
	if !ok {
		return nil, xfile.Info{}, storage.NotFound("blob: reference not found")
	}
	if ownerErr := blobref.CheckOwner(scope, record.Metadata); ownerErr != nil {
		return nil, xfile.Info{}, ownerErr
	}
	size, digest, err := blobref.ConfirmStored(record.data, record.Metadata)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	if err := blobref.Verify(ref, size, digest); err != nil {
		return nil, xfile.Info{}, err
	}
	return io.NopCloser(bytes.NewReader(record.data)), blobref.Info(ref, size, digest), nil
}

func (b *blobInstance) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return err
	}
	storageKey := blobref.ScopedKey(scope, key)
	b.mu.Lock()
	if record, ok := b.data[storageKey]; ok && blobref.Authorized(scope, record.Metadata) {
		delete(b.data, storageKey)
	}
	b.mu.Unlock()
	return nil
}

var (
	_ storage.BlobStore = (*blobInstance)(nil)
	_ xfile.Resolver    = (*blobInstance)(nil)
)

// ---------------------------------------------------------------------------
// KindLock — action.Mutex
// ---------------------------------------------------------------------------

// Lock is the in-memory Mutex backend.
type Lock struct{}

func NewLock() *Lock { return &Lock{} }

func (Lock) Name() string       { return backendName }
func (Lock) Kind() storage.Kind { return storage.KindLock }

func (Lock) Open(_ map[string]string) (storage.Instance, error) {
	return &lockInstance{holders: make(map[string]*lockHolder)}, nil
}

type lockHolder struct {
	owner   string
	expires time.Time
}

type lockInstance struct {
	mu      sync.Mutex
	holders map[string]*lockHolder
}

func (l *lockInstance) Kind() storage.Kind { return storage.KindLock }
func (l *lockInstance) Close() error       { return nil }

func (l *lockInstance) TryLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if ttl <= 0 {
		return false, storage.BadRequest("memory lock: ttl must be positive")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if h, ok := l.holders[key]; ok && now.Before(h.expires) {
		return false, nil
	}
	l.holders[key] = &lockHolder{owner: backendName, expires: now.Add(ttl)}
	return true, nil
}

func (l *lockInstance) Unlock(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.holders, key)
	return nil
}

var _ action.Mutex = (*lockInstance)(nil)

// ---------------------------------------------------------------------------
// KindFencedLock — action.FencedMutex
// ---------------------------------------------------------------------------

// FencedLock is the in-memory FencedMutex backend. The fence counter
// is process-local; a real backend (Redis, etcd) must source it from
// the underlying store's monotonic version.
type FencedLock struct{}

func NewFencedLock() *FencedLock { return &FencedLock{} }

func (FencedLock) Name() string       { return backendName }
func (FencedLock) Kind() storage.Kind { return storage.KindFencedLock }

func (FencedLock) Open(_ map[string]string) (storage.Instance, error) {
	return &fencedInstance{holders: make(map[string]*fencedHolder)}, nil
}

type fencedHolder struct {
	lease   action.LockLease
	expires time.Time
}

type fencedInstance struct {
	mu      sync.Mutex
	holders map[string]*fencedHolder
	fence   int64
}

func (f *fencedInstance) Kind() storage.Kind { return storage.KindFencedLock }
func (f *fencedInstance) Close() error       { return nil }

func (f *fencedInstance) Acquire(ctx context.Context, key string, ttl time.Duration) (action.LockLease, bool, error) {
	if err := ctx.Err(); err != nil {
		return action.LockLease{}, false, err
	}
	if ttl < 300*time.Millisecond {
		return action.LockLease{}, false, storage.BadRequest("memory fenced lock: ttl must be at least 300ms")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	if h, ok := f.holders[key]; ok && now.Before(h.expires) {
		return action.LockLease{}, false, nil
	}
	f.fence++
	lease := action.LockLease{
		Key:   key,
		Owner: backendName,
		Fence: f.fence,
	}
	f.holders[key] = &fencedHolder{lease: lease, expires: now.Add(ttl)}
	return lease, true, nil
}

func (f *fencedInstance) Renew(ctx context.Context, lease action.LockLease, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.holders[lease.Key]
	if !ok || h.lease.Owner != lease.Owner || h.lease.Fence != lease.Fence {
		return false, nil
	}
	h.expires = time.Now().Add(ttl)
	return true, nil
}

func (f *fencedInstance) Release(ctx context.Context, lease action.LockLease) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	h, ok := f.holders[lease.Key]
	if !ok || h.lease.Owner != lease.Owner || h.lease.Fence != lease.Fence {
		return false, nil
	}
	delete(f.holders, lease.Key)
	return true, nil
}

var _ action.FencedMutex = (*fencedInstance)(nil)

// ---------------------------------------------------------------------------
// KindIdem — action.IdempotencyStore (+IdempotencyCoordinator)
// ---------------------------------------------------------------------------

// Idem is the in-memory IdempotencyStore backend. It also implements
// action.IdempotencyCoordinator so callers that probe for the
// coordinator capability get the full state machine.
type Idem struct{}

func NewIdem() *Idem { return &Idem{} }

func (Idem) Name() string       { return backendName }
func (Idem) Kind() storage.Kind { return storage.KindIdem }

func (Idem) Open(opts map[string]string) (storage.Instance, error) {
	ttl, err := storage.ParseDuration(opts["ttl"], 0)
	if err != nil {
		return nil, err
	}
	return &idemInstance{
		entries: make(map[string]idemSlot),
		ttl:     ttl,
	}, nil
}

type idemSlot struct {
	entry   action.IdempotencyEntry
	expires time.Time
	// Coordinator state
	claimToken string
	claimExp   time.Time
	claimHash  string
}

type idemInstance struct {
	mu      sync.Mutex
	entries map[string]idemSlot
	ttl     time.Duration
}

func (i *idemInstance) Kind() storage.Kind { return storage.KindIdem }
func (i *idemInstance) Close() error       { return nil }

func (i *idemInstance) Get(_ context.Context, key string) (action.IdempotencyEntry, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	slot, ok := i.entries[key]
	if !ok {
		return action.IdempotencyEntry{}, false
	}
	now := time.Now()
	if !slot.expires.IsZero() && now.After(slot.expires) {
		delete(i.entries, key)
		return action.IdempotencyEntry{}, false
	}
	return slot.entry, true
}

func (i *idemInstance) Set(_ context.Context, key string, entry action.IdempotencyEntry, ttl time.Duration) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if ttl <= 0 {
		ttl = i.ttl
	}
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	i.entries[key] = idemSlot{entry: entry, expires: exp}
}

// --- Coordinator capability ---

func (i *idemInstance) Claim(_ context.Context, key, requestHash string, leaseTTL time.Duration) (action.IdempotencyClaim, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	now := time.Now()

	// Expire stale claim.
	if slot, ok := i.entries[key]; ok {
		if !slot.claimExp.IsZero() && now.After(slot.claimExp) && slot.entry.Body == nil {
			delete(i.entries, key)
		}
	}

	if slot, ok := i.entries[key]; ok {
		if slot.entry.Body != nil {
			// Already completed.
			if slot.entry.RequestHash != requestHash {
				return action.IdempotencyClaim{State: action.IdempotencyClaimConflict}, nil
			}
			return action.IdempotencyClaim{State: action.IdempotencyClaimCompleted, Entry: slot.entry}, nil
		}
		// In-progress.
		if slot.claimHash != requestHash {
			return action.IdempotencyClaim{State: action.IdempotencyClaimConflict}, nil
		}
		return action.IdempotencyClaim{State: action.IdempotencyClaimInProgress}, nil
	}

	token := key + "-" + now.Format(time.RFC3339Nano)
	i.entries[key] = idemSlot{
		claimToken: token,
		claimExp:   now.Add(leaseTTL),
		claimHash:  requestHash,
	}
	return action.IdempotencyClaim{
		State: action.IdempotencyClaimAcquired,
		Token: token,
	}, nil
}

func (i *idemInstance) Complete(_ context.Context, key, token string, entry action.IdempotencyEntry, ttl time.Duration) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	slot, ok := i.entries[key]
	if !ok || slot.claimToken != token {
		return storage.Conflictf("idempotency: claim lost for key %q", key)
	}
	if ttl <= 0 {
		ttl = i.ttl
	}
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	i.entries[key] = idemSlot{entry: entry, expires: exp}
	return nil
}

func (i *idemInstance) Release(_ context.Context, key, token string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	slot, ok := i.entries[key]
	if !ok || slot.claimToken != token {
		return nil
	}
	if slot.entry.Body == nil {
		delete(i.entries, key)
	}
	return nil
}

var (
	_ action.IdempotencyStore       = (*idemInstance)(nil)
	_ action.IdempotencyCoordinator = (*idemInstance)(nil)
)

// ---------------------------------------------------------------------------
// KindJournal — action.Journal
// ---------------------------------------------------------------------------

// Journal is the in-memory Journal backend. Used by action.Durable
// for step-level memoization.
type Journal struct{}

func NewJournal() *Journal { return &Journal{} }

func (Journal) Name() string       { return backendName }
func (Journal) Kind() storage.Kind { return storage.KindJournal }

func (Journal) Open(_ map[string]string) (storage.Instance, error) {
	return &journalInstance{steps: make(map[string]map[string][]byte)}, nil
}

type journalInstance struct {
	mu    sync.RWMutex
	steps map[string]map[string][]byte // executionID -> stepName -> payload
}

func (j *journalInstance) Kind() storage.Kind { return storage.KindJournal }
func (j *journalInstance) Close() error       { return nil }

func (j *journalInstance) GetStep(_ context.Context, executionID, stepName string) (data []byte, hit bool, err error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	steps, ok := j.steps[executionID]
	if !ok {
		return nil, false, nil
	}
	payload, ok := steps[stepName]
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, len(payload))
	copy(out, payload)
	return out, true, nil
}

func (j *journalInstance) RecordStep(_ context.Context, executionID, stepName string, payload []byte) error {
	stored := make([]byte, len(payload))
	copy(stored, payload)
	j.mu.Lock()
	defer j.mu.Unlock()
	steps, ok := j.steps[executionID]
	if !ok {
		steps = make(map[string][]byte)
		j.steps[executionID] = steps
	}
	steps[stepName] = stored
	return nil
}

var _ action.Journal = (*journalInstance)(nil)

// ---------------------------------------------------------------------------
// KindTx — action.TxRunner
// ---------------------------------------------------------------------------

// Tx is the in-memory TxRunner backend. It does not provide real
// transactional semantics; it simply invokes fn. Useful only for
// tests where transactional behavior is mocked.
type Tx struct{}

func NewTx() *Tx { return &Tx{} }

func (Tx) Name() string       { return backendName }
func (Tx) Kind() storage.Kind { return storage.KindTx }

func (Tx) Open(_ map[string]string) (storage.Instance, error) {
	return &txInstance{}, nil
}

type txInstance struct{}

func (txInstance) Kind() storage.Kind { return storage.KindTx }
func (txInstance) Close() error       { return nil }
func (txInstance) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

var _ action.TxRunner = (*txInstance)(nil)

// ---------------------------------------------------------------------------
// KindRateLimit — action.RateLimiter
// ---------------------------------------------------------------------------

// RateLimit is the in-memory RateLimiter backend.
type RateLimit struct{}

func NewRateLimit() *RateLimit { return &RateLimit{} }

func (RateLimit) Name() string       { return backendName }
func (RateLimit) Kind() storage.Kind { return storage.KindRateLimit }

func (RateLimit) Open(opts map[string]string) (storage.Instance, error) {
	rps, err := parseRPS(opts["rps"])
	if err != nil {
		return nil, err
	}
	burst := 1
	if opts["burst"] != "" {
		burst, err = parseInt(opts["burst"])
		if err != nil {
			return nil, err
		}
	}
	if burst < 1 {
		burst = 1
	}
	return &rateLimitInstance{rps: rps, burst: burst}, nil
}

type rateLimitInstance struct {
	mu     sync.Mutex
	bucket *tokenBucket
	rps    float64
	burst  int
}

func (r *rateLimitInstance) Kind() storage.Kind { return storage.KindRateLimit }
func (r *rateLimitInstance) Close() error       { return nil }

func (r *rateLimitInstance) Allow(ctx context.Context, _ string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bucket == nil {
		r.bucket = newTokenBucket(r.rps, r.burst)
	}
	return r.bucket.allow(time.Now()), nil
}

var _ action.RateLimiter = (*rateLimitInstance)(nil)
