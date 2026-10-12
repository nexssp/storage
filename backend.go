// Package storage defines the universal adapter contract for pluggable
// storage backends used by Nexss Flow and any other consumer of
// nexssp/kernel.
//
// The contract is intentionally tiny: a Backend advertises a Name and a
// Kind, and produces an Instance from an options map. Every Instance
// satisfies one or more concrete Kernel interfaces (CacheLayer[V],
// Mutex, FencedMutex, IdempotencyStore, Journal, TxRunner, RateLimiter,
// or the proposed BlobStore defined in blob.go).
//
// Typical use:
//
//	reg := storage.NewRegistry()
//	reg.MustRegister(memory.NewKV())
//	reg.MustRegister(memory.NewBlob())
//
//	decl := storage.Declaration{
//	    Name:    "kv:memory",
//	    Backend: "memory",
//	    Kind:    storage.KindKV,
//	    Options: map[string]string{"ttl": "5m"},
//	}
//	inst, err := decl.Open(reg)
//	// inst implements action.CacheLayer[any]
//
// A Flow extension wires this Registry into a Bundle; see
// ../flow-storage-extension/library.go.
package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"
)

// Kind enumerates the storage contracts an adapter can implement. Each
// Kind maps 1:1 to a Kernel interface, so the type system guarantees
// the right method set is available after Instance assertion.
//
// Add a new Kind only when a new Kernel storage interface appears. Do
// not add Kinds for non-storage contracts (logging, metrics, etc.).
type Kind string

const (
	// KindKV maps to action.CacheLayer[V]. A KV store serves typed
	// values keyed by string with per-entry TTL. Use this for caches,
	// session stores, and small structured records.
	KindKV Kind = "kv"

	// KindBlob maps to BlobStore (defined in blob.go, proposed for
	// promotion to package action). A Blob store serves opaque byte
	// slices without TTL semantics. Use this for files, documents,
	// snapshots, and large payloads.
	KindBlob Kind = "blob"

	// KindLock maps to action.Mutex. A basic distributed lock with
	// TryLock/Unlock semantics. Use this for short critical sections
	// where stale writers are tolerable.
	KindLock Kind = "lock"

	// KindFencedLock maps to action.FencedMutex. A fenced lock returns
	// a monotonic LockLease that downstream systems can use to reject
	// stale writers. Use this for any storage backend that supports
	// fencing tokens (Redis ACL, etcd revision, Postgres advisory
	// locks with version columns).
	KindFencedLock Kind = "fenced_lock"

	// KindIdem maps to action.IdempotencyStore. The advanced
	// IdempotencyCoordinator and IdempotencyLeaseRenewer contracts are
	// optional capabilities an Instance may additionally satisfy; the
	// Flow extension probes for them at use time.
	KindIdem Kind = "idem"

	// KindJournal maps to action.Journal. Used by action.Durable for
	// step-level memoization so resumed executions skip already-run
	// steps. Use this for workflow checkpoint storage.
	KindJournal Kind = "journal"

	// KindTx maps to action.TxRunner. A TxRunner wraps a function in a
	// transaction boundary. Use this for relational databases and
	// document stores that expose atomic multi-op transactions.
	KindTx Kind = "tx"

	// KindRateLimit maps to action.RateLimiter. A distributed
	// token-bucket admission control. Use this when rate-limit state
	// must be shared across processes (Redis, etcd).
	KindRateLimit Kind = "rate_limit"
)

// AllKinds returns every defined Kind, sorted. Useful for diagnostics
// and CLI help output.
func AllKinds() []Kind {
	return []Kind{
		KindKV,
		KindBlob,
		KindLock,
		KindFencedLock,
		KindIdem,
		KindJournal,
		KindTx,
		KindRateLimit,
	}
}

// KernelInterface returns the Kernel interface name an Instance of
// this Kind is expected to satisfy. Used in error messages and
// diagnostics; not used for runtime assertion.
func (k Kind) KernelInterface() string {
	switch k {
	case KindKV:
		return "action.CacheLayer[any]"
	case KindBlob:
		return "storage.BlobStore"
	case KindLock:
		return "action.Mutex"
	case KindFencedLock:
		return "action.FencedMutex"
	case KindIdem:
		return "action.IdempotencyStore"
	case KindJournal:
		return "action.Journal"
	case KindTx:
		return "action.TxRunner"
	case KindRateLimit:
		return "action.RateLimiter"
	default:
		return "<unknown>"
	}
}

// Instance is a type-erased handle to an opened store. Callers assert
// to the specific Kernel interface matching the declared Kind at use
// time. This is the only reflection-like step in the system, and it
// happens once per declaration, never per call.
//
// An Instance MAY satisfy multiple Kernel interfaces (e.g., a
// relational database backend might implement both TxRunner and
// Journal). The Kind only declares the primary contract; consumers may
// probe for additional capabilities with the standard Go type-assertion
// idiom:
//
//	if rl, ok := inst.(action.RateLimiter); ok { ... }
type Instance interface {
	// Kind echoes the Kind the Instance was opened under. Must match
	// the Backend that produced it.
	Kind() Kind

	// Close releases the Instance's resources. It is called exactly
	// once by the Registry when the host is shutting down. After
	// Close returns, the Instance must not be used.
	Close() error
}

// Backend is the contract every adapter implements. Open returns an
// Instance whose concrete type satisfies the Kernel interface for the
// declared Kind.
//
// Open MUST be safe to call more than once with the same opts map.
// Callers may open fresh Instances per declaration, or share one
// Instance across declarations; the Registry caches by Declaration
// name regardless.
//
// Open MUST validate opts and return a classified error
// (xerr.BadRequest for invalid user input, xerr.Unavailable for
// infrastructure failures) rather than panicking.
type Backend interface {
	// Name is the adapter identifier, e.g. "memory", "redis", "s3".
	// Two backends with the same Name cannot coexist in one Registry.
	Name() string

	// Kind is the primary storage contract this Backend provides.
	// A Backend declares exactly one Kind; adapters that can serve
	// multiple Kinds (e.g., Redis can do both KV and Lock) register
	// separate Backend instances for each Kind.
	Kind() Kind

	// Open returns a fresh Instance bound to opts. opts keys are
	// backend-specific; common keys include "addr", "ttl",
	// "namespace", "bucket", "region". Unrecognized keys MUST be
	// rejected with xerr.BadRequest.
	Open(opts map[string]string) (Instance, error)
}

// Declaration is the parsed form of an @storage block. It binds a
// human-readable Name to a Backend and its connection options.
//
// Flow parses declarations from source; pure-Go consumers construct
// them directly. Either way, Declaration.Open is the only way to
// obtain a live Instance from a Registry.
type Declaration struct {
	// Name is the unique identifier callers use to reference this
	// declaration. Convention: "<kind>:<backend>" (e.g. "kv:redis"),
	// but any unique non-empty string is allowed. The Flow @storage
	// directive uses "as <alias>" to override.
	Name string

	// Backend names the adapter to open. Must match a Backend in the
	// target Registry. Example: "memory", "redis", "s3".
	Backend string

	// Kind is the storage contract. Must match Backend.Kind() or
	// Declaration.Open returns a classified mismatch error.
	Kind Kind

	// Options are backend-specific connection details. Validated by
	// the Backend at Open time.
	Options map[string]string

	// Position carries source location for error attribution. Pure-Go
	// consumers leave it zero. Flow fills it from the directive.
	Position Position
}

// Position is a minimal source-location record. It mirrors
// core.Position from flow/compiler but lives here so the storage
// package does not depend on flow.
type Position struct {
	File string
	Line int
}

func (p Position) String() string {
	if p.File == "" {
		return ""
	}
	if p.Line > 0 {
		return fmt.Sprintf("%s:%d", p.File, p.Line)
	}
	return p.File
}

// Open looks up the Backend in reg, validates the Kind match, and
// returns a fresh Instance.
func (d Declaration) Open(reg *Registry) (Instance, error) {
	if d.Name == "" {
		return nil, BadRequestf("storage: declaration name is required")
	}
	if d.Backend == "" {
		return nil, BadRequestf("storage: declaration %q: backend is required", d.Name)
	}
	if d.Kind == "" {
		return nil, BadRequestf("storage: declaration %q: kind is required", d.Name)
	}
	b, ok := reg.Lookup(d.Backend, d.Kind)
	if !ok {
		return nil, NotFoundf("storage: declaration %q: backend %q (kind %q) not registered",
			d.Name, d.Backend, d.Kind)
	}
	if b.Kind() != d.Kind {
		return nil, BadRequestf(
			"storage: declaration %q: backend %q is kind %q, expected %q",
			d.Name, d.Backend, b.Kind(), d.Kind)
	}
	return reg.openLocked(d.Backend, d.Kind, d.Options)
}

// Registry holds the available backends and tracks opened Instances
// for lifecycle management. It is a struct, not a package global, so
// two flows in the same process can have independent storage
// configurations. This addresses the anti-pattern flagged in
// flow/AGENTS.md §7 (currently exhibited by extensions/decide).
//
// Backends are keyed by (Name, Kind) — a single adapter like Redis
// can serve both KV and Lock contracts by registering two Backend
// instances with Name="redis" but different Kind values. This keeps
// the user's mental model simple ("the redis backend" is one thing)
// while letting the type system distinguish which contract is in use.
type Registry struct {
	mu       sync.RWMutex
	backends map[backendKey]Backend
	opened   []Instance
}

// backendKey is the composite map key.
type backendKey struct {
	name string
	kind Kind
}

// NewRegistry returns an empty Registry. Callers Register backends
// before any Declaration.Open call.
func NewRegistry() *Registry {
	return &Registry{backends: make(map[backendKey]Backend)}
}

func (r *Registry) Register(b Backend) error {
	if b == nil {
		return BadRequest("storage: backend cannot be nil")
	}
	name := b.Name()
	if name == "" {
		return BadRequest("storage: backend name cannot be empty")
	}
	if b.Kind() == "" {
		return BadRequestf("storage: backend %q: kind cannot be empty", name)
	}
	key := backendKey{name: name, kind: b.Kind()}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.backends[key]; exists {
		return Conflict("storage: duplicate backend " + name + " (kind " + string(b.Kind()) + ")")
	}
	r.backends[key] = b
	return nil
}

// MustRegister panics on registration error. Intended for built-in
// backends whose absence is a programmer bug, not a user error.
func (r *Registry) MustRegister(b Backend) {
	if err := r.Register(b); err != nil {
		panic(err)
	}
}

// Lookup returns the Backend with the given (name, kind) and a found
// flag. Use LookupKind to enumerate Backends regardless of Kind.
func (r *Registry) Lookup(name string, kind Kind) (Backend, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.backends[backendKey{name: name, kind: kind}]
	return b, ok
}

// LookupKind returns every Backend registered under name, regardless
// of Kind. Useful for diagnostics: "what kinds does 'redis' serve?".
func (r *Registry) LookupKind(name string) []Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Backend
	for k, b := range r.backends {
		if k.name == name {
			out = append(out, b)
		}
	}
	return out
}

// Names returns every registered Backend name, de-duplicated and
// sorted. A name appears once even if the same adapter serves
// multiple Kinds.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]struct{}, len(r.backends))
	for k := range r.backends {
		seen[k.name] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// openLocked is the internal Open path. It calls Backend.Open and
// tracks the resulting Instance for Close.
func (r *Registry) openLocked(backendName string, kind Kind, opts map[string]string) (Instance, error) {
	b, ok := r.Lookup(backendName, kind)
	if !ok {
		return nil, NotFoundf("storage: backend %q (kind %q) not registered", backendName, kind)
	}
	inst, err := b.Open(opts)
	if err != nil {
		// Already-classified errors pass through; wrap unknown ones as
		// Unavailable so the kind travels with the cause.
		//
		//nolint:errcheck // errors.AsType returns (target, bool); the
		// target is intentionally discarded because the original wrapped
		// err is returned to preserve the full unwrap chain for callers.
		if _, isAppErr := errors.AsType[*xerr.AppError](err); isAppErr {
			return nil, err
		}
		return nil, Unavailablef(err, "storage: open %q failed", backendName)
	}
	r.mu.Lock()
	r.opened = append(r.opened, inst)
	r.mu.Unlock()
	return inst, nil
}

// Close releases every opened Instance. Errors are joined but never
// abort the loop; one failing Close does not leak the rest.
//
// Close honors ctx for backends whose Close blocks on network I/O.
// Callers should pass a context with a deadline so a hung backend
// cannot stall host shutdown indefinitely.
func (r *Registry) Close(ctx context.Context) error {
	r.mu.Lock()
	opened := r.opened
	r.opened = nil
	r.mu.Unlock()

	var errs []error
	for _, inst := range opened {
		if ctx.Err() != nil {
			errs = append(errs, ctx.Err())
			break
		}
		if err := inst.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// AssertKV asserts that inst satisfies action.CacheLayer[any] and
// returns the typed handle. The Kind check is defensive: a well-formed
// Registry never hands back an Instance of the wrong Kind, but a
// consumer may have an Instance from another source.
func AssertKV(inst Instance) (action.CacheLayer[any], error) {
	if inst.Kind() != KindKV {
		return nil, BadRequestf("storage: instance is kind %q, expected %q", inst.Kind(), KindKV)
	}
	layer, ok := inst.(action.CacheLayer[any])
	if !ok {
		return nil, Internalf("storage: backend %q did not implement action.CacheLayer[any]", inst.Kind())
	}
	return layer, nil
}

// AssertBlob asserts that inst satisfies BlobStore.
func AssertBlob(inst Instance) (BlobStore, error) {
	if inst.Kind() != KindBlob {
		return nil, BadRequestf("storage: instance is kind %q, expected %q", inst.Kind(), KindBlob)
	}
	store, ok := inst.(BlobStore)
	if !ok {
		return nil, Internalf("storage: backend %q did not implement BlobStore", inst.Kind())
	}
	return store, nil
}

// AssertMutex asserts that inst satisfies action.Mutex.
func AssertMutex(inst Instance) (action.Mutex, error) {
	if inst.Kind() != KindLock {
		return nil, BadRequestf("storage: instance is kind %q, expected %q", inst.Kind(), KindLock)
	}
	m, ok := inst.(action.Mutex)
	if !ok {
		return nil, Internalf("storage: backend %q did not implement action.Mutex", inst.Kind())
	}
	return m, nil
}

// AssertFencedMutex asserts that inst satisfies action.FencedMutex.
func AssertFencedMutex(inst Instance) (action.FencedMutex, error) {
	if inst.Kind() != KindFencedLock {
		return nil, BadRequestf("storage: instance is kind %q, expected %q", inst.Kind(), KindFencedLock)
	}
	m, ok := inst.(action.FencedMutex)
	if !ok {
		return nil, Internalf("storage: backend %q did not implement action.FencedMutex", inst.Kind())
	}
	return m, nil
}

// AssertIdempotencyStore asserts that inst satisfies
// action.IdempotencyStore. Callers may additionally probe for
// IdempotencyCoordinator and IdempotencyLeaseRenewer via type
// assertion; see action.IdempotencyCoordinator.
func AssertIdempotencyStore(inst Instance) (action.IdempotencyStore, error) {
	if inst.Kind() != KindIdem {
		return nil, BadRequestf("storage: instance is kind %q, expected %q", inst.Kind(), KindIdem)
	}
	s, ok := inst.(action.IdempotencyStore)
	if !ok {
		return nil, Internalf("storage: backend %q did not implement action.IdempotencyStore", inst.Kind())
	}
	return s, nil
}

// AssertJournal asserts that inst satisfies action.Journal.
func AssertJournal(inst Instance) (action.Journal, error) {
	if inst.Kind() != KindJournal {
		return nil, BadRequestf("storage: instance is kind %q, expected %q", inst.Kind(), KindJournal)
	}
	j, ok := inst.(action.Journal)
	if !ok {
		return nil, Internalf("storage: backend %q did not implement action.Journal", inst.Kind())
	}
	return j, nil
}

// AssertTxRunner asserts that inst satisfies action.TxRunner.
func AssertTxRunner(inst Instance) (action.TxRunner, error) {
	if inst.Kind() != KindTx {
		return nil, BadRequestf("storage: instance is kind %q, expected %q", inst.Kind(), KindTx)
	}
	t, ok := inst.(action.TxRunner)
	if !ok {
		return nil, Internalf("storage: backend %q did not implement action.TxRunner", inst.Kind())
	}
	return t, nil
}

// AssertRateLimiter asserts that inst satisfies action.RateLimiter.
func AssertRateLimiter(inst Instance) (action.RateLimiter, error) {
	if inst.Kind() != KindRateLimit {
		return nil, BadRequestf("storage: instance is kind %q, expected %q", inst.Kind(), KindRateLimit)
	}
	rl, ok := inst.(action.RateLimiter)
	if !ok {
		return nil, Internalf("storage: backend %q did not implement action.RateLimiter", inst.Kind())
	}
	return rl, nil
}

// ParseDuration is a tiny helper that mirrors time.ParseDuration but
// returns a classified BadRequest on parse failure. Backends may use
// it to validate duration options consistently.
func ParseDuration(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, BadRequestf("storage: invalid duration %q: %v", s, err)
	}
	return d, nil
}
