package nexssflow

import (
	"context"
	"sync"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xctx"
)

// HookProbeKey is the context key set by the storage.events hook when
// any storage event fires. Test fixtures and runtime probes read it
// to assert that hooks fired.
//
// The value is a *StorageEvents pointer (never nil when set) so
// consumers can read the accumulated event log without copying.
var HookProbeKey = xctx.NewKey[*StorageEvents]("flow.storage.events")

// StorageEvents is the per-request accumulator that the
// storage.events hook stamps into the context on first miss/hit.
// Concurrent-safe; intended for observability and test assertion.
type StorageEvents struct {
	mu     sync.Mutex
	events []StorageEvent
}

// StorageEvent records one storage operation outcome.
type StorageEvent struct {
	Kind   string // "cache_hit", "cache_miss", "error"
	Store  string // the @storage declaration name
	Action string // the action name, e.g. "storage.kv.get"
	Key    string // the key passed to the action, if any
}

// Record appends an event. Safe for concurrent use.
func (s *StorageEvents) Record(e StorageEvent) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

// Events returns a snapshot copy of the accumulated events, in
// chronological order.
func (s *StorageEvents) Events() []StorageEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]StorageEvent, len(s.events))
	copy(out, s.events)
	return out
}

// eventsFromContext returns the accumulator for this request, or nil
// if the storage.events hook is not attached.
func eventsFromContext(ctx context.Context) *StorageEvents {
	if e, ok := HookProbeKey.From(ctx); ok && e != nil {
		return e
	}
	return nil
}

// recordStorageEvent stamps a StorageEvent into the context's
// accumulator, if one exists. No-op when the hook is not attached —
// this keeps the action hot path zero-allocation when nobody is
// observing.
func recordStorageEvent(ctx context.Context, kind, store, actionName, key string) {
	if e := eventsFromContext(ctx); e != nil {
		e.Record(StorageEvent{
			Kind: kind, Store: store, Action: actionName, Key: key,
		})
	}
}

// registerStorageHooks installs the named "storage.events" hook into
// Kernel's process-wide hook registry. The hook's Before callback
// stamps a fresh StorageEvents accumulator into the request context
// so storage.* actions can record events into it.
//
// Users attach it with:
//
//	@hook:storage.events
//
// The hook fires OnError as well, recording any error from a storage
// action so a fallback pipeline can inspect what went wrong.
func registerStorageHooks() {
	action.MustRegisterHook("storage.events", func() action.AnyHook {
		return action.AnyHook{
			Before: func(ctx context.Context, _ any, _ *action.Meta) (context.Context, error) {
				// Only stamp if not already present — a nested
				// action inherits the outer accumulator.
				if _, ok := HookProbeKey.From(ctx); !ok {
					ctx = HookProbeKey.With(ctx, &StorageEvents{})
				}
				return ctx, nil
			},
		}
	})
}
