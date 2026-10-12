package nexssflow

import (
	"context"

	"github.com/nexssp/kernel/action"
)

// probeAction returns the accumulated storage events from the request
// context. Use it after @hook:storage.events to inspect what happened:
//
//	@hook:storage.events
//	cache.kv.get @{ key: "missing-key" }
//	-> storage.probe
//
// The result is a summary: { hits: N, misses: N, errors: N, events: [...] }
// This is how you assert on cache behavior in .nflow fixtures:
//
//	@assert: result.misses == 1
//	@assert: result.hits == 0
func probeAction() action.AnyAction {
	return action.New("storage.probe",
		func(ctx context.Context, _ any) (probeRes, error) {
			events := eventsFromContext(ctx)
			if events == nil {
				return probeRes{
					Attached: false,
				}, nil
			}
			evs := events.Events()
			var hits, misses, errors int
			for _, e := range evs {
				switch e.Kind {
				case "cache_hit":
					hits++
				case "cache_miss":
					misses++
				case "error":
					errors++
				}
			}
			return probeRes{
				Attached: true,
				Hits:     hits,
				Misses:   misses,
				Errors:   errors,
				Events:   evs,
			}, nil
		}).
		Description("Report accumulated storage events from the request context").
		Tag("storage", "probe").
		Build()
}

type probeRes struct {
	Attached bool           `json:"attached"`
	Hits     int            `json:"hits"`
	Misses   int            `json:"misses"`
	Errors   int            `json:"errors"`
	Events   []StorageEvent `json:"events,omitempty"`
}
