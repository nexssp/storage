package nexssflow

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/storage"
)

func storageModifiers() []compiler.Modifier {
	noopStr := func(b *action.Builder[any, any], _ string) *action.Builder[any, any] { return b }
	return []compiler.Modifier{
		compiler.WithInheritable(compiler.String("cache_store", noopStr)),
		compiler.WithInheritable(compiler.String("cache_ttl", noopStr)),
		compiler.WithInheritable(compiler.String("idempotent_store", noopStr)),
		compiler.WithInheritable(compiler.String("dedup_store", noopStr)),
	}
}

func storageAdvisor(s *store) compiler.AtomAdviseFunc {
	return func(atom *compiler.Atom, builder *action.Builder[any, any]) error {
		if atom == nil || builder == nil {
			return nil
		}

		var cacheStore, cacheTTL, idemStore, dedupStore string
		var hasCacheStore, hasCacheTTL, hasIdemStore, hasDedupStore bool

		remaining := atom.Modifiers[:0]
		for _, raw := range atom.Modifiers {
			name, value := splitModifier(raw)
			switch name {
			case "cache_store":
				cacheStore = value
				hasCacheStore = true
			case "cache_ttl":
				cacheTTL = value
				hasCacheTTL = true
			case "idempotent_store":
				idemStore = value
				hasIdemStore = true
			case "dedup_store":
				dedupStore = value
				hasDedupStore = true
			default:
				remaining = append(remaining, raw)
			}
		}
		atom.Modifiers = remaining

		if hasCacheStore {
			if err := applyCacheStore(s, builder, cacheStore, cacheTTL); err != nil {
				return err
			}
		} else if hasCacheTTL {
			return storage.BadRequest("cache_ttl modifier requires cache_store")
		}

		if hasIdemStore {
			if err := applyIdempotentStore(s, builder, idemStore); err != nil {
				return err
			}
		}

		if hasDedupStore {
			if err := applyDedupStore(s, builder, dedupStore); err != nil {
				return err
			}
		}

		return nil
	}
}

func splitModifier(raw string) (name, value string) {
	if before, after, ok := strings.Cut(raw, "="); ok {
		return before, after
	}
	return raw, ""
}

func applyCacheStore(s *store, b *action.Builder[any, any], storeName, ttlStr string) error {
	if storeName == "" {
		return storage.BadRequest("cache_store: store name is required (use :cache_store=NAME)")
	}
	decl, err := s.declaration(storeName)
	if err != nil {
		return err
	}
	inst, err := s.instance(storeName)
	if err != nil {
		return err
	}
	layer, err := storage.AssertKV(inst)
	if err != nil {
		return err
	}

	ttlRaw := decl.Options["ttl"]
	if ttlStr != "" {
		ttlRaw = ttlStr
	}

	ttl, err := storage.ParseDuration(ttlRaw, 5*time.Minute)
	if err != nil {
		return err
	}
	b.Cache(ttl, defaultCacheKey, layer)
	return nil
}

func applyIdempotentStore(s *store, b *action.Builder[any, any], storeName string) error {
	if storeName == "" {
		return storage.BadRequest("idempotent_store: store name is required")
	}
	inst, err := s.instance(storeName)
	if err != nil {
		return err
	}
	st, err := storage.AssertIdempotencyStore(inst)
	if err != nil {
		return err
	}
	b.IdempotentWithConfig(action.IdempotencyConfig{Enabled: true, Store: st})
	return nil
}

func applyDedupStore(s *store, b *action.Builder[any, any], storeName string) error {
	if storeName == "" {
		return storage.BadRequest("dedup_store: store name is required")
	}
	inst, err := s.instance(storeName)
	if err != nil {
		return err
	}
	if _, err := storage.AssertMutex(inst); err != nil {
		return err
	}
	b.Dedup(defaultCacheKey)
	return nil
}

func defaultCacheKey(request any) string {
	if request == nil {
		return ""
	}
	data, err := json.Marshal(request)
	if err != nil {
		return ""
	}
	return string(data)
}
