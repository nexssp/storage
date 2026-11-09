package nexssflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/storage"
)

func storageModifiers() []compiler.Modifier {
	storageOnly := func(name string) compiler.Modifier {
		return compiler.WithInheritable(compiler.Modifier{
			Name:      name,
			ValueKind: compiler.ModifierKindString,
			Apply: func(_ *action.Builder[any, any], _ string) error {
				return storage.BadRequestf(":%s requires an @storage declaration", name)
			},
		})
	}

	return []compiler.Modifier{
		storageOnly("cache_store"),
		storageOnly("cache_ttl"),
		storageOnly("idempotent_store"),
		compiler.WithInheritable(compiler.Modifier{
			Name:      "local_dedup",
			ValueKind: compiler.ModifierKindString,
			Apply: func(b *action.Builder[any, any], _ string) error {
				b.Dedup(func(request any) string { return requestKey("dedup", request) })
				return nil
			},
		}),
	}
}

func storageAdvisor(s *store) compiler.AtomAdviseFunc {
	return func(atom *compiler.Atom, builder *action.Builder[any, any]) error {
		if atom == nil || builder == nil {
			return nil
		}

		var cacheStore, cacheTTL, idemStore string
		var hasCacheStore, hasCacheTTL, hasIdemStore bool

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
			default:
				remaining = append(remaining, raw)
			}
		}
		atom.Modifiers = remaining

		if hasCacheStore {
			if atom.Name == "" {
				return storage.BadRequest("cache_store requires a named action")
			}
			if err := applyCacheStore(s, builder, atom.Name, cacheStore, cacheTTL); err != nil {
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

		return nil
	}
}

func splitModifier(raw string) (name, value string) {
	if before, after, ok := strings.Cut(raw, "="); ok {
		return before, after
	}
	return raw, ""
}

func applyCacheStore(s *store, b *action.Builder[any, any], actionName, storeName, ttlStr string) error {
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
	b.Cache(ttl, func(request any) string { return requestKey(actionName, request) }, layer)
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

func requestKey(namespace string, request any) string {
	data, err := json.Marshal(request)
	if err != nil {
		slog.Warn("storage middleware bypassed for request that cannot be encoded", "namespace", namespace, "error", err)
		return ""
	}
	digest := sha256.Sum256(data)
	return namespace + ":" + hex.EncodeToString(digest[:])
}
