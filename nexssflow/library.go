package nexssflow

import (
	"context"
	"embed"

	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/memory"
	"github.com/nexssp/storage/postgres"
	"github.com/nexssp/storage/redis"
	"github.com/nexssp/storage/sqlite"
)

const ID = "storage"

var fixturesFS embed.FS

func init() {
	compiler.Register(ID, Bundle)
	registerStorageHooks()
}

// Bundle returns a fresh Bundle for a single @require declaration.
//
// A single bundle instance owns:
//
//   - one storage.Registry (the backends are shared, read-only, for
//     every compilation this bundle serves), and
//   - a list of per-compilation stores it created, so Shutdown can
//     close every opened Instance on teardown.
//
// The three callbacks below cooperate on a single per-compilation
// store. The order is:
//
//  1. OnPreprocess  creates the store from meta["storage"] once,
//     installs the :cache_store= / :idempotent_store= /
//     :dedup_store= advisor against it, and records it
//     in meta[storeMetaKey].
//  2. Materialize   reuses the store from meta (does NOT create a
//     second one) and mounts <alias>.<kind>.<op>
//     actions that close over it.
//  3. WrapPipeline  injects the same store into the runtime context
//     so the generic storage.* actions can resolve it
//     via currentStore(ctx).
//
// If a flow declares no @storage directives, all three callbacks are
// no-ops and the compiled program is not wrapped at all.
func Bundle(_ map[string]string) compiler.Bundle {
	b := newBundle()
	return compiler.Bundle{
		ID:           ID,
		Libraries:    []action.Library{buildLibrary()},
		Directives:   []compiler.Directive{storageDirective()},
		Modifiers:    storageModifiers(),
		OnPreprocess: onPreprocess(b),
		Materialize:  materialize(b),
		WrapPipeline: wrapPipeline(b),
		Shutdowns:    []compiler.ShutdownFunc{b.Close},
		Fixtures:     fixturesFS,
	}
}

func registerMemoryBackends(reg *storage.Registry) {
	for _, bk := range []storage.Backend{
		memory.NewKV(),
		memory.NewBlob(),
		memory.NewLock(),
		memory.NewFencedLock(),
		memory.NewIdem(),
		memory.NewJournal(),
		memory.NewTx(),
		memory.NewRateLimit(),
	} {
		reg.MustRegister(bk)
	}
}

func registerPersistentBackends(reg *storage.Registry) {
	for _, register := range []func(*storage.Registry) error{
		sqlite.Register,
		postgres.Register,
		redis.Register,
	} {
		if err := register(reg); err != nil {
			panic(err)
		}
	}
}

// onPreprocess runs after Preprocess, once per compilation. It creates
// the per-compilation store (idempotently — the same meta map is
// shared with Materialize and WrapPipeline) and installs the storage
// advisor that handles the three :*_store= modifiers.
//
// When the source declares no @storage directives, this returns an
// empty contribution: no store, no advisor, no runtime wrapper.
func onPreprocess(b *bundle) compiler.OnPreprocessFunc {
	return func(meta map[string]any) compiler.PreprocessContributions {
		decls := declarationsFromMeta(meta)
		if len(decls) == 0 {
			return compiler.PreprocessContributions{}
		}
		s := storeFromMeta(meta)
		if s == nil {
			s = b.newStore(decls)
			meta[storeMetaKey] = s
		}
		return compiler.PreprocessContributions{
			CompileOpts: []compiler.CompileOption{
				compiler.WithAtomAdvisors(storageAdvisor(s)),
			},
		}
	}
}

// wrapPipeline injects the per-compilation store into the execution
// context so generic storage.* actions can resolve `store: "cache"`
// without a static closure. When there is no store (no @storage in the
// source), the inner program is returned unchanged.
func wrapPipeline(_ *bundle) compiler.PipelineWrapFunc {
	return func(meta map[string]any, inner action.AnyAction) (action.AnyAction, error) {
		s := storeFromMeta(meta)
		if s == nil {
			return inner, nil
		}
		return action.New("storage.wrap",
			func(ctx context.Context, req any) (any, error) {
				ctx = storeKey.With(ctx, s)
				return action.InvokeAny(ctx, inner, req)
			}).Build(), nil
	}
}
