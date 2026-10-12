package nexssflow

import (
	"github.com/nexssp/flow/compiler"
	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xerr"

	"github.com/nexssp/storage"
)

func materialize(b *bundle) compiler.Materializer {
	return func(req compiler.MaterializeReq) error {
		decls := declarationsFromMeta(req.Meta)
		if len(decls) == 0 {
			return nil
		}

		// CRITICAL: reuse the store that OnPreprocess created.
		// OnPreprocess installed a storageAdvisor that closes over
		// that store; the aliased actions below must close over the
		// SAME store, otherwise :cache_store= and cache.kv.get talk
		// to different backend instances.
		s := storeFromMeta(req.Meta)
		if s == nil {
			s = b.newStore(decls)
			req.Meta[storeMetaKey] = s
		}

		for _, decl := range decls {
			acts, err := buildAliasedActions(s, decl)
			if err != nil {
				return xerr.BadRequest("@storage "+decl.Name+": "+err.Error(), err)
			}
			if err := req.Resolver.Mount(action.Library{
				Name:    decl.Name,
				Actions: acts,
			}); err != nil {
				return xerr.Internal("@storage " + decl.Name + ": mount: " + err.Error())
			}
		}
		return nil
	}
}

const storeMetaKey = "storage.store"

func declarationsFromMeta(meta map[string]any) []storage.Declaration {
	decls, _ := meta["storage"].([]storage.Declaration)
	return decls
}

func storeFromMeta(meta map[string]any) *store {
	s, _ := meta[storeMetaKey].(*store)
	return s
}

func buildAliasedActions(s *store, decl storage.Declaration) ([]action.AnyAction, error) {
	switch decl.Kind {
	case storage.KindKV:
		return []action.AnyAction{
			aliasedKVSet(s, decl.Name),
			aliasedKVGet(s, decl.Name),
			aliasedKVDel(s, decl.Name),
		}, nil
	case storage.KindBlob:
		return []action.AnyAction{
			aliasedBlobPut(s, decl.Name),
			aliasedBlobGet(s, decl.Name),
			aliasedBlobDel(s, decl.Name),
		}, nil
	case storage.KindLock:
		return []action.AnyAction{
			aliasedLockAcquire(s, decl.Name),
			aliasedLockRelease(s, decl.Name),
		}, nil
	case storage.KindFencedLock:
		return []action.AnyAction{
			aliasedFencedAcquire(s, decl.Name),
			aliasedFencedRelease(s, decl.Name),
		}, nil
	case storage.KindIdem:
		return []action.AnyAction{
			aliasedIdemClaim(s, decl.Name),
			aliasedIdemComplete(s, decl.Name),
			aliasedIdemRelease(s, decl.Name),
		}, nil
	case storage.KindJournal:
		return []action.AnyAction{
			aliasedJournalGet(s, decl.Name),
			aliasedJournalRecord(s, decl.Name),
		}, nil
	case storage.KindTx:
		return []action.AnyAction{aliasedTxRun(s, decl.Name)}, nil
	case storage.KindRateLimit:
		return []action.AnyAction{aliasedRateLimitCheck(s, decl.Name)}, nil
	default:
		return nil, xerr.BadRequest("unknown kind " + string(decl.Kind))
	}
}
