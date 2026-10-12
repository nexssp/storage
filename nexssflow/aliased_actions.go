package nexssflow

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nexssp/kernel/action"

	"github.com/nexssp/storage"
)

// aliased_actions.go builds one action per (declaration, operation)
// pair. Each action's canonical name is "<decl.Name>.<kind>.<op>"
// (e.g. "cache.kv.get"). The store name is baked into a closure, so
// callers don't pass a `store:` argument — the compiler resolves the
// action name at capability-lookup time, producing a clean
// compile-time error if the store was not declared.
//
// All aliased actions record events into the request context (when
// the storage.events hook is attached) exactly like the generic
// storage.* actions.

// ---------------------------------------------------------------------------
// KV aliased actions — <alias>.kv.{get,set,del}
// ---------------------------------------------------------------------------

type aliasedKVGetReq struct {
	Key string `json:"key"`
}

func aliasedKVGet(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".kv.get",
		func(ctx context.Context, req aliasedKVGetReq) (kvGetRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				recordStorageEvent(ctx, "error", storeName, storeName+".kv.get", req.Key)
				return kvGetRes{}, err
			}
			layer, err := storage.AssertKV(inst)
			if err != nil {
				return kvGetRes{}, err
			}
			val, hit, err := layer.Get(ctx, req.Key)
			if err != nil {
				recordStorageEvent(ctx, "error", storeName, storeName+".kv.get", req.Key)
				return kvGetRes{}, storage.Unavailable("kv.get failed", err)
			}
			if hit {
				recordStorageEvent(ctx, "cache_hit", storeName, storeName+".kv.get", req.Key)
			} else {
				recordStorageEvent(ctx, "cache_miss", storeName, storeName+".kv.get", req.Key)
			}
			return kvGetRes{Value: val, Hit: hit}, nil
		}).
		Description("Get a value from "+storeName).
		Tag("storage", "kv", "get").
		Build()
}

type aliasedKVSetReq struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
	TTL   string `json:"ttl,omitempty"`
}

func aliasedKVSet(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".kv.set",
		func(ctx context.Context, req aliasedKVSetReq) (kvSetRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				return kvSetRes{}, err
			}
			layer, err := storage.AssertKV(inst)
			if err != nil {
				return kvSetRes{}, err
			}
			ttl, err := storage.ParseDuration(req.TTL, 0)
			if err != nil {
				return kvSetRes{}, err
			}
			if err := layer.Set(ctx, req.Key, req.Value, ttl); err != nil {
				return kvSetRes{}, storage.Unavailable("kv.set failed", err)
			}
			return kvSetRes{OK: true}, nil
		}).
		Description("Set a value in "+storeName).
		Tag("storage", "kv", "set").
		Build()
}

type aliasedKVDelReq struct {
	Key string `json:"key"`
}

func aliasedKVDel(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".kv.del",
		func(ctx context.Context, req aliasedKVDelReq) (kvDelRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				return kvDelRes{}, err
			}
			layer, err := storage.AssertKV(inst)
			if err != nil {
				return kvDelRes{}, err
			}
			type deleter interface {
				Delete(ctx context.Context, key string) error
			}
			d, ok := layer.(deleter)
			if !ok {
				return kvDelRes{}, storage.BadRequestf(
					"backend does not support Delete")
			}
			if err := d.Delete(ctx, req.Key); err != nil {
				return kvDelRes{}, storage.Unavailable("kv.del failed", err)
			}
			return kvDelRes{OK: true}, nil
		}).
		Description("Delete a key from "+storeName).
		Tag("storage", "kv", "del").
		Build()
}

// ---------------------------------------------------------------------------
// Blob aliased actions — <alias>.blob.{get,put,del}
// ---------------------------------------------------------------------------

func aliasedBlobGet(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".blob.get",
		func(ctx context.Context, req struct {
			Key string `json:"key"`
		},
		) (blobGetRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				return blobGetRes{}, err
			}
			store, err := storage.AssertBlob(inst)
			if err != nil {
				return blobGetRes{}, err
			}
			data, hit, err := store.Get(ctx, req.Key)
			if err != nil {
				return blobGetRes{}, storage.Unavailable("blob.get failed", err)
			}
			return blobGetRes{Data: data, Hit: hit}, nil
		}).
		Description("Get a blob from "+storeName).
		Tag("storage", "blob", "get").
		Build()
}

func aliasedBlobPut(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".blob.put",
		func(ctx context.Context, req struct {
			Key  string `json:"key"`
			Data any    `json:"data"`
		},
		) (blobPutRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				return blobPutRes{}, err
			}
			store, err := storage.AssertBlob(inst)
			if err != nil {
				return blobPutRes{}, err
			}
			data, err := toBytes(req.Data)
			if err != nil {
				return blobPutRes{}, err
			}
			if err := store.Put(ctx, req.Key, data); err != nil {
				return blobPutRes{}, storage.Unavailable("blob.put failed", err)
			}
			return blobPutRes{OK: true}, nil
		}).
		Description("Put a blob into "+storeName).
		Tag("storage", "blob", "put").
		Build()
}

// different Kind (Blob) and calls a different method (Delete).
//
//nolint:dupl // structurally similar to aliasedLockRelease but asserts a
func aliasedBlobDel(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".blob.del",
		func(ctx context.Context, req struct {
			Key string `json:"key"`
		},
		) (blobDelRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				return blobDelRes{}, err
			}
			store, err := storage.AssertBlob(inst)
			if err != nil {
				return blobDelRes{}, err
			}
			if err := store.Delete(ctx, req.Key); err != nil {
				return blobDelRes{}, storage.Unavailable("blob.del failed", err)
			}
			return blobDelRes{OK: true}, nil
		}).
		Description("Delete a blob from "+storeName).
		Tag("storage", "blob", "del").
		Build()
}

// ---------------------------------------------------------------------------
// Lock aliased actions — <alias>.{acquire,release}
// ---------------------------------------------------------------------------

func aliasedLockAcquire(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".acquire",
		func(ctx context.Context, req struct {
			Key string `json:"key"`
			TTL string `json:"ttl"`
		},
		) (lockAcquireRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				return lockAcquireRes{}, err
			}
			m, err := storage.AssertMutex(inst)
			if err != nil {
				return lockAcquireRes{}, err
			}
			ttl, err := storage.ParseDuration(req.TTL, 30*time.Second)
			if err != nil {
				return lockAcquireRes{}, err
			}
			ok, err := m.TryLock(ctx, req.Key, ttl)
			if err != nil {
				return lockAcquireRes{}, storage.Unavailable("acquire failed", err)
			}
			return lockAcquireRes{Acquired: ok}, nil
		}).
		Description("Acquire a lock from "+storeName).
		Tag("storage", "lock", "acquire").
		Build()
}

// different Kind (Mutex) and calls a different method (Unlock).
//
//nolint:dupl // structurally similar to aliasedBlobDel but asserts a
func aliasedLockRelease(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".release",
		func(ctx context.Context, req struct {
			Key string `json:"key"`
		},
		) (lockReleaseRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				return lockReleaseRes{}, err
			}
			m, err := storage.AssertMutex(inst)
			if err != nil {
				return lockReleaseRes{}, err
			}
			if err := m.Unlock(ctx, req.Key); err != nil {
				return lockReleaseRes{}, storage.Unavailable("release failed", err)
			}
			return lockReleaseRes{OK: true}, nil
		}).
		Description("Release a lock from "+storeName).
		Tag("storage", "lock", "release").
		Build()
}

// ---------------------------------------------------------------------------
// FencedLock aliased actions — <alias>.fenced.{acquire,renew,release}
// ---------------------------------------------------------------------------

func aliasedFencedAcquire(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".fenced.acquire",
		func(ctx context.Context, req struct {
			Key string `json:"key"`
			TTL string `json:"ttl"`
		}) (struct {
			Lease    any  `json:"lease"`
			Acquired bool `json:"acquired"`
		}, error,
		) {
			inst, err := s.instance(storeName)
			if err != nil {
				return struct {
					Lease    any  `json:"lease"`
					Acquired bool `json:"acquired"`
				}{}, err
			}
			m, err := storage.AssertFencedMutex(inst)
			if err != nil {
				return struct {
					Lease    any  `json:"lease"`
					Acquired bool `json:"acquired"`
				}{}, err
			}
			ttl, err := storage.ParseDuration(req.TTL, 30*time.Second)
			if err != nil {
				return struct {
					Lease    any  `json:"lease"`
					Acquired bool `json:"acquired"`
				}{}, err
			}
			lease, ok, err := m.Acquire(ctx, req.Key, ttl)
			if err != nil {
				return struct {
					Lease    any  `json:"lease"`
					Acquired bool `json:"acquired"`
				}{}, storage.Unavailable("fenced acquire failed", err)
			}
			return struct {
				Lease    any  `json:"lease"`
				Acquired bool `json:"acquired"`
			}{Lease: lease, Acquired: ok}, nil
		}).
		Description("Acquire a fenced lock from "+storeName).
		Tag("storage", "fenced", "acquire").
		Build()
}

func aliasedFencedRelease(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".fenced.release",
		func(ctx context.Context, req struct {
			Key   string `json:"key"`
			Lease any    `json:"lease"`
		},
		) (lockReleaseRes, error) {
			inst, err := s.instance(storeName)
			if err != nil {
				return lockReleaseRes{}, err
			}
			m, err := storage.AssertFencedMutex(inst)
			if err != nil {
				return lockReleaseRes{}, err
			}
			// The lease is passed as a map; reconstruct it.
			lease := leaseFromAny(req.Lease)
			_, err = m.Release(ctx, lease)
			if err != nil {
				return lockReleaseRes{}, storage.Unavailable("fenced release failed", err)
			}
			return lockReleaseRes{OK: true}, nil
		}).
		Description("Release a fenced lock from "+storeName).
		Tag("storage", "fenced", "release").
		Build()
}

// ---------------------------------------------------------------------------
// Idempotency aliased actions — <alias>.idem.{claim,complete,release}
// ---------------------------------------------------------------------------

func aliasedIdemClaim(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".idem.claim",
		func(ctx context.Context, req struct {
			Key         string `json:"key"`
			RequestHash string `json:"request_hash"`
			LeaseTTL    string `json:"lease_ttl"`
		}) (struct {
			State string `json:"state"`
			Token string `json:"token"`
		}, error,
		) {
			inst, err := s.instance(storeName)
			if err != nil {
				return struct {
					State string `json:"state"`
					Token string `json:"token"`
				}{}, err
			}
			coord, ok := inst.(action.IdempotencyCoordinator)
			if !ok {
				return struct {
					State string `json:"state"`
					Token string `json:"token"`
				}{}, storage.BadRequestf("backend does not support IdempotencyCoordinator")
			}
			leaseTTL, err := storage.ParseDuration(req.LeaseTTL, 2*time.Minute)
			if err != nil {
				return struct {
					State string `json:"state"`
					Token string `json:"token"`
				}{}, err
			}
			claim, err := coord.Claim(ctx, req.Key, req.RequestHash, leaseTTL)
			if err != nil {
				return struct {
					State string `json:"state"`
					Token string `json:"token"`
				}{}, storage.Unavailable("idem claim failed", err)
			}
			return struct {
				State string `json:"state"`
				Token string `json:"token"`
			}{State: claimStateString(claim.State), Token: claim.Token}, nil
		}).
		Description("Claim an idempotency key on "+storeName).
		Tag("storage", "idem", "claim").
		Build()
}

func aliasedIdemComplete(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".idem.complete",
		func(ctx context.Context, req struct {
			Key   string `json:"key"`
			Token string `json:"token"`
			Body  any    `json:"body"`
		}) (struct {
			OK bool `json:"ok"`
		}, error,
		) {
			inst, err := s.instance(storeName)
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, err
			}
			coord, ok := inst.(action.IdempotencyCoordinator)
			if !ok {
				return struct {
					OK bool `json:"ok"`
				}{}, storage.BadRequestf("backend does not support IdempotencyCoordinator")
			}
			bodyBytes, err := json.Marshal(req.Body)
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, storage.BadRequestf("cannot encode body: %v", err)
			}
			entry := action.IdempotencyEntry{
				Body:        bodyBytes,
				StoredAt:    time.Now(),
				RequestHash: req.Token,
			}
			if err := coord.Complete(ctx, req.Key, req.Token, entry, time.Hour); err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, storage.Unavailable("idem complete failed", err)
			}
			return struct {
				OK bool `json:"ok"`
			}{OK: true}, nil
		}).
		Description("Complete an idempotency claim on "+storeName).
		Tag("storage", "idem", "complete").
		Build()
}

func aliasedIdemRelease(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".idem.release",
		func(ctx context.Context, req struct {
			Key   string `json:"key"`
			Token string `json:"token"`
		}) (struct {
			OK bool `json:"ok"`
		}, error,
		) {
			inst, err := s.instance(storeName)
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, err
			}
			coord, ok := inst.(action.IdempotencyCoordinator)
			if !ok {
				return struct {
					OK bool `json:"ok"`
				}{}, storage.BadRequestf("backend does not support IdempotencyCoordinator")
			}
			if err := coord.Release(ctx, req.Key, req.Token); err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, storage.Unavailable("idem release failed", err)
			}
			return struct {
				OK bool `json:"ok"`
			}{OK: true}, nil
		}).
		Description("Release an idempotency claim on "+storeName).
		Tag("storage", "idem", "release").
		Build()
}

// ---------------------------------------------------------------------------
// Journal aliased actions — <alias>.journal.{get,record}
// ---------------------------------------------------------------------------

func aliasedJournalGet(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".journal.get",
		func(ctx context.Context, req struct {
			ExecutionID string `json:"execution_id"`
			Step        string `json:"step"`
		}) (struct {
			Data []byte `json:"data"`
			Hit  bool   `json:"hit"`
		}, error,
		) {
			inst, err := s.instance(storeName)
			if err != nil {
				return struct {
					Data []byte `json:"data"`
					Hit  bool   `json:"hit"`
				}{}, err
			}
			j, err := storage.AssertJournal(inst)
			if err != nil {
				return struct {
					Data []byte `json:"data"`
					Hit  bool   `json:"hit"`
				}{}, err
			}
			data, hit, err := j.GetStep(ctx, req.ExecutionID, req.Step)
			if err != nil {
				return struct {
					Data []byte `json:"data"`
					Hit  bool   `json:"hit"`
				}{}, storage.Unavailable("journal get failed", err)
			}
			return struct {
				Data []byte `json:"data"`
				Hit  bool   `json:"hit"`
			}{Data: data, Hit: hit}, nil
		}).
		Description("Get a journal step from "+storeName).
		Tag("storage", "journal", "get").
		Build()
}

func aliasedJournalRecord(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".journal.record",
		func(ctx context.Context, req struct {
			ExecutionID string `json:"execution_id"`
			Step        string `json:"step"`
			Data        any    `json:"data"`
		}) (struct {
			OK bool `json:"ok"`
		}, error,
		) {
			inst, err := s.instance(storeName)
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, err
			}
			j, err := storage.AssertJournal(inst)
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, err
			}
			dataBytes, err := toBytes(req.Data)
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, err
			}
			if err := j.RecordStep(ctx, req.ExecutionID, req.Step, dataBytes); err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, storage.Unavailable("journal record failed", err)
			}
			return struct {
				OK bool `json:"ok"`
			}{OK: true}, nil
		}).
		Description("Record a journal step on "+storeName).
		Tag("storage", "journal", "record").
		Build()
}

// ---------------------------------------------------------------------------
// Tx aliased action — <alias>.tx.run
// ---------------------------------------------------------------------------

func aliasedTxRun(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".tx.run",
		func(ctx context.Context, _ struct {
			Step string `json:"step"`
		}) (struct {
			OK bool `json:"ok"`
		}, error,
		) {
			inst, err := s.instance(storeName)
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, err
			}
			runner, err := storage.AssertTxRunner(inst)
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, err
			}
			err = runner.RunInTx(ctx, func(_ context.Context) error { return nil })
			if err != nil {
				return struct {
					OK bool `json:"ok"`
				}{}, storage.Unavailable("tx.run failed", err)
			}
			return struct {
				OK bool `json:"ok"`
			}{OK: true}, nil
		}).
		Description("Run in a transaction on "+storeName).
		Tag("storage", "tx", "run").
		Build()
}

// ---------------------------------------------------------------------------
// RateLimit aliased action — <alias>.rate.check
// ---------------------------------------------------------------------------

func aliasedRateLimitCheck(s *store, storeName string) action.AnyAction {
	return action.New(storeName+".rate.check",
		func(ctx context.Context, req struct {
			Key string `json:"key"`
		}) (struct {
			Allowed bool `json:"allowed"`
		}, error,
		) {
			inst, err := s.instance(storeName)
			if err != nil {
				return struct {
					Allowed bool `json:"allowed"`
				}{}, err
			}
			rl, err := storage.AssertRateLimiter(inst)
			if err != nil {
				return struct {
					Allowed bool `json:"allowed"`
				}{}, err
			}
			ok, err := rl.Allow(ctx, req.Key)
			if err != nil {
				return struct {
					Allowed bool `json:"allowed"`
				}{}, storage.Unavailable("rate.check failed", err)
			}
			return struct {
				Allowed bool `json:"allowed"`
			}{Allowed: ok}, nil
		}).
		Description("Check rate limit on "+storeName).
		Tag("storage", "rate", "check").
		Build()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func claimStateString(s action.IdempotencyClaimState) string {
	switch s {
	case action.IdempotencyClaimAcquired:
		return "acquired"
	case action.IdempotencyClaimCompleted:
		return "completed"
	case action.IdempotencyClaimInProgress:
		return "in_progress"
	case action.IdempotencyClaimConflict:
		return "conflict"
	default:
		return "unknown"
	}
}

func leaseFromAny(v any) action.LockLease {
	m, ok := v.(map[string]any)
	if !ok {
		return action.LockLease{}
	}
	lease := action.LockLease{}
	if s, ok := m["Key"].(string); ok {
		lease.Key = s
	}
	if s, ok := m["Owner"].(string); ok {
		lease.Owner = s
	}
	if f, ok := m["Fence"].(float64); ok {
		lease.Fence = int64(f)
	}
	return lease
}
