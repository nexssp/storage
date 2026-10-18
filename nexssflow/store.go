package nexssflow

import (
	"context"
	"sync"

	"github.com/nexssp/kernel/xctx"

	"github.com/nexssp/storage"
)

// storeKey carries the per-compilation store through the execution
// context so generic `storage.*` actions and the WrapPipeline hook can
// find it without a runtime `store:` argument.
var storeKey = xctx.NewKey[*store]("storage.store")

// pipelineTransactionKey identifies the tx declaration currently
// wrapping a Flow pipeline. The aliased tx.run action uses it as an
// explicit in-pipeline assertion; transaction boundaries are established
// by WrapPipeline, not by an individual action.
var pipelineTransactionKey = xctx.NewKey[string]("storage.pipeline_transaction")

// bundle owns the backend registry (shared, read-only after init) and
// tracks every per-compilation store so Shutdown can close them.
type bundle struct {
	reg *storage.Registry

	mu     sync.Mutex
	stores []*store
}

func newBundle() *bundle {
	b := &bundle{reg: storage.NewRegistry()}
	registerMemoryBackends(b.reg)
	registerPersistentBackends(b.reg)
	return b
}

func (b *bundle) newStore(decls []storage.Declaration) *store {
	s := &store{
		reg:   b.reg,
		decls: make(map[string]storage.Declaration, len(decls)),
		insts: make(map[string]storage.Instance),
	}
	for _, d := range decls {
		s.decls[d.Name] = d
	}
	b.mu.Lock()
	b.stores = append(b.stores, s)
	b.mu.Unlock()
	return s
}

func (b *bundle) Close(ctx context.Context) error {
	b.mu.Lock()
	stores := b.stores
	b.stores = nil
	b.mu.Unlock()
	for _, s := range stores {
		_ = s.Close(ctx)
	}
	return nil
}

type store struct {
	reg   *storage.Registry
	decls map[string]storage.Declaration
	insts map[string]storage.Instance
	mu    sync.Mutex
}

func (s *store) declaration(name string) (storage.Declaration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.decls[name]
	if !ok {
		return storage.Declaration{}, storage.NotFoundf("storage: store %q not declared", name)
	}
	return d, nil
}

func (s *store) instance(name string) (storage.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst, ok := s.insts[name]; ok {
		return inst, nil
	}
	decl, ok := s.decls[name]
	if !ok {
		return nil, storage.NotFoundf("storage: store %q not declared", name)
	}
	inst, err := decl.Open(s.reg)
	if err != nil {
		return nil, err
	}
	s.insts[name] = inst
	return inst, nil
}

func (s *store) Close(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, inst := range s.insts {
		_ = inst.Close()
	}
	s.insts = make(map[string]storage.Instance)
	return nil
}

// storeFromContext returns the store installed by the WrapPipeline hook.
func storeFromContext(ctx context.Context) (*store, bool) {
	s, ok := storeKey.From(ctx)
	return s, ok && s != nil
}
