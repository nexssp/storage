package nexssflow

import (
	"context"
	"errors"
	"testing"

	"github.com/nexssp/storage"
)

type closeTestStorageInstance struct {
	closeErr   error
	closeCount int
}

func (*closeTestStorageInstance) Kind() storage.Kind { return storage.KindKV }
func (i *closeTestStorageInstance) Close() error {
	i.closeCount++
	return i.closeErr
}

func TestBundleCloseAttemptsAllInstancesAndJoinsErrors(t *testing.T) {
	closeErr := errors.New("backend close failed")
	first := &closeTestStorageInstance{closeErr: closeErr}
	second := &closeTestStorageInstance{}
	b := &bundle{stores: []*store{
		{insts: map[string]storage.Instance{"first": first}},
		{insts: map[string]storage.Instance{"second": second}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := b.Close(ctx)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v, want cancellation and backend close error", err)
	}
	if first.closeCount != 1 || second.closeCount != 1 {
		t.Fatalf("close counts = (%d, %d), want (1, 1)", first.closeCount, second.closeCount)
	}
}
