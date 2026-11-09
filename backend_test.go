package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/nexssp/kernel/xerr"
)

type errorTestBackend struct {
	err error
}

func (errorTestBackend) Name() string { return "error-test" }
func (errorTestBackend) Kind() Kind   { return KindKV }
func (b errorTestBackend) Open(map[string]string) (Instance, error) {
	return nil, b.err
}

func openErrorTestBackend(t *testing.T, backendErr error) error {
	t.Helper()

	reg := NewRegistry()
	if err := reg.Register(errorTestBackend{err: backendErr}); err != nil {
		t.Fatalf("register backend: %v", err)
	}

	_, err := (Declaration{
		Name:    "test",
		Backend: "error-test",
		Kind:    KindKV,
	}).Open(reg)
	return err
}

func sameErrorInstance(left, right error) bool {
	leftValue, rightValue := reflect.ValueOf(left), reflect.ValueOf(right)
	return leftValue.IsValid() && rightValue.IsValid() &&
		leftValue.Type() == rightValue.Type() && leftValue.Kind() == reflect.Pointer &&
		leftValue.Pointer() == rightValue.Pointer()
}

func TestRegistryOpenPreservesClassifiedWrappedError(t *testing.T) {
	cause := errors.New("invalid option")
	classified := xerr.BadRequest("bad storage options", cause)
	wrapped := fmt.Errorf("backend initialization: %w", classified)

	got := openErrorTestBackend(t, wrapped)
	if !sameErrorInstance(got, wrapped) {
		t.Fatalf("Open error = %v, want original wrapped error %v", got, wrapped)
	}
	if !errors.Is(got, cause) {
		t.Fatalf("Open error %v does not preserve cause %v", got, cause)
	}
	appErr, ok := errors.AsType[*xerr.AppError](got)
	if !ok || appErr != classified {
		t.Fatalf("classified error = %v, want original AppError %v", appErr, classified)
	}
}

func TestRegistryOpenPreservesJoinedClassifiedError(t *testing.T) {
	classified := xerr.NotFound("storage object not found")
	other := errors.New("additional backend diagnostic")
	joined := errors.Join(classified, other)

	got := openErrorTestBackend(t, joined)
	if !sameErrorInstance(got, joined) {
		t.Fatalf("Open error = %v, want original joined error %v", got, joined)
	}
	if !errors.Is(got, classified) || !errors.Is(got, other) {
		t.Fatalf("Open error %v does not preserve both joined errors", got)
	}
}

func TestRegistryOpenClassifiesUnknownErrorAndPreservesCause(t *testing.T) {
	cause := errors.New("connection refused")
	backendErr := fmt.Errorf("connect to storage: %w", cause)

	got := openErrorTestBackend(t, backendErr)
	appErr, ok := errors.AsType[*xerr.AppError](got)
	if !ok {
		t.Fatalf("Open error %T = %v, want *xerr.AppError", got, got)
	}
	if appErr.Kind != xerr.KindUnavailable {
		t.Fatalf("Open error kind = %q, want %q", appErr.Kind, xerr.KindUnavailable)
	}
	if !sameErrorInstance(appErr.Cause, backendErr) {
		t.Fatalf("Unavailable cause = %v, want original backend error %v", appErr.Cause, backendErr)
	}
	if !errors.Is(got, cause) {
		t.Fatalf("Open error %v does not preserve root cause %v", got, cause)
	}
}

type closeTestInstance struct {
	closeCount int
	closeErr   error
}

func (*closeTestInstance) Kind() Kind { return KindKV }
func (i *closeTestInstance) Close() error {
	i.closeCount++
	return i.closeErr
}

type closeTestBackend struct {
	instances []*closeTestInstance
	opened    int
}

func (*closeTestBackend) Name() string { return "close-test" }
func (*closeTestBackend) Kind() Kind   { return KindKV }
func (b *closeTestBackend) Open(map[string]string) (Instance, error) {
	instance := b.instances[b.opened]
	b.opened++
	return instance, nil
}

func TestRegistryCloseAttemptsAllInstancesAfterCancellation(t *testing.T) {
	closeErr := errors.New("close failed")
	instances := []*closeTestInstance{{closeErr: closeErr}, {}}
	reg := NewRegistry()
	if err := reg.Register(&closeTestBackend{instances: instances}); err != nil {
		t.Fatal(err)
	}
	declaration := Declaration{Name: "test", Backend: "close-test", Kind: KindKV}
	for range instances {
		if _, err := declaration.Open(reg); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := reg.Close(ctx)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, closeErr) {
		t.Fatalf("Close error = %v, want cancellation and backend close error", err)
	}
	for index, instance := range instances {
		if instance.closeCount != 1 {
			t.Errorf("instance %d Close calls = %d, want 1", index, instance.closeCount)
		}
	}
}
