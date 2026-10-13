package nexssflow

import (
	"testing"

	"github.com/nexssp/storage"
)

func TestBundleRegistersPersistentBackends(t *testing.T) {
	b := newBundle()
	for _, pair := range []struct {
		name string
		kind storage.Kind
	}{
		{"sqlite", storage.KindKV},
		{"sqlite", storage.KindBlob},
		{"sqlite", storage.KindTx},
		{"postgres", storage.KindKV},
		{"postgres", storage.KindBlob},
		{"postgres", storage.KindTx},
		{"redis", storage.KindKV},
		{"redis", storage.KindBlob},
		{"redis", storage.KindLock},
	} {
		if _, ok := b.reg.Lookup(pair.name, pair.kind); !ok {
			t.Errorf("missing backend %s (%s)", pair.name, pair.kind)
		}
	}
}
