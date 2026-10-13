// Package sqlite provides SQLite implementations of storage KV, Blob, and
// Tx backends. Schema changes are deliberately not run by Open; deploy the
// embedded Goose migrations separately before starting applications.
package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"

	// Blank import registers the modernc SQLite database/sql driver.
	_ "modernc.org/sqlite"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/internal/sqladapter"
)

//go:embed migrations/sqlite/*.sql
var migrationFiles embed.FS

// NewKV returns a SQLite KV backend. Options: path (required), namespace,
// and ttl. Run the embedded migrations before using its methods.
func NewKV() storage.Backend {
	return sqladapter.NewBackend("sqlite", "sqlite", storage.KindKV, "path")
}

// NewBlob returns a SQLite Blob backend. Options: path (required) and namespace.
func NewBlob() storage.Backend {
	return sqladapter.NewBackend("sqlite", "sqlite", storage.KindBlob, "path")
}

// NewTx returns a SQLite transaction backend. Options: path (required).
func NewTx() storage.Backend {
	return sqladapter.NewBackend("sqlite", "sqlite", storage.KindTx, "path")
}

// Register adds all SQLite backend kinds to reg.
func Register(reg *storage.Registry) error {
	for _, backend := range []storage.Backend{NewKV(), NewBlob(), NewTx()} {
		if err := reg.Register(backend); err != nil {
			return err
		}
	}
	return nil
}

// Migrations returns the embedded SQLite Goose migration filesystems.
func Migrations() fs.FS {
	migrations, err := fs.Sub(migrationFiles, "migrations/sqlite")
	if err != nil {
		panic("sqlite: invalid embedded migrations directory: " + err.Error())
	}
	return migrations
}

// TxFromContext retrieves the transaction supplied to a TxRunner callback.
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	return sqladapter.TxFromContext(ctx)
}
