// Package postgres provides PostgreSQL implementations of storage KV, Blob,
// and Tx backends. Schema changes are deliberately not run by Open; deploy the
// embedded Goose migrations separately before starting applications.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"

	// Blank import registers the pgx database/sql driver.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/internal/sqladapter"
)

//go:embed migrations/postgres/*.sql
var migrationFiles embed.FS

// NewKV returns a PostgreSQL KV backend. Options: dsn (required), namespace,
// and ttl. Run the embedded migrations before using its methods.
func NewKV() storage.Backend {
	return sqladapter.NewBackend("postgres", "pgx", storage.KindKV, "dsn")
}

// NewBlob returns a PostgreSQL Blob backend. Options: dsn (required) and namespace.
func NewBlob() storage.Backend {
	return sqladapter.NewBackend("postgres", "pgx", storage.KindBlob, "dsn")
}

// NewTx returns a PostgreSQL transaction backend. Options: dsn (required).
func NewTx() storage.Backend {
	return sqladapter.NewBackend("postgres", "pgx", storage.KindTx, "dsn")
}

// Register adds all PostgreSQL backend kinds to reg.
func Register(reg *storage.Registry) error {
	for _, backend := range []storage.Backend{NewKV(), NewBlob(), NewTx()} {
		if err := reg.Register(backend); err != nil {
			return err
		}
	}
	return nil
}

// Migrations returns the embedded PostgreSQL Goose migration filesystem.
func Migrations() fs.FS {
	migrations, err := fs.Sub(migrationFiles, "migrations/postgres")
	if err != nil {
		panic("postgres: invalid embedded migrations directory: " + err.Error())
	}
	return migrations
}

// TxFromContext retrieves the transaction supplied to a TxRunner callback.
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	return sqladapter.TxFromContext(ctx)
}
