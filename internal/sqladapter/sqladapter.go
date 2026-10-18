// Package sqladapter contains the shared SQL implementation used by the
// SQLite and PostgreSQL adapters. It never creates or upgrades schema.
package sqladapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexssp/kernel/action"
	"github.com/nexssp/kernel/xfile"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/internal/blobref"
)

// Backend adapts one SQL driver and storage kind to the storage contract.
type Backend struct {
	name, driver, option string
	kind                 storage.Kind
}

// NewBackend constructs a backend. option is "path" for SQLite or "dsn"
// for PostgreSQL. SQL schemas must be upgraded separately with Goose.
func NewBackend(name, driver string, kind storage.Kind, option string) *Backend {
	return &Backend{name: name, driver: driver, kind: kind, option: option}
}

func (b *Backend) Name() string       { return b.name }
func (b *Backend) Kind() storage.Kind { return b.kind }

func (b *Backend) Open(opts map[string]string) (storage.Instance, error) {
	for key := range opts {
		if key != b.option && key != "namespace" && key != "ttl" {
			return nil, storage.BadRequestf("%s: unknown option %q", b.name, key)
		}
	}
	addr := opts[b.option]
	if addr == "" {
		return nil, storage.BadRequestf("%s: %q is required", b.name, b.option)
	}
	if b.driver == "sqlite" && !hasSQLiteBusyTimeout(addr) {
		separator := "?"
		if strings.Contains(addr, "?") {
			separator = "&"
		}
		addr += separator + "_busy_timeout=5000"
	}
	ttl, err := storage.ParseDuration(opts["ttl"], 0)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(b.driver, addr)
	if err != nil {
		return nil, storage.Unavailablef(err, "%s: open database", b.name)
	}
	if b.driver == "sqlite" && (strings.HasPrefix(addr, ":memory:") || strings.HasPrefix(addr, "file::memory:")) {
		db.SetMaxOpenConns(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, storage.Unavailablef(err, "%s: connect database", b.name)
	}
	namespace := opts["namespace"]
	if namespace == "" {
		namespace = "default"
	}
	inst := &instance{
		db:         db,
		name:       b.name,
		kind:       b.kind,
		namespace:  namespace,
		defaultTTL: ttl,
		postgres:   b.driver == "pgx",
		databaseID: sha256.Sum256([]byte(b.driver + "\x00" + addr)),
	}
	if b.kind == storage.KindBlob {
		return &blobInstance{instance: inst}, nil
	}
	return inst, nil
}

func hasSQLiteBusyTimeout(addr string) bool {
	return strings.Contains(addr, "_busy_timeout=") || strings.Contains(addr, "_timeout=") || strings.Contains(addr, "busy_timeout%3d")
}

type instance struct {
	db         *sql.DB
	name       string
	kind       storage.Kind
	namespace  string
	defaultTTL time.Duration
	postgres   bool
	databaseID [32]byte
	closeOnce  sync.Once
	closeErr   error
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (i *instance) executor(ctx context.Context) (sqlExecutor, error) {
	binding, ok := transactionFromContext(ctx)
	if !ok {
		return i.db, nil
	}
	if binding.databaseID != i.databaseID {
		return nil, storage.BadRequest("sql storage: transaction database does not match storage backend")
	}
	return binding.tx, nil
}

func (i *instance) queryRow(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	executor, err := i.executor(ctx)
	if err != nil {
		return nil, err
	}
	return executor.QueryRowContext(ctx, query, args...), nil
}

func (i *instance) exec(ctx context.Context, query string, args ...any) error {
	executor, err := i.executor(ctx)
	if err != nil {
		return err
	}
	_, err = executor.ExecContext(ctx, query, args...)
	return err
}

func (i *instance) Kind() storage.Kind { return i.kind }
func (i *instance) Close() error {
	i.closeOnce.Do(func() { i.closeErr = i.db.Close() })
	if i.closeErr != nil {
		return storage.Unavailablef(i.closeErr, "sql storage: close database")
	}
	return nil
}

func (i *instance) placeholder(n int) string {
	if i.postgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func (i *instance) table() string {
	if i.kind == storage.KindBlob {
		return "nexss_storage_blobs"
	}
	return "nexss_storage_kv"
}

func (i *instance) Get(ctx context.Context, key string) (value any, hit bool, resultErr error) {
	raw, hit, err := i.getRaw(ctx, key)
	if err != nil || !hit {
		return nil, hit, err
	}
	if decodeErr := json.Unmarshal(raw, &value); decodeErr != nil {
		return nil, false, storage.Internalf("sql storage: decode key %q: %v", key, decodeErr)
	}
	return value, true, nil
}

func (i *instance) getRaw(ctx context.Context, key string) (raw []byte, hit bool, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	var expires sql.NullInt64
	q := "SELECT value, expires_at FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2)
	row, err := i.queryRow(ctx, q, i.namespace, key)
	if err != nil {
		return nil, false, err
	}
	err = row.Scan(&raw, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, storage.Unavailablef(err, "sql storage: get key")
	}
	if expires.Valid && expires.Int64 <= time.Now().UnixNano() {
		if err := i.exec(ctx, "DELETE FROM "+i.table()+" WHERE namespace="+i.placeholder(1)+" AND key="+i.placeholder(2), i.namespace, key); err != nil {
			return nil, false, storage.Unavailablef(err, "sql storage: delete expired key")
		}
		return nil, false, nil
	}
	return append([]byte(nil), raw...), true, nil
}

var savepointSequence atomic.Uint64

// Update serializes the read/modify/write inside a SQL transaction. SQLite's
// no-op UPDATE obtains its database write lock before the SELECT (even on miss);
// PostgreSQL uses a transaction-scoped advisory lock, including for absent keys.
//
//nolint:gocyclo // Lock, callback, savepoint, and commit must share one transaction.
func (i *instance) Update(ctx context.Context, key string, fn func(any, bool) (any, time.Duration, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return storage.BadRequest("sql storage: atomic update callback is required")
	}
	binding, ambient := transactionFromContext(ctx)
	if ambient && binding.databaseID != i.databaseID {
		return storage.BadRequest("sql storage: transaction database does not match storage backend")
	}
	var tx *sql.Tx
	txctx := ctx
	owned := false
	if ambient {
		tx = binding.tx
	} else {
		var err error
		tx, err = i.db.BeginTx(ctx, nil)
		if err != nil {
			return storage.Unavailablef(err, "sql storage: begin atomic update")
		}
		txctx = context.WithValue(ctx, txContextKey{}, txBinding{tx: tx, databaseID: i.databaseID})
		owned = true
	}
	finish := func(err error) error {
		if owned {
			if err != nil {
				if rb := tx.Rollback(); rb != nil && !errors.Is(rb, sql.ErrTxDone) {
					return errors.Join(err, storage.Unavailablef(rb, "sql storage: rollback atomic update"))
				}
				return err
			}
			if e := tx.Commit(); e != nil {
				return storage.Unavailablef(e, "sql storage: commit atomic update")
			}
			return nil
		}
		return err
	}
	if i.postgres {
		// hashtextextended is stable within PostgreSQL and collisions only serialize unrelated keys.
		if _, err := tx.ExecContext(txctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", fmt.Sprintf("%d:%s%s", len(i.namespace), i.namespace, key)); err != nil {
			return finish(storage.Unavailablef(err, "sql storage: lock atomic key"))
		}
	} else {
		// Any UPDATE starts SQLite's write transaction before reading, preventing stale reads across connections/processes.
		if _, err := tx.ExecContext(txctx, "UPDATE nexss_storage_kv SET value=value WHERE namespace=? AND key=?", i.namespace, key); err != nil {
			return finish(storage.Unavailablef(err, "sql storage: lock atomic key"))
		}
	}
	savepoint := "storage_atomic_" + strconv.FormatUint(savepointSequence.Add(1), 10)
	if ambient {
		if _, err := tx.ExecContext(txctx, "SAVEPOINT "+savepoint); err != nil {
			return finish(storage.Unavailablef(err, "sql storage: create atomic savepoint"))
		}
	}
	raw, hit, err := i.getRaw(txctx, key)
	if err != nil {
		return i.atomicFail(txctx, tx, finish, ambient, savepoint, err)
	}
	var current any
	if hit {
		if err = json.Unmarshal(raw, &current); err != nil {
			return i.atomicFail(txctx, tx, finish, ambient, savepoint, storage.Internalf("sql storage: decode key %q: %v", key, err))
		}
	}
	next, ttl, err := fn(current, hit)
	if err != nil {
		return i.atomicFail(txctx, tx, finish, ambient, savepoint, err)
	}
	raw, err = json.Marshal(next)
	if err != nil {
		return i.atomicFail(txctx, tx, finish, ambient, savepoint, storage.BadRequestf("sql storage: encode value: %v", err))
	}
	if err = i.put(txctx, key, raw, ttl); err != nil {
		return i.atomicFail(txctx, tx, finish, ambient, savepoint, err)
	}
	if ambient {
		if _, err = tx.ExecContext(txctx, "RELEASE SAVEPOINT "+savepoint); err != nil {
			return finish(storage.Unavailablef(err, "sql storage: release atomic savepoint"))
		}
	}
	return finish(nil)
}

func (i *instance) atomicFail(ctx context.Context, tx *sql.Tx, finish func(error) error, ambient bool, savepoint string, cause error) error {
	if ambient {
		_, rb := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint)
		_, rel := tx.ExecContext(ctx, "RELEASE SAVEPOINT "+savepoint)
		if rb != nil || rel != nil {
			return finish(errors.Join(cause, rb, rel))
		}
	}
	return finish(cause)
}

func (i *instance) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return storage.BadRequestf("sql storage: encode value: %v", err)
	}
	return i.put(ctx, key, raw, ttl)
}

func (i *instance) put(ctx context.Context, key string, raw []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = i.defaultTTL
	}
	var expires any
	if ttl > 0 {
		expires = time.Now().Add(ttl).UnixNano()
	}
	p1, p2, p3, p4 := i.placeholder(1), i.placeholder(2), i.placeholder(3), i.placeholder(4)
	q := "INSERT INTO " + i.table() + " (namespace,key,value,expires_at) VALUES (" + p1 + "," + p2 + "," + p3 + "," + p4 + ") ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value, expires_at=excluded.expires_at"
	if err := i.exec(ctx, q, i.namespace, key, raw, expires); err != nil {
		return storage.Unavailablef(err, "sql storage: set key")
	}
	return nil
}

// Delete is an extension to CacheLayer exposed for Flow's kv.del action.
func (i *instance) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q := "DELETE FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2)
	if err := i.exec(ctx, q, i.namespace, key); err != nil {
		return storage.Unavailablef(err, "sql storage: delete key")
	}
	return nil
}

func (i *instance) Put(ctx context.Context, key string, data []byte) error {
	return i.put(ctx, key, data, 0)
}

type blobInstance struct{ *instance }

func (i *blobInstance) Get(ctx context.Context, key string) (xfile.Ref, bool, error) {
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, false, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return xfile.Ref{}, false, err
	}
	var meta blobref.Metadata
	q := "SELECT blob_size, digest, owner_user, owner_tenant FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2) + " AND owner_user=" + i.placeholder(3) + " AND owner_tenant=" + i.placeholder(4)
	row, err := i.queryRow(ctx, q, i.namespace, key, scope.CallerID, scope.TenantID)
	if err != nil {
		return xfile.Ref{}, false, err
	}
	err = row.Scan(&meta.Size, &meta.Digest, &meta.CallerID, &meta.TenantID)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !blobref.Authorized(scope, meta)) {
		return xfile.Ref{}, false, nil
	}
	if err != nil {
		return xfile.Ref{}, false, storage.Unavailablef(err, "sql storage: lookup blob")
	}
	if err := blobref.ValidateMetadata(meta); err != nil {
		return xfile.Ref{}, false, err
	}
	return blobref.NewRef(i.name, key, meta), true, nil
}

func (i *blobInstance) Put(ctx context.Context, key string, src io.Reader) (xfile.Ref, error) {
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return xfile.Ref{}, err
	}
	if src == nil {
		return xfile.Ref{}, storage.BadRequest("blob: source reader is required")
	}
	data, err := io.ReadAll(src)
	if err != nil {
		return xfile.Ref{}, storage.BadRequestf("blob: read source: %v", err)
	}
	meta := blobref.MetadataFor(scope, data)
	if err := ctx.Err(); err != nil {
		return xfile.Ref{}, err
	}
	p1, p2, p3, p4 := i.placeholder(1), i.placeholder(2), i.placeholder(3), i.placeholder(4)
	p5, p6, p7, p8 := i.placeholder(5), i.placeholder(6), i.placeholder(7), i.placeholder(8)
	// Table and placeholders are fixed adapter metadata; values are bound as parameters.
	q := "INSERT INTO " + i.table() + " (namespace,key,value,blob_size,digest,owner_user,owner_tenant,expires_at) VALUES (" + p1 + "," + p2 + "," + p3 + "," + p4 + "," + p5 + "," + p6 + "," + p7 + "," + p8 + ") ON CONFLICT(namespace,owner_tenant,owner_user,key) DO UPDATE SET value=excluded.value, blob_size=excluded.blob_size, digest=excluded.digest, expires_at=excluded.expires_at"
	if err := i.exec(ctx, q, i.namespace, key, data, meta.Size, meta.Digest, meta.CallerID, meta.TenantID, nil); err != nil {
		return xfile.Ref{}, storage.Unavailablef(err, "sql storage: put blob")
	}
	return blobref.NewRef(i.name, key, meta), nil
}

func (i *blobInstance) Open(ctx context.Context, ref xfile.Ref) (io.ReadCloser, xfile.Info, error) {
	if err := ctx.Err(); err != nil {
		return nil, xfile.Info{}, err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	key, err := blobref.Key(i.name, ref)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	var data []byte
	var meta blobref.Metadata
	q := "SELECT value, blob_size, digest, owner_user, owner_tenant FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2) + " AND owner_user=" + i.placeholder(3) + " AND owner_tenant=" + i.placeholder(4)
	row, err := i.queryRow(ctx, q, i.namespace, key, scope.CallerID, scope.TenantID)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	err = row.Scan(&data, &meta.Size, &meta.Digest, &meta.CallerID, &meta.TenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, xfile.Info{}, storage.NotFound("blob: reference not found")
	}
	if err != nil {
		return nil, xfile.Info{}, storage.Unavailablef(err, "sql storage: open blob")
	}
	if ownerErr := blobref.CheckOwner(scope, meta); ownerErr != nil {
		return nil, xfile.Info{}, ownerErr
	}
	size, digest, err := blobref.ConfirmStored(data, meta)
	if err != nil {
		return nil, xfile.Info{}, err
	}
	if err := blobref.Verify(ref, size, digest); err != nil {
		return nil, xfile.Info{}, err
	}
	return io.NopCloser(bytes.NewReader(data)), blobref.Info(ref, size, digest), nil
}

func (i *blobInstance) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, err := blobref.Scope(ctx)
	if err != nil {
		return err
	}
	// Table and placeholders are fixed adapter metadata; values are bound as parameters.
	q := "DELETE FROM " + i.table() + " WHERE namespace=" + i.placeholder(1) + " AND key=" + i.placeholder(2) + " AND owner_user=" + i.placeholder(3) + " AND owner_tenant=" + i.placeholder(4)
	if err := i.exec(ctx, q, i.namespace, key, scope.CallerID, scope.TenantID); err != nil {
		return storage.Unavailablef(err, "sql storage: delete blob")
	}
	return nil
}

func (i *instance) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return storage.BadRequest("sql storage: transaction callback is required")
	}
	if binding, ok := transactionFromContext(ctx); ok {
		if binding.databaseID != i.databaseID {
			return storage.BadRequest("sql storage: nested transaction database does not match active transaction")
		}
		return fn(ctx)
	}
	tx, err := i.db.BeginTx(ctx, nil)
	if err != nil {
		return storage.Unavailablef(err, "sql storage: begin transaction")
	}
	txCtx := context.WithValue(ctx, txContextKey{}, txBinding{tx: tx, databaseID: i.databaseID})
	if err := fn(txCtx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			return errors.Join(err, storage.Unavailablef(rbErr, "sql storage: rollback transaction"))
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return storage.Unavailablef(err, "sql storage: commit transaction")
	}
	return nil
}

type txContextKey struct{}

type txBinding struct {
	tx         *sql.Tx
	databaseID [32]byte
}

func transactionFromContext(ctx context.Context) (txBinding, bool) {
	binding, ok := ctx.Value(txContextKey{}).(txBinding)
	return binding, ok
}

// TxFromContext returns the SQL transaction placed in a RunInTx callback's
// context. It lets application code share the transaction with its own SQL.
func TxFromContext(ctx context.Context) (*sql.Tx, bool) {
	binding, ok := transactionFromContext(ctx)
	return binding.tx, ok
}

var (
	_ action.CacheLayer[any] = (*instance)(nil)
	_ storage.AtomicKV       = (*instance)(nil)
	_ storage.BlobStore      = (*blobInstance)(nil)
	_ xfile.Resolver         = (*blobInstance)(nil)
	_ action.TxRunner        = (*instance)(nil)
)
