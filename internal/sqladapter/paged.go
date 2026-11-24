package sqladapter

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/nexssp/storage"
)

const pagedKVTable = "nexss_storage_kv_pages"

type sqlQueryExecutor interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (i *instance) Append(ctx context.Context, key, cursor string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if i.kind != storage.KindKV {
		return storage.BadRequestf("sql storage: instance is kind %q, expected %q", i.kind, storage.KindKV)
	}
	if cursor == "" {
		return storage.BadRequest("storage page cursor is required")
	}
	key, err := i.storageKey(ctx, key)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return storage.BadRequestf("sql storage: encode page value: %v", err)
	}
	executor, err := i.executor(ctx)
	if err != nil {
		return err
	}
	query := "INSERT INTO " + pagedKVTable + " (namespace, key, cursor, value) VALUES (" +
		i.placeholder(1) + "," + i.placeholder(2) + "," + i.placeholder(3) + "," + i.placeholder(4) +
		") ON CONFLICT (namespace, key, cursor) DO NOTHING"
	result, err := executor.ExecContext(ctx, query, i.namespace, key, cursor, encoded)
	if err != nil {
		return storage.Unavailablef(err, "sql storage: append paged value")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return storage.Unavailablef(err, "sql storage: inspect page append")
	}
	if changed == 0 {
		return storage.Conflict("storage page cursor already exists")
	}
	return nil
}

func (i *instance) Page(ctx context.Context, key, after string, limit int) (storage.KVPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.KVPage{}, err
	}
	if i.kind != storage.KindKV {
		return storage.KVPage{}, storage.BadRequestf("sql storage: instance is kind %q, expected %q", i.kind, storage.KindKV)
	}
	if limit <= 0 {
		return storage.KVPage{}, storage.BadRequest("storage page limit must be positive")
	}
	key, err := i.storageKey(ctx, key)
	if err != nil {
		return storage.KVPage{}, err
	}

	var afterSequence int64
	cursorFound := after == ""
	if after != "" {
		query := "SELECT page_sequence FROM " + pagedKVTable + " WHERE namespace=" + i.placeholder(1) +
			" AND key=" + i.placeholder(2) + " AND cursor=" + i.placeholder(3)
		row, queryErr := i.queryRow(ctx, query, i.namespace, key, after)
		if queryErr != nil {
			return storage.KVPage{}, storage.Unavailablef(queryErr, "sql storage: look up page cursor")
		}
		if scanErr := row.Scan(&afterSequence); scanErr != nil {
			if errors.Is(scanErr, sql.ErrNoRows) {
				return storage.KVPage{Items: []storage.KVPageItem{}, CursorFound: false}, nil
			}
			return storage.KVPage{}, storage.Unavailablef(scanErr, "sql storage: read page cursor")
		}
		cursorFound = true
	}

	query := "SELECT page_sequence, cursor, value FROM " + pagedKVTable + " WHERE namespace=" + i.placeholder(1) +
		" AND key=" + i.placeholder(2) + " AND page_sequence>" + i.placeholder(3) +
		" ORDER BY page_sequence ASC LIMIT " + i.placeholder(4)
	executor, err := i.executor(ctx)
	if err != nil {
		return storage.KVPage{}, err
	}
	queryer, ok := executor.(sqlQueryExecutor)
	if !ok {
		return storage.KVPage{}, storage.Internal("sql storage: executor does not support page queries")
	}
	rows, err := queryer.QueryContext(ctx, query, i.namespace, key, afterSequence, int64(limit))
	if err != nil {
		return storage.KVPage{}, storage.Unavailablef(err, "sql storage: query page")
	}
	defer rows.Close()
	items := make([]storage.KVPageItem, 0)
	var lastSequence int64
	for rows.Next() {
		var sequence int64
		var cursor string
		var raw []byte
		if err := rows.Scan(&sequence, &cursor, &raw); err != nil {
			return storage.KVPage{}, storage.Unavailablef(err, "sql storage: scan page item")
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return storage.KVPage{}, storage.Internalf("sql storage: decode page item %q: %v", cursor, err)
		}
		items = append(items, storage.KVPageItem{Cursor: cursor, Value: value})
		lastSequence = sequence
	}
	if err := rows.Err(); err != nil {
		return storage.KVPage{}, storage.Unavailablef(err, "sql storage: iterate page")
	}

	hasMore := false
	if len(items) == limit {
		query := "SELECT EXISTS (SELECT 1 FROM " + pagedKVTable + " WHERE namespace=" + i.placeholder(1) +
			" AND key=" + i.placeholder(2) + " AND page_sequence>" + i.placeholder(3) + ")"
		row, err := i.queryRow(ctx, query, i.namespace, key, lastSequence)
		if err != nil {
			return storage.KVPage{}, storage.Unavailablef(err, "sql storage: check for remaining page items")
		}
		if err := row.Scan(&hasMore); err != nil {
			return storage.KVPage{}, storage.Unavailablef(err, "sql storage: read remaining page status")
		}
	}
	return storage.KVPage{Items: items, CursorFound: cursorFound, HasMore: hasMore}, nil
}

var _ storage.PagedKV = (*instance)(nil)
