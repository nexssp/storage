package memory

import (
	"context"
	"slices"

	"github.com/nexssp/storage"
	"github.com/nexssp/storage/internal/tenantkey"
)

func (k *kvInstance) Append(ctx context.Context, key, cursor string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cursor == "" {
		return storage.BadRequest("storage page cursor is required")
	}
	if k.tenantScoped {
		var err error
		key, err = tenantkey.Key(ctx, key)
		if err != nil {
			return err
		}
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	if k.pages == nil {
		k.pages = make(map[string][]storage.KVPageItem)
	}
	if k.pageCursors == nil {
		k.pageCursors = make(map[string]map[string]int)
	}
	positions := k.pageCursors[key]
	if _, exists := positions[cursor]; exists {
		return storage.Conflict("storage page cursor already exists")
	}
	if positions == nil {
		positions = make(map[string]int)
		k.pageCursors[key] = positions
	}
	positions[cursor] = len(k.pages[key])
	k.pages[key] = append(k.pages[key], storage.KVPageItem{Cursor: cursor, Value: value})
	return nil
}

func (k *kvInstance) Page(ctx context.Context, key, after string, limit int) (storage.KVPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.KVPage{}, err
	}
	if limit <= 0 {
		return storage.KVPage{}, storage.BadRequest("storage page limit must be positive")
	}
	if k.tenantScoped {
		var err error
		key, err = tenantkey.Key(ctx, key)
		if err != nil {
			return storage.KVPage{}, err
		}
	}

	k.mu.RLock()
	defer k.mu.RUnlock()
	items := k.pages[key]
	start := 0
	if after != "" {
		position, found := k.pageCursors[key][after]
		if !found {
			return storage.KVPage{Items: []storage.KVPageItem{}, CursorFound: false}, nil
		}
		start = position + 1
	}

	end := len(items)
	if remaining := len(items) - start; limit < remaining {
		end = start + limit
	}
	return storage.KVPage{
		Items:       slices.Clone(items[start:end]),
		CursorFound: true,
		HasMore:     end < len(items),
	}, nil
}

var _ storage.PagedKV = (*kvInstance)(nil)
