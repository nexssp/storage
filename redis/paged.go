package redis

import (
	"context"
	"encoding/json"
	"errors"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nexssp/storage"
)

var appendPagedItemScript = goredis.NewScript(`
if redis.call('HEXISTS', KEYS[2], ARGV[1]) == 1 then
  return 0
end
local stream_id = redis.call('XADD', KEYS[1], '*', 'cursor', ARGV[1], 'value', ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], stream_id)
return 1
`)

func (i *instance) pagedStreamKey(key string) string {
	return i.key("kvpage-stream:" + key)
}

func (i *instance) pagedCursorKey(key string) string {
	return i.key("kvpage-cursors:" + key)
}

func (i *instance) Append(ctx context.Context, key, cursor string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if i.kind != storage.KindKV {
		return storage.BadRequestf("redis: instance is kind %q, expected %q", i.kind, storage.KindKV)
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
		return storage.BadRequestf("redis: encode page value: %v", err)
	}
	added, err := appendPagedItemScript.Run(ctx, i.client, []string{
		i.pagedStreamKey(key), i.pagedCursorKey(key),
	}, cursor, encoded).Int()
	if err != nil {
		return storage.Unavailablef(err, "redis: append paged value")
	}
	if added == 0 {
		return storage.Conflict("storage page cursor already exists")
	}
	return nil
}

func (i *instance) Page(ctx context.Context, key, after string, limit int) (storage.KVPage, error) {
	if err := ctx.Err(); err != nil {
		return storage.KVPage{}, err
	}
	if i.kind != storage.KindKV {
		return storage.KVPage{}, storage.BadRequestf("redis: instance is kind %q, expected %q", i.kind, storage.KindKV)
	}
	if limit <= 0 {
		return storage.KVPage{}, storage.BadRequest("storage page limit must be positive")
	}
	key, err := i.storageKey(ctx, key)
	if err != nil {
		return storage.KVPage{}, err
	}
	streamKey := i.pagedStreamKey(key)
	cursorFound := after == ""
	start := "0-0"
	if after != "" {
		start, err = i.client.HGet(ctx, i.pagedCursorKey(key), after).Result()
		if errors.Is(err, goredis.Nil) {
			return storage.KVPage{Items: []storage.KVPageItem{}, CursorFound: false}, nil
		}
		if err != nil {
			return storage.KVPage{}, storage.Unavailablef(err, "redis: look up page cursor")
		}
		cursorFound = true
		start = "(" + start
	}
	messages, err := i.client.XRangeN(ctx, streamKey, start, "+", int64(limit)).Result()
	if err != nil {
		return storage.KVPage{}, storage.Unavailablef(err, "redis: query page")
	}
	items := make([]storage.KVPageItem, 0, len(messages))
	for _, message := range messages {
		cursor, ok := message.Values["cursor"].(string)
		if !ok || cursor == "" {
			return storage.KVPage{}, storage.Internal("redis: paged stream item has no cursor")
		}
		raw, ok := redisValueBytes(message.Values["value"])
		if !ok {
			return storage.KVPage{}, storage.Internalf("redis: paged stream item %q has no value", cursor)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return storage.KVPage{}, storage.Internalf("redis: decode page item %q: %v", cursor, err)
		}
		items = append(items, storage.KVPageItem{Cursor: cursor, Value: value})
	}

	hasMore := false
	if len(messages) == limit && len(messages) > 0 {
		last := messages[len(messages)-1].ID
		remaining, err := i.client.XRangeN(ctx, streamKey, "("+last, "+", 1).Result()
		if err != nil {
			return storage.KVPage{}, storage.Unavailablef(err, "redis: check for remaining page items")
		}
		hasMore = len(remaining) > 0
	}
	return storage.KVPage{Items: items, CursorFound: cursorFound, HasMore: hasMore}, nil
}

var _ storage.PagedKV = (*instance)(nil)
