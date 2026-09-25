package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisCommands is the narrow slice of *redis.Client this demo actually
// calls. Depending on this instead of *redis.Client directly means the
// cache-aside logic in Service can be unit tested against a fake that
// implements just these three methods — no live HydraCache cluster
// required to prove the logic is correct. The wiring to a real go-redis
// client against a real HydraCache server is already proven separately
// by internal/network's go-redis compatibility suite in the main module.
type redisCommands interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

// ProductCache is a thin cache-aside layer over HydraCache: it never talks
// to the Store itself, only Get/Set/Invalidate for the JSON-encoded
// Product a Service puts there.
type ProductCache struct {
	client redisCommands
	ttl    time.Duration
}

func NewProductCache(client redisCommands, ttl time.Duration) *ProductCache {
	return &ProductCache{client: client, ttl: ttl}
}

func productKey(id int64) string {
	return fmt.Sprintf("product:%d", id)
}

// Get returns the cached product, or (Product{}, false, nil) on a clean
// cache miss. A non-nil error means the cache itself is unreachable or
// returned malformed data — the caller falls back to the Store rather
// than treating either case as "product doesn't exist."
func (c *ProductCache) Get(ctx context.Context, id int64) (Product, bool, error) {
	raw, err := c.client.Get(ctx, productKey(id)).Result()
	if errors.Is(err, redis.Nil) {
		return Product{}, false, nil
	}
	if err != nil {
		return Product{}, false, fmt.Errorf("cache get %d: %w", id, err)
	}
	var p Product
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return Product{}, false, fmt.Errorf("cache get %d: malformed cached value: %w", id, err)
	}
	return p, true, nil
}

func (c *ProductCache) Set(ctx context.Context, p Product) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("cache set %d: encode: %w", p.ID, err)
	}
	if err := c.client.Set(ctx, productKey(p.ID), raw, c.ttl).Err(); err != nil {
		return fmt.Errorf("cache set %d: %w", p.ID, err)
	}
	return nil
}

func (c *ProductCache) Invalidate(ctx context.Context, id int64) error {
	if err := c.client.Del(ctx, productKey(id)).Err(); err != nil {
		return fmt.Errorf("cache invalidate %d: %w", id, err)
	}
	return nil
}
