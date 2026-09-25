package main

import (
	"context"
	"fmt"
)

// Source reports where a Service.GetProduct answer came from — the whole
// point of this demo is making that visible, not just the final value.
type Source string

const (
	SourceCache    Source = "cache"
	SourceDatabase Source = "database"
)

// Service implements the cache-aside (read-through) pattern: reads check
// the cache first and fall back to the store on a miss, populating the
// cache on the way out; writes go to the store and then invalidate the
// cache entry rather than trying to keep it in sync in place — simpler,
// and correct as long as the next read repopulates it.
type Service struct {
	store Store
	cache *ProductCache
}

func NewService(store Store, cache *ProductCache) *Service {
	return &Service{store: store, cache: cache}
}

// GetProduct is the read path this whole demo exists to show off. A cache
// error (the cluster being down, mid-failover, or otherwise unreachable)
// is not treated as a cache miss — it degrades to reading straight from
// the store rather than serving stale data or failing the request, which
// is what "the cache is allowed to fail without taking the app down with
// it" actually means in a cache-aside design.
func (s *Service) GetProduct(ctx context.Context, id int64) (Product, Source, error) {
	if s.cache != nil {
		if p, ok, err := s.cache.Get(ctx, id); err == nil && ok {
			return p, SourceCache, nil
		}
		// Cache miss or cache error both fall through to the store below.
	}

	p, err := s.store.GetProduct(ctx, id)
	if err != nil {
		return Product{}, "", err
	}

	if s.cache != nil {
		// Best-effort: a failed cache populate must not fail the read that
		// already succeeded against the store.
		_ = s.cache.Set(ctx, p)
	}
	return p, SourceDatabase, nil
}

func (s *Service) CreateProduct(ctx context.Context, p Product) (Product, error) {
	created, err := s.store.CreateProduct(ctx, p)
	if err != nil {
		return Product{}, fmt.Errorf("create product: %w", err)
	}
	return created, nil
}

// UpdateProduct writes to the store, then invalidates rather than
// updates the cache entry — the next GetProduct repopulates it from the
// now-current row, which is simpler than keeping two copies in sync and
// has no correctness gap as long as invalidation happens after the write
// commits (it does: UpdateProduct returns only after the store confirms).
func (s *Service) UpdateProduct(ctx context.Context, p Product) (Product, error) {
	updated, err := s.store.UpdateProduct(ctx, p)
	if err != nil {
		return Product{}, fmt.Errorf("update product: %w", err)
	}
	if s.cache != nil {
		// Best-effort for the same reason as in GetProduct: the write to
		// the store already succeeded and must be reported as such even
		// if the cache is unreachable.
		_ = s.cache.Invalidate(ctx, updated.ID)
	}
	return updated, nil
}
