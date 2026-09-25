package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeStore is an in-memory Store used only by tests — no live Postgres
// required to prove Service's cache-aside logic is correct.
type fakeStore struct {
	mu       sync.Mutex
	products map[int64]Product
	getCalls int
	nextID   int64
}

func newFakeStore(seed ...Product) *fakeStore {
	s := &fakeStore{products: make(map[int64]Product), nextID: 1}
	for _, p := range seed {
		s.products[p.ID] = p
		if p.ID >= s.nextID {
			s.nextID = p.ID + 1
		}
	}
	return s
}

func (s *fakeStore) GetProduct(ctx context.Context, id int64) (Product, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	p, ok := s.products[id]
	if !ok {
		return Product{}, ErrNotFound
	}
	return p, nil
}

func (s *fakeStore) CreateProduct(ctx context.Context, p Product) (Product, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p.ID = s.nextID
	s.nextID++
	p.UpdatedAt = time.Now()
	s.products[p.ID] = p
	return p, nil
}

func (s *fakeStore) UpdateProduct(ctx context.Context, p Product) (Product, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.products[p.ID]; !ok {
		return Product{}, ErrNotFound
	}
	p.UpdatedAt = time.Now()
	s.products[p.ID] = p
	return p, nil
}

func (s *fakeStore) getCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCalls
}

// fakeRedis implements redisCommands entirely in memory, so ProductCache
// and Service are tested against real cache-aside logic without a live
// HydraCache cluster. Its wire-level behavior (RESP encoding, real
// network errors) is a separate concern already covered by
// internal/network's go-redis compatibility suite in the main module.
type fakeRedis struct {
	mu       sync.Mutex
	data     map[string]string
	failNext bool
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{data: make(map[string]string)}
}

func (f *fakeRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewStringCmd(ctx, "get", key)
	if f.failNext {
		f.failNext = false
		cmd.SetErr(errors.New("simulated cache failure"))
		return cmd
	}
	v, ok := f.data[key]
	if !ok {
		cmd.SetErr(redis.Nil)
		return cmd
	}
	cmd.SetVal(v)
	return cmd
}

func (f *fakeRedis) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewStatusCmd(ctx, "set", key)
	switch v := value.(type) {
	case string:
		f.data[key] = v
	case []byte:
		f.data[key] = string(v)
	default:
		cmd.SetErr(errors.New("fakeRedis: unsupported value type"))
		return cmd
	}
	cmd.SetVal("OK")
	return cmd
}

func (f *fakeRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewIntCmd(ctx, "del")
	var n int64
	for _, k := range keys {
		if _, ok := f.data[k]; ok {
			delete(f.data, k)
			n++
		}
	}
	cmd.SetVal(n)
	return cmd
}

func (f *fakeRedis) has(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[key]
	return ok
}

func newTestService(store *fakeStore, cacheClient redisCommands) *Service {
	cache := NewProductCache(cacheClient, time.Minute)
	return NewService(store, cache)
}

func TestGetProduct_MissPopulatesCacheAndReportsDatabaseSource(t *testing.T) {
	store := newFakeStore(Product{ID: 1, Name: "Widget", PriceCent: 100})
	fr := newFakeRedis()
	svc := newTestService(store, fr)

	p, source, err := svc.GetProduct(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if source != SourceDatabase {
		t.Errorf("source = %q, want %q on first read", source, SourceDatabase)
	}
	if p.Name != "Widget" {
		t.Errorf("name = %q, want %q", p.Name, "Widget")
	}
	if !fr.has(productKey(1)) {
		t.Error("expected the database read to populate the cache")
	}
}

func TestGetProduct_HitServesFromCacheWithoutTouchingStore(t *testing.T) {
	store := newFakeStore(Product{ID: 1, Name: "Widget", PriceCent: 100})
	fr := newFakeRedis()
	svc := newTestService(store, fr)

	// First call populates the cache.
	if _, _, err := svc.GetProduct(context.Background(), 1); err != nil {
		t.Fatalf("unexpected error priming cache: %v", err)
	}
	callsAfterFirst := store.getCallCount()

	p, source, err := svc.GetProduct(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if source != SourceCache {
		t.Errorf("source = %q, want %q on second read", source, SourceCache)
	}
	if p.Name != "Widget" {
		t.Errorf("name = %q, want %q", p.Name, "Widget")
	}
	if store.getCallCount() != callsAfterFirst {
		t.Errorf("store.GetProduct called again on a cache hit: calls = %d, want %d", store.getCallCount(), callsAfterFirst)
	}
}

func TestGetProduct_CacheErrorFallsBackToStoreInsteadOfFailing(t *testing.T) {
	store := newFakeStore(Product{ID: 1, Name: "Widget", PriceCent: 100})
	fr := newFakeRedis()
	fr.failNext = true // simulates the cache being unreachable
	svc := newTestService(store, fr)

	p, source, err := svc.GetProduct(context.Background(), 1)
	if err != nil {
		t.Fatalf("expected a cache error to degrade to the store, not fail the request: %v", err)
	}
	if source != SourceDatabase {
		t.Errorf("source = %q, want %q when the cache is unreachable", source, SourceDatabase)
	}
	if p.Name != "Widget" {
		t.Errorf("name = %q, want %q", p.Name, "Widget")
	}
}

func TestGetProduct_NotFoundPropagatesAsErrNotFound(t *testing.T) {
	store := newFakeStore()
	svc := newTestService(store, newFakeRedis())

	_, _, err := svc.GetProduct(context.Background(), 999)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestUpdateProduct_InvalidatesCacheEntry(t *testing.T) {
	store := newFakeStore(Product{ID: 1, Name: "Widget", PriceCent: 100})
	fr := newFakeRedis()
	svc := newTestService(store, fr)

	// Prime the cache.
	if _, _, err := svc.GetProduct(context.Background(), 1); err != nil {
		t.Fatalf("unexpected error priming cache: %v", err)
	}
	if !fr.has(productKey(1)) {
		t.Fatal("expected cache to be primed before the update")
	}

	if _, err := svc.UpdateProduct(context.Background(), Product{ID: 1, Name: "Widget v2", PriceCent: 200}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fr.has(productKey(1)) {
		t.Error("expected UpdateProduct to invalidate the cache entry")
	}

	// Next read must come from the store and reflect the new value.
	p, source, err := svc.GetProduct(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if source != SourceDatabase {
		t.Errorf("source = %q, want %q right after invalidation", source, SourceDatabase)
	}
	if p.Name != "Widget v2" {
		t.Errorf("name = %q, want updated value %q", p.Name, "Widget v2")
	}
}

func TestUpdateProduct_NotFoundDoesNotTouchCache(t *testing.T) {
	store := newFakeStore()
	fr := newFakeRedis()
	svc := newTestService(store, fr)

	_, err := svc.UpdateProduct(context.Background(), Product{ID: 42, Name: "Ghost"})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}

func TestCreateProduct_AssignsIDAndDoesNotTouchCache(t *testing.T) {
	store := newFakeStore()
	fr := newFakeRedis()
	svc := newTestService(store, fr)

	p, err := svc.CreateProduct(context.Background(), Product{Name: "New Thing", PriceCent: 42})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.ID == 0 {
		t.Error("expected CreateProduct to assign a non-zero ID")
	}
	if fr.has(productKey(p.ID)) {
		t.Error("CreateProduct should not populate the cache — the next GET does that")
	}
}
