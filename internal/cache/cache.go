package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Cache interface {
	Set(key string, value []byte, ttl time.Duration) error
	SetNX(key string, value []byte, ttl time.Duration) bool
	SetXX(key string, value []byte, ttl time.Duration) bool
	Get(key string) ([]byte, error)
	Delete(key string) (bool, error)
	Exists(key string) (bool, error)
	TTL(key string) (time.Duration, error)
	Expire(key string, ttl time.Duration) error
	Persist(key string) error
	Keys() ([]string, error)
	Size() int
	Flush()
	Ping() string
}

type Options struct {
	EvictionPolicy       EvictionPolicy
	EvictionCapacity     int
	ActiveExpiration     bool
	ExpirationInterval   time.Duration
	ExpirationSampleSize int
}

func DefaultOptions() *Options {
	return &Options{
		EvictionPolicy:       EvictionLRU,
		EvictionCapacity:     100000,
		ActiveExpiration:     true,
		ExpirationInterval:   time.Second,
		ExpirationSampleSize: 10,
	}
}

type LocalCache struct {
	store    *Store
	opts     *Options
	stopCh   chan struct{}
	wg       sync.WaitGroup
	keysRead atomic.Int64
	keysMiss atomic.Int64
}

func New(opts *Options) *LocalCache {
	if opts == nil {
		opts = DefaultOptions()
	}
	c := &LocalCache{
		store:  NewStore(),
		opts:   opts,
		stopCh: make(chan struct{}),
	}
	if opts.ActiveExpiration {
		c.wg.Add(1)
		go c.activeExpirationLoop()
	}
	return c
}

// Set stores a key-value pair. A ttl of 0 means no expiry — the key persists
// until explicitly deleted. This differs from Expire(k, 0), which sets an
// immediately-expiring TTL (matching Redis semantics for both cases).
func (c *LocalCache) Set(key string, value []byte, ttl time.Duration) error {
	entry := NewEntry(key, value, ttl)
	c.store.Set(entry)
	return nil
}

func (c *LocalCache) SetNX(key string, value []byte, ttl time.Duration) bool {
	entry := NewEntry(key, value, ttl)
	return c.store.SetNX(entry)
}

func (c *LocalCache) SetXX(key string, value []byte, ttl time.Duration) bool {
	entry := NewEntry(key, value, ttl)
	return c.store.SetXX(entry)
}

func (c *LocalCache) Get(key string) ([]byte, error) {
	entry, ok := c.store.Get(key)
	if !ok {
		c.keysMiss.Add(1)
		return nil, fmt.Errorf("key not found")
	}
	if entry.IsExpired() {
		c.store.Delete(key)
		c.keysMiss.Add(1)
		return nil, fmt.Errorf("key expired")
	}
	c.keysRead.Add(1)
	return entry.Value, nil
}

func (c *LocalCache) Delete(key string) (bool, error) {
	deleted := c.store.Delete(key)
	return deleted, nil
}

func (c *LocalCache) Exists(key string) (bool, error) {
	entry, ok := c.store.Get(key)
	if !ok {
		return false, nil
	}
	if entry.IsExpired() {
		c.store.Delete(key)
		return false, nil
	}
	return true, nil
}

func (c *LocalCache) TTL(key string) (time.Duration, error) {
	entry, ok := c.store.Get(key)
	if !ok {
		return -1, fmt.Errorf("key not found")
	}
	if entry.IsExpired() {
		c.store.Delete(key)
		return -1, fmt.Errorf("key expired")
	}
	return entry.TTL(), nil
}

// Expire sets a new TTL on an existing key. A ttl of 0 means "expire
// immediately" (the key will be gone on next access) — this differs from
// Set(k, v, 0), where 0 means "no expiry."
//
// If the key is already expired (but not yet reaped), Expire treats it as
// non-existent and returns an error rather than resuscitating it. This
// matches Redis's passive-expiry-then-check behavior.
func (c *LocalCache) Expire(key string, ttl time.Duration) error {
	entry, ok := c.store.Get(key)
	if !ok {
		return fmt.Errorf("key not found")
	}
	if entry.IsExpired() {
		c.store.Delete(key)
		return fmt.Errorf("key not found")
	}
	entry.ExpiresAt.Store(time.Now().Add(ttl).UnixNano())
	return nil
}

func (c *LocalCache) Persist(key string) error {
	entry, ok := c.store.Get(key)
	if !ok {
		return fmt.Errorf("key not found")
	}
	entry.ExpiresAt.Store(0)
	return nil
}

func (c *LocalCache) Keys() ([]string, error) {
	return c.store.Keys(), nil
}

func (c *LocalCache) Size() int {
	return c.store.Size()
}

func (c *LocalCache) Flush() {
	c.store.Flush()
}

func (c *LocalCache) Ping() string {
	return "PONG"
}

// SnapshotItem is a plain-data representation of a cache entry suitable
// for persistence. It holds the raw field values without the atomic
// wrappers or mutable state of an in-memory *Entry.
type SnapshotItem struct {
	Key       string
	Value     []byte
	ExpiresAt int64
	CreatedAt int64
}

// Snapshot returns a point-in-time consistent snapshot of all entries.
// The returned map is safe to iterate after the single RLock held by
// Store.Snapshot() is released.
func (c *LocalCache) Snapshot() map[string]SnapshotItem {
	raw := c.store.Snapshot()
	snap := make(map[string]SnapshotItem, len(raw))
	for k, e := range raw {
		snap[k] = SnapshotItem{
			Key:       e.Key,
			Value:     e.Value,
			ExpiresAt: e.ExpiresAt.Load(),
			CreatedAt: e.CreatedAt,
		}
	}
	return snap
}

// BulkLoad inserts entries directly into the store, preserving their
// original CreatedAt and ExpiresAt timestamps. Used during recovery to
// restore state without resetting metadata. Entries that are already
// expired (ExpiresAt > 0 && ExpiresAt < now) are skipped — they would
// be deleted on first access anyway.
func (c *LocalCache) BulkLoad(entries map[string]*Entry) int {
	loaded := 0
	now := time.Now().UnixNano()
	for _, e := range entries {
		expiresAt := e.ExpiresAt.Load()
		if expiresAt > 0 && expiresAt < now {
			continue
		}
		c.store.Set(e)
		loaded++
	}
	return loaded
}

func (c *LocalCache) HitRate() float64 {
	total := c.keysRead.Load() + c.keysMiss.Load()
	if total == 0 {
		return 0
	}
	return float64(c.keysRead.Load()) / float64(total)
}

func (c *LocalCache) Stats() CacheStats {
	return CacheStats{
		Keys:    c.Size(),
		Hits:    c.keysRead.Load(),
		Misses:  c.keysMiss.Load(),
		HitRate: c.HitRate(),
	}
}

type CacheStats struct {
	Keys    int
	Hits    int64
	Misses  int64
	HitRate float64
}

func (c *LocalCache) activeExpirationLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.opts.ExpirationInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.evictExpired()
		}
	}
}

func (c *LocalCache) evictExpired() {
	sampled := 0
	var expired []string
	c.store.Range(func(key string, entry *Entry) bool {
		if sampled >= c.opts.ExpirationSampleSize {
			return false
		}
		sampled++
		if entry.IsExpired() {
			expired = append(expired, key)
		}
		return true
	})
	for _, key := range expired {
		c.store.Delete(key)
	}
}

func (c *LocalCache) Shutdown() {
	close(c.stopCh)
	c.wg.Wait()
}
