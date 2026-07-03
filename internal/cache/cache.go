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

func (c *LocalCache) Expire(key string, ttl time.Duration) error {
	entry, ok := c.store.Get(key)
	if !ok {
		return fmt.Errorf("key not found")
	}
	entry.ExpiresAt = time.Now().Add(ttl).UnixNano()
	return nil
}

func (c *LocalCache) Persist(key string) error {
	entry, ok := c.store.Get(key)
	if !ok {
		return fmt.Errorf("key not found")
	}
	entry.ExpiresAt = 0
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
	c.store.Range(func(key string, entry *Entry) bool {
		if sampled >= c.opts.ExpirationSampleSize {
			return false
		}
		sampled++
		if entry.IsExpired() {
			c.store.Delete(key)
		}
		return true
	})
}

func (c *LocalCache) Shutdown() {
	close(c.stopCh)
	c.wg.Wait()
}
