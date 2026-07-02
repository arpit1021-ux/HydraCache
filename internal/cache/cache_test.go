package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewEntry(t *testing.T) {
	entry := NewEntry("testkey", []byte("testvalue"), 5*time.Second)
	if entry.Key != "testkey" {
		t.Errorf("expected key 'testkey', got '%s'", entry.Key)
	}
	if string(entry.Value) != "testvalue" {
		t.Errorf("expected value 'testvalue', got '%s'", string(entry.Value))
	}
	if entry.ExpiresAt == 0 {
		t.Error("expected ExpiresAt to be set")
	}
}

func TestEntryIsExpired(t *testing.T) {
	entry := NewEntry("key", []byte("val"), 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	if !entry.IsExpired() {
		t.Error("expected entry to be expired after TTL")
	}

	entry2 := NewEntry("key2", []byte("val2"), 0)
	if entry2.IsExpired() {
		t.Error("entry with no TTL should not be expired")
	}
}

func TestLocalCacheSetGet(t *testing.T) {
	c := New(nil)
	defer c.Shutdown()

	err := c.Set("hello", []byte("world"), 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	val, err := c.Get("hello")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "world" {
		t.Errorf("expected 'world', got '%s'", string(val))
	}
}

func TestLocalCacheDelete(t *testing.T) {
	c := New(nil)
	defer c.Shutdown()

	c.Set("key1", []byte("val1"), 0)
	deleted, err := c.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if !deleted {
		t.Error("expected deleted to be true")
	}

	_, err = c.Get("key1")
	if err == nil {
		t.Error("expected error for deleted key")
	}
}

func TestLocalCacheExists(t *testing.T) {
	c := New(nil)
	defer c.Shutdown()

	c.Set("exists", []byte("yes"), 0)
	exists, _ := c.Exists("exists")
	if !exists {
		t.Error("expected key to exist")
	}

	exists, _ = c.Exists("nope")
	if exists {
		t.Error("expected key not to exist")
	}
}

func TestLocalCacheTTL(t *testing.T) {
	c := New(nil)
	defer c.Shutdown()

	c.Set("ttl_key", []byte("val"), 10*time.Second)
	ttl, err := c.TTL("ttl_key")
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	if ttl <= 0 {
		t.Errorf("expected positive TTL, got %v", ttl)
	}

	c.Set("no_ttl", []byte("val"), 0)
	ttl, err = c.TTL("no_ttl")
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	if ttl != -1 {
		t.Errorf("expected -1 for key without TTL, got %v", ttl)
	}
}

func TestLocalCacheExpiration(t *testing.T) {
	c := New(nil)
	defer c.Shutdown()

	c.Set("expiring", []byte("val"), 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	_, err := c.Get("expiring")
	if err == nil {
		t.Error("expected error for expired key")
	}
}

func TestLocalCacheConcurrentAccess(t *testing.T) {
	c := New(&Options{
		EvictionPolicy:       EvictionLRU,
		EvictionCapacity:     1000,
		ActiveExpiration:     false,
		ExpirationInterval:   time.Second,
		ExpirationSampleSize: 10,
	})
	defer c.Shutdown()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := fmt.Sprintf("key:%d", n)
			c.Set(key, []byte(fmt.Sprintf("value:%d", n)), 0)
			c.Get(key)
			c.Exists(key)
			c.Delete(key)
		}(i)
	}
	wg.Wait()
}

func TestLocalCacheKeys(t *testing.T) {
	c := New(nil)
	defer c.Shutdown()

	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Set("c", []byte("3"), 0)

	keys, err := c.Keys()
	if err != nil {
		t.Fatalf("Keys failed: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
}

func TestLocalCacheFlush(t *testing.T) {
	c := New(nil)
	defer c.Shutdown()

	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Flush()

	if c.Size() != 0 {
		t.Errorf("expected 0 keys after flush, got %d", c.Size())
	}
}

func TestLocalCacheStats(t *testing.T) {
	c := New(nil)
	defer c.Shutdown()

	c.Set("key", []byte("val"), 0)
	c.Get("key")
	c.Get("nonexistent")

	stats := c.Stats()
	if stats.Keys != 1 {
		t.Errorf("expected 1 key, got %d", stats.Keys)
	}
	if stats.Hits != 1 {
		t.Errorf("expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("expected 1 miss, got %d", stats.Misses)
	}
}

func TestBloomFilter(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	bf.Add("hello")
	bf.Add("world")

	if !bf.Contains("hello") {
		t.Error("expected bloom filter to contain 'hello'")
	}
	if !bf.Contains("world") {
		t.Error("expected bloom filter to contain 'world'")
	}
	if bf.Contains("missing") {
		t.Error("bloom filter false positive")
	}
}

func TestLRUEviction(t *testing.T) {
	lru := NewLRU(3)

	lru.Put(NewEntry("a", []byte("1"), 0))
	lru.Put(NewEntry("b", []byte("2"), 0))
	lru.Put(NewEntry("c", []byte("3"), 0))
	lru.Put(NewEntry("d", []byte("4"), 0))

	if _, ok := lru.Get("a"); ok {
		t.Error("expected 'a' to be evicted")
	}
	if _, ok := lru.Get("d"); !ok {
		t.Error("expected 'd' to be present")
	}
}

func TestLFUEviction(t *testing.T) {
	lfu := NewLFU(3)

	lfu.Put(NewEntry("a", []byte("1"), 0))
	lfu.Put(NewEntry("b", []byte("2"), 0))
	lfu.Put(NewEntry("c", []byte("3"), 0))

	lfu.Get("a")
	lfu.Get("a")
	lfu.Get("b")

	lfu.Put(NewEntry("d", []byte("4"), 0))

	if _, ok := lfu.Get("c"); ok {
		t.Error("expected 'c' to be evicted (least frequently used)")
	}
}
