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
	if entry.ExpiresAt.Load() == 0 {
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

// --- Fix 1: race test for ExpiresAt atomic access ---

func TestExpirePersistRace(t *testing.T) {
	c := New(&Options{
		ActiveExpiration:     false,
		ExpirationSampleSize: 10,
	})
	defer c.Shutdown()

	c.Set("racekey", []byte("val"), 0)

	var wg sync.WaitGroup
	const goroutines = 50
	const iterations = 200

	// Writers: alternate between Expire and Persist
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if j%2 == 0 {
					_ = c.Expire("racekey", 10*time.Second)
				} else {
					_ = c.Persist("racekey")
				}
			}
		}(i)
	}

	// Readers: hammer Get/IsExpired/TTL concurrently
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, _ = c.Get("racekey")
				_, _ = c.Exists("racekey")
				_, _ = c.TTL("racekey")
			}
		}(i)
	}

	wg.Wait()
}

// --- Fix 3: Expire on an already-expired key must not resuscitate ---

func TestExpireDoesNotResuscitateExpiredKey(t *testing.T) {
	c := New(&Options{
		ActiveExpiration:     false,
		ExpirationSampleSize: 10,
	})
	defer c.Shutdown()

	c.Set("shortlived", []byte("gone"), 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	err := c.Expire("shortlived", 10*time.Second)
	if err == nil {
		t.Fatal("Expire on expired key should return an error")
	}

	exists, _ := c.Exists("shortlived")
	if exists {
		t.Fatal("expired key must not be resuscitated by Expire")
	}
}

// --- Shutdown correctness: activeExpirationLoop exits after Shutdown ---

func TestActiveExpirationShutdown(t *testing.T) {
	c := New(&Options{
		ActiveExpiration:     true,
		ExpirationInterval:   time.Millisecond,
		ExpirationSampleSize: 100,
	})

	// Stuff many short-lived keys in so the sweeper has work to do.
	for i := 0; i < 500; i++ {
		c.Set(fmt.Sprintf("k%d", i), []byte("v"), 1*time.Millisecond)
	}

	time.Sleep(20 * time.Millisecond)

	sizeBefore := c.Size()
	c.Shutdown()

	// After Shutdown returns the goroutine must have exited.
	// Verify by checking the size is stable — no more sweeps.
	time.Sleep(50 * time.Millisecond)
	if c.Size() != sizeBefore {
		t.Errorf("cache size changed after Shutdown: before=%d after=%d", sizeBefore, c.Size())
	}
}

// --- Active vs lazy agreement ---

func TestActiveVsLazyExpirationAgree(t *testing.T) {
	// Test lazy path: Get triggers expiration.
	cLazy := New(&Options{
		ActiveExpiration:     false,
		ExpirationSampleSize: 10,
	})
	cLazy.Set("lazy", []byte("v"), 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	// Lazy: Get deletes the expired key.
	_, errGet := cLazy.Get("lazy")
	lazyExists, _ := cLazy.Exists("lazy")
	lazyTTL, _ := cLazy.TTL("lazy")
	lazyKeys, _ := cLazy.Keys()
	lazySize := cLazy.Size()

	cLazy.Shutdown()

	// Test active path: sweeper deletes the expired key.
	cActive := New(&Options{
		ActiveExpiration:     true,
		ExpirationInterval:   time.Millisecond,
		ExpirationSampleSize: 100,
	})
	cActive.Set("active", []byte("v"), 1*time.Millisecond)
	time.Sleep(50 * time.Millisecond) // let the sweeper run

	activeExists, _ := cActive.Exists("active")
	activeTTL, _ := cActive.TTL("active")
	activeKeys, _ := cActive.Keys()
	activeSize := cActive.Size()
	_, errActiveGet := cActive.Get("active")

	cActive.Shutdown()

	// Both paths must produce identical observable state.
	if (errGet != nil) != (errActiveGet != nil) {
		t.Errorf("Get error mismatch: lazy=%v active=%v", errGet, errActiveGet)
	}
	if lazyExists != activeExists {
		t.Errorf("Exists mismatch: lazy=%v active=%v", lazyExists, activeExists)
	}
	if (lazyTTL < 0) != (activeTTL < 0) {
		t.Errorf("TTL sign mismatch: lazy=%v active=%v", lazyTTL, activeTTL)
	}
	if len(lazyKeys) != len(activeKeys) {
		t.Errorf("Keys mismatch: lazy=%d active=%d", len(lazyKeys), len(activeKeys))
	}
	if lazySize != activeSize {
		t.Errorf("Size mismatch: lazy=%d active=%d", lazySize, activeSize)
	}
}

// --- Edge cases ---

func TestTTLOfZeroMeansNoExpiry(t *testing.T) {
	c := New(&Options{ActiveExpiration: false})
	defer c.Shutdown()

	c.Set("permanent", []byte("v"), 0)
	time.Sleep(5 * time.Millisecond)

	exists, _ := c.Exists("permanent")
	if !exists {
		t.Fatal("TTL=0 key must never expire")
	}

	ttl, _ := c.TTL("permanent")
	if ttl != -1 {
		t.Errorf("TTL for no-expiry key should be -1, got %v", ttl)
	}
}

func TestExpireNonexistentKey(t *testing.T) {
	c := New(&Options{ActiveExpiration: false})
	defer c.Shutdown()

	err := c.Expire("ghost", 10*time.Second)
	if err == nil {
		t.Fatal("Expire on nonexistent key must return error")
	}
}

func TestPersistOnKeyWithNoTTL(t *testing.T) {
	c := New(&Options{ActiveExpiration: false})
	defer c.Shutdown()

	c.Set("nottl", []byte("v"), 0)
	err := c.Persist("nottl")
	if err != nil {
		t.Fatalf("Persist on key with no TTL should not error, got: %v", err)
	}

	exists, _ := c.Exists("nottl")
	if !exists {
		t.Fatal("key must still exist after Persist with no TTL")
	}

	ttl, _ := c.TTL("nottl")
	if ttl != -1 {
		t.Errorf("expected -1 (no expiry) after Persist, got %v", ttl)
	}
}

func TestExpireOnAlreadyExpiredKey(t *testing.T) {
	c := New(&Options{ActiveExpiration: false})
	defer c.Shutdown()

	c.Set("dying", []byte("v"), 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	err := c.Expire("dying", 10*time.Second)
	if err == nil {
		t.Fatal("Expire on expired key should return not-found error")
	}

	exists, _ := c.Exists("dying")
	if exists {
		t.Fatal("expired key should have been cleaned up by Expire")
	}
}
