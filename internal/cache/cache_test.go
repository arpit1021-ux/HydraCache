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

	c.Shutdown()

	// After Shutdown returns, the sweep goroutine must have exited —
	// verify by checking the size is stable afterward (no further
	// sweeps happening in the background). Capturing the "before" size
	// AFTER Shutdown returns, not before calling it, is what actually
	// isolates "the sweep goroutine stopped" from "the sweeper was
	// still legitimately running" — capturing it earlier races against
	// the sweeper's own normal, still-in-progress work between that
	// snapshot and the Shutdown() call, which made this test flake
	// under repeated runs even with correct shutdown behavior.
	sizeAfterShutdown := c.Size()
	time.Sleep(50 * time.Millisecond)
	if c.Size() != sizeAfterShutdown {
		t.Errorf("cache size changed after Shutdown: before=%d after=%d", sizeAfterShutdown, c.Size())
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

// --- Store.Snapshot() race test ---

func TestStoreSnapshotRace(t *testing.T) {
	s := NewStore()
	const goroutines = 50
	const iterations = 200

	var wg sync.WaitGroup

	// Writers: hammer Set and Delete concurrently.
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("k%d", j%20)
				e := NewEntry(key, []byte(fmt.Sprintf("v%d_%d", n, j)), 0)
				if j%3 == 0 {
					s.Delete(key)
				} else {
					s.Set(e)
				}
			}
		}(i)
	}

	// Readers: call Snapshot() concurrently.
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				snap := s.Snapshot()
				// Verify the snapshot is internally consistent: no key
				// should have a nil value.
				for k, v := range snap {
					if v == nil {
						t.Errorf("Snapshot returned nil value for key %q", k)
					}
				}
			}
		}()
	}

	wg.Wait()
}

// --- LocalCache.Snapshot() test ---

func TestLocalCacheSnapshot(t *testing.T) {
	c := New(&Options{ActiveExpiration: false})
	defer c.Shutdown()

	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 5*time.Second)
	c.Set("c", []byte("3"), 0)

	snap := c.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 entries in snapshot, got %d", len(snap))
	}

	// "a" has no TTL — ExpiresAt should be 0
	if e, ok := snap["a"]; !ok {
		t.Fatal("key 'a' missing")
	} else if e.ExpiresAt != 0 {
		t.Errorf("key 'a' ExpiresAt should be 0, got %d", e.ExpiresAt)
	}

	// "b" has TTL — ExpiresAt should be a future timestamp
	if e, ok := snap["b"]; !ok {
		t.Fatal("key 'b' missing")
	} else if e.ExpiresAt <= time.Now().UnixNano() {
		t.Errorf("key 'b' ExpiresAt should be in the future, got %d", e.ExpiresAt)
	}

	// Values should match
	if string(snap["a"].Value) != "1" {
		t.Errorf("expected value '1', got %q", snap["a"].Value)
	}
}

// --- BulkLoad test ---

func TestBulkLoad(t *testing.T) {
	c := New(&Options{ActiveExpiration: false})
	defer c.Shutdown()

	now := time.Now().UnixNano()
	entries := map[string]*Entry{
		"live":  NewEntryWithTTL("live", []byte("yes"), now+int64(time.Hour), now),
		"stale": NewEntryWithTTL("stale", []byte("no"), now-int64(time.Minute), now),
		"perm":  NewEntryWithTTL("perm", []byte("forever"), 0, now),
	}

	loaded := c.BulkLoad(entries)
	if loaded != 2 {
		t.Errorf("expected 2 loaded (stale should be skipped), got %d", loaded)
	}

	val, err := c.Get("live")
	if err != nil || string(val) != "yes" {
		t.Errorf("live key: val=%q err=%v", val, err)
	}

	val, err = c.Get("perm")
	if err != nil || string(val) != "forever" {
		t.Errorf("perm key: val=%q err=%v", val, err)
	}

	_, err = c.Get("stale")
	if err == nil {
		t.Error("stale key should have been skipped by BulkLoad")
	}
}

func TestCompareAndDelete_DeletesOnMatch(t *testing.T) {
	c := New(nil)
	c.Set("k", []byte("v1"), 0)

	deleted, err := c.CompareAndDelete("k", []byte("v1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !deleted {
		t.Error("expected CompareAndDelete to succeed when the value matches")
	}
	if _, err := c.Get("k"); err == nil {
		t.Error("key should be gone after a matching CompareAndDelete")
	}
}

func TestCompareAndDelete_RefusesOnMismatch(t *testing.T) {
	c := New(nil)
	c.Set("k", []byte("v1"), 0)
	c.Set("k", []byte("v2"), 0) // a "concurrent write" landed after the caller read v1

	deleted, err := c.CompareAndDelete("k", []byte("v1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted {
		t.Error("expected CompareAndDelete to refuse when the current value no longer matches")
	}

	val, err := c.Get("k")
	if err != nil {
		t.Fatalf("key should still exist: %v", err)
	}
	if string(val) != "v2" {
		t.Errorf("value = %q, want the surviving concurrent write v2", val)
	}
}

func TestCompareAndDelete_MissingKey(t *testing.T) {
	c := New(nil)
	deleted, err := c.CompareAndDelete("ghost", []byte("anything"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted {
		t.Error("expected CompareAndDelete on a missing key to report false")
	}
}

func TestCompareAndDelete_ConcurrentWritersOnlyOneSurvivesUnaffected(t *testing.T) {
	c := New(nil)
	c.Set("k", []byte("original"), 0)

	const attempts = 50
	var wg sync.WaitGroup
	successes := make(chan bool, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _ := c.CompareAndDelete("k", []byte("original"))
			successes <- ok
		}()
	}
	wg.Wait()
	close(successes)

	trueCount := 0
	for ok := range successes {
		if ok {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Errorf("expected exactly 1 of %d concurrent CompareAndDelete calls to succeed, got %d", attempts, trueCount)
	}
}

// --- Memory-bound enforcement: the audit's headline finding was that
// EvictionCapacity/EvictionPolicy were configured but never actually
// enforced anywhere — Store.Set inserted unconditionally, so the cache
// grew without bound regardless of configuration. These tests prove
// that's no longer true, for both a real byte budget and entry count.

func noExpiryOpts(policy EvictionPolicy, capacity int, maxBytes int64) *Options {
	return &Options{
		EvictionPolicy:   policy,
		EvictionCapacity: capacity,
		MaxMemoryBytes:   maxBytes,
		ActiveExpiration: false,
	}
}

func TestLocalCache_MaxMemoryBytesEvictsUnderPressure(t *testing.T) {
	// Each entry is ~(1+100+64) bytes; budget for ~5 of them.
	c := New(noExpiryOpts(EvictionLRU, 0, 5*(1+100+entryOverheadBytes)))

	val := make([]byte, 100)
	for i := 0; i < 50; i++ {
		c.Set(fmt.Sprintf("%d", i), val, 0)
	}

	if got := c.MemoryBytes(); got > 5*(1+100+entryOverheadBytes) {
		t.Errorf("MemoryBytes() = %d, expected to stay under the configured budget", got)
	}
	if c.Size() >= 50 {
		t.Errorf("Size() = %d, expected most of 50 writes to have been evicted", c.Size())
	}
	if c.EvictionsCount() == 0 {
		t.Error("expected EvictionsCount() > 0 once the memory budget was exceeded")
	}
}

func TestLocalCache_EvictionCapacityEnforced(t *testing.T) {
	c := New(noExpiryOpts(EvictionLRU, 5, 0))

	for i := 0; i < 20; i++ {
		c.Set(fmt.Sprintf("%d", i), []byte("v"), 0)
	}

	if c.Size() != 5 {
		t.Errorf("Size() = %d, want exactly 5 (EvictionCapacity)", c.Size())
	}
	if c.EvictionsCount() != 15 {
		t.Errorf("EvictionsCount() = %d, want 15", c.EvictionsCount())
	}
}

func TestLocalCache_LRUEvictsLeastRecentlyUsedFirst(t *testing.T) {
	c := New(noExpiryOpts(EvictionLRU, 3, 0))

	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Set("c", []byte("3"), 0)

	// Touch "a" so it's no longer the least-recently-used.
	c.Get("a")

	c.Set("d", []byte("4"), 0) // must evict "b", the true LRU victim

	if _, err := c.Get("a"); err != nil {
		t.Error("expected 'a' to survive (recently touched)")
	}
	if _, err := c.Get("b"); err == nil {
		t.Error("expected 'b' to be evicted (least recently used)")
	}
	if _, err := c.Get("c"); err != nil {
		t.Error("expected 'c' to survive")
	}
	if _, err := c.Get("d"); err != nil {
		t.Error("expected 'd' to survive (just inserted)")
	}
}

func TestLocalCache_LFUEvictsLeastFrequentlyUsedFirst(t *testing.T) {
	c := New(noExpiryOpts(EvictionLFU, 3, 0))

	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Set("c", []byte("3"), 0)

	c.Get("a")
	c.Get("a")
	c.Get("c")

	c.Set("d", []byte("4"), 0) // "b" has the lowest access frequency

	if _, err := c.Get("b"); err == nil {
		t.Error("expected 'b' to be evicted (least frequently used)")
	}
	if _, err := c.Get("a"); err != nil {
		t.Error("expected 'a' to survive (accessed twice)")
	}
	if _, err := c.Get("d"); err != nil {
		t.Error("expected 'd' to survive (just inserted)")
	}
}

func TestLocalCache_NoLimitsConfiguredNeverEvicts(t *testing.T) {
	c := New(noExpiryOpts(EvictionLRU, 0, 0))

	for i := 0; i < 1000; i++ {
		c.Set(fmt.Sprintf("%d", i), []byte("v"), 0)
	}

	if c.Size() != 1000 {
		t.Errorf("Size() = %d, want 1000 (no bound configured, nothing should be evicted)", c.Size())
	}
	if c.EvictionsCount() != 0 {
		t.Errorf("EvictionsCount() = %d, want 0", c.EvictionsCount())
	}
}

func TestLocalCache_FlushResetsMemoryAndEvictionTracking(t *testing.T) {
	c := New(noExpiryOpts(EvictionLRU, 100, 0))
	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprintf("%d", i), []byte("v"), 0)
	}
	if c.MemoryBytes() == 0 {
		t.Fatal("expected nonzero MemoryBytes before Flush")
	}

	c.Flush()

	if got := c.MemoryBytes(); got != 0 {
		t.Errorf("MemoryBytes() after Flush = %d, want 0", got)
	}
	// The tracker must also be reset — writing fresh entries afterward
	// should track and evict correctly, not carry stale bookkeeping from
	// keys that no longer exist in the store.
	for i := 0; i < 5; i++ {
		c.Set(fmt.Sprintf("new-%d", i), []byte("v"), 0)
	}
	if c.Size() != 5 {
		t.Errorf("Size() after post-flush writes = %d, want 5", c.Size())
	}
}

func TestLocalCache_DeleteUntracksKey(t *testing.T) {
	c := New(noExpiryOpts(EvictionLRU, 2, 0))
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	c.Delete("a")
	c.Set("c", []byte("3"), 0)

	// Capacity is 2; after deleting "a" and adding "c", only "b" and "c"
	// should remain — "a" being deleted must not leave a phantom
	// eviction-tracker entry that later gets evicted instead of a real
	// key, nor should it inflate the count toward evicting "b" or "c"
	// unnecessarily.
	if c.Size() != 2 {
		t.Fatalf("Size() = %d, want 2", c.Size())
	}
	if _, err := c.Get("b"); err != nil {
		t.Error("expected 'b' to survive")
	}
	if _, err := c.Get("c"); err != nil {
		t.Error("expected 'c' to survive")
	}
}

func TestStore_MemoryBytesTracksAllMutations(t *testing.T) {
	s := NewStore()
	if s.MemoryBytes() != 0 {
		t.Fatal("expected 0 bytes for an empty store")
	}

	e1 := NewEntry("k1", []byte("hello"), 0) // size = 2+5+64 = 71
	s.Set(e1)
	if got, want := s.MemoryBytes(), e1.Size; got != want {
		t.Errorf("after Set: MemoryBytes() = %d, want %d", got, want)
	}

	e1b := NewEntry("k1", []byte("hello world"), 0) // overwrite, longer value
	s.Set(e1b)
	if got, want := s.MemoryBytes(), e1b.Size; got != want {
		t.Errorf("after overwriting Set: MemoryBytes() = %d, want %d", got, want)
	}

	e2 := NewEntry("k2", []byte("v"), 0)
	if !s.SetNX(e2) {
		t.Fatal("SetNX on new key should succeed")
	}
	if got, want := s.MemoryBytes(), e1b.Size+e2.Size; got != want {
		t.Errorf("after SetNX: MemoryBytes() = %d, want %d", got, want)
	}

	// SetNX on an existing key is a no-op and must not double-count.
	s.SetNX(NewEntry("k2", []byte("should-not-apply"), 0))
	if got, want := s.MemoryBytes(), e1b.Size+e2.Size; got != want {
		t.Errorf("after no-op SetNX: MemoryBytes() = %d, want %d", got, want)
	}

	e2x := NewEntry("k2", []byte("replaced-via-xx"), 0)
	if !s.SetXX(e2x) {
		t.Fatal("SetXX on existing key should succeed")
	}
	if got, want := s.MemoryBytes(), e1b.Size+e2x.Size; got != want {
		t.Errorf("after SetXX: MemoryBytes() = %d, want %d", got, want)
	}

	s.Delete("k1")
	if got, want := s.MemoryBytes(), e2x.Size; got != want {
		t.Errorf("after Delete: MemoryBytes() = %d, want %d", got, want)
	}

	s.CompareAndDelete("k2", e2x.Value)
	if got := s.MemoryBytes(); got != 0 {
		t.Errorf("after CompareAndDelete: MemoryBytes() = %d, want 0", got)
	}

	s.Set(NewEntry("k3", []byte("v"), 0))
	s.Flush()
	if got := s.MemoryBytes(); got != 0 {
		t.Errorf("after Flush: MemoryBytes() = %d, want 0", got)
	}
}
