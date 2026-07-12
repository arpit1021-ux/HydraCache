package cluster

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hydracache/hydracache/internal/hashring"
	"github.com/hydracache/hydracache/internal/network"
)

func TestCollectAffectedKeys_RealKeys(t *testing.T) {
	c := newTestCache()
	self := NewNode("self", "127.0.0.1:7000")
	topo := NewTopology()
	topo.AddNode(self)
	ring := hashring.New(150)
	ring.AddNode("self")

	// Seed 200 real keys into the local cache.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("user:%d", i)
		if err := c.Set(key, []byte(fmt.Sprintf("val%d", i)), 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	mgr := NewManager(self, topo, ring, c)

	// Add a new node to the ring — this changes which keys hash where.
	ring.AddNode("newnode")

	affected := mgr.collectAffectedKeys("newnode")

	// 1. Every returned key must actually hash to newnode.
	for _, key := range affected {
		if ring.GetNode(key) != "newnode" {
			t.Errorf("key %q returned as affected but hashes to %s", key, ring.GetNode(key))
		}
	}

	// 2. Every real key that hashes to newnode must appear.
	allKeys, _ := c.Keys()
	var expected int
	for _, key := range allKeys {
		if ring.GetNode(key) == "newnode" {
			expected++
		}
	}
	if len(affected) != expected {
		t.Errorf("collectAffectedKeys returned %d keys, expected %d", len(affected), expected)
	}

	// 3. No synthetic __rebalance: probe keys.
	for _, key := range affected {
		if len(key) > 12 && key[:12] == "__rebalance:" {
			t.Errorf("found synthetic probe key %q", key)
		}
	}
}

func TestMigrateKey_RealMigration(t *testing.T) {
	// --- Set up source node ---
	srcCache := newTestCache()
	srcTopo := NewTopology()
	srcNode := NewNode("src", "127.0.0.1:0")
	srcTopo.AddNode(srcNode)
	srcRing := hashring.New(150)
	srcRing.AddNode("src")
	srcRing.AddNode("dst")

	// --- Set up target server with its own cache ---
	dstCache := newTestCache()
	dstSrv := network.NewServer(network.ServerConfig{Addr: "127.0.0.1:0"}, dstCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := dstSrv.Start(ctx); err != nil {
		t.Fatalf("target server start: %v", err)
	}
	t.Cleanup(func() { dstSrv.Shutdown() })

	dstAddr := dstSrv.Addr().String()
	dstNode := NewNode("dst", dstAddr)
	srcTopo.AddNode(dstNode)

	srcMgr := NewManager(srcNode, srcTopo, srcRing, srcCache)

	// --- Seed keys with and without TTL ---
	srcCache.Set("plain", []byte("hello"), 0)
	srcCache.Set("ttlkey", []byte("world"), 30*time.Second)

	// --- Migrate "plain" (no expiry) ---
	client := network.NewClient(dstAddr)
	if err := client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	if err := srcMgr.migrateSingleKey("plain", client); err != nil {
		t.Fatalf("migrateSingleKey plain: %v", err)
	}

	// Target has the value.
	val, err := dstCache.Get("plain")
	if err != nil {
		t.Fatalf("target should have 'plain': %v", err)
	}
	if string(val) != "hello" {
		t.Errorf("target value = %q, want hello", string(val))
	}
	// Target TTL should be -1 (no expiry).
	ttl, err := dstCache.TTL("plain")
	if err != nil {
		t.Fatalf("target TTL: %v", err)
	}
	if ttl != -1 {
		t.Errorf("target TTL = %v, want -1 (no expiry)", ttl)
	}
	// Source no longer has the key.
	if _, err := srcCache.Get("plain"); err == nil {
		t.Error("source should not have 'plain' after migration")
	}

	// --- Migrate "ttlkey" (with TTL) ---
	if err := srcMgr.migrateSingleKey("ttlkey", client); err != nil {
		t.Fatalf("migrateSingleKey ttlkey: %v", err)
	}

	val, err = dstCache.Get("ttlkey")
	if err != nil {
		t.Fatalf("target should have 'ttlkey': %v", err)
	}
	if string(val) != "world" {
		t.Errorf("target value = %q, want world", string(val))
	}
	ttl, err = dstCache.TTL("ttlkey")
	if err != nil {
		t.Fatalf("target TTL: %v", err)
	}
	if ttl < 20*time.Second || ttl > 31*time.Second {
		t.Errorf("target TTL = %v, want ~30s remaining", ttl)
	}
	if _, err := srcCache.Get("ttlkey"); err == nil {
		t.Error("source should not have 'ttlkey' after migration")
	}
}

func TestMigrateKey_TargetUnreachable(t *testing.T) {
	srcCache := newTestCache()
	srcTopo := NewTopology()
	srcNode := NewNode("src", "127.0.0.1:0")
	srcTopo.AddNode(srcNode)
	srcRing := hashring.New(150)
	srcRing.AddNode("src")
	srcRing.AddNode("dst")

	// Target address points to a port that nothing listens on.
	dstNode := NewNode("dst", "127.0.0.1:19876")
	srcTopo.AddNode(dstNode)

	srcMgr := NewManager(srcNode, srcTopo, srcRing, srcCache)
	srcCache.Set("key1", []byte("val1"), 0)

	// migrateKeys should fail to connect and return (0, error).
	migrated, err := srcMgr.migrateKeys([]string{"key1"}, "dst")
	if err == nil {
		t.Error("expected error when target is unreachable")
	}
	if migrated != 0 {
		t.Errorf("migrated = %d, want 0", migrated)
	}

	// Source still has the key.
	val, err := srcCache.Get("key1")
	if err != nil {
		t.Fatalf("source should still have key1: %v", err)
	}
	if string(val) != "val1" {
		t.Errorf("source value = %q, want val1", string(val))
	}
}

func TestSingleConnectionPerRebalance(t *testing.T) {
	srcCache := newTestCache()
	dstCache := newTestCache()

	// Seed 50 keys that will hash to "dst".
	for i := 0; i < 50; i++ {
		srcCache.Set(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("v%d", i)), 0)
	}

	srcTopo := NewTopology()
	srcNode := NewNode("src", "127.0.0.1:0")
	srcTopo.AddNode(srcNode)
	srcRing := hashring.New(150)
	srcRing.AddNode("src")
	srcRing.AddNode("dst")

	dstSrv := network.NewServer(network.ServerConfig{Addr: "127.0.0.1:0", MaxConns: 100}, dstCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := dstSrv.Start(ctx); err != nil {
		t.Fatalf("target server start: %v", err)
	}
	t.Cleanup(func() { dstSrv.Shutdown() })

	dstAddr := dstSrv.Addr().String()
	dstNode := NewNode("dst", dstAddr)
	srcTopo.AddNode(dstNode)

	srcMgr := NewManager(srcNode, srcTopo, srcRing, srcCache)

	// Poll ConnectionCount during migration to find the max.
	var maxConn int64
	stopPoll := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-stopPoll:
				return
			default:
			}
			cc := dstSrv.ConnectionCount()
			for {
				old := atomic.LoadInt64(&maxConn)
				if cc <= old {
					break
				}
				if atomic.CompareAndSwapInt64(&maxConn, old, cc) {
					break
				}
			}
			time.Sleep(50 * time.Microsecond)
		}
	}()

	// Find keys that hash to "dst" and migrate them.
	dstKeys := srcMgr.collectAffectedKeys("dst")
	migrated, batchErr := srcMgr.migrateKeys(dstKeys, "dst")

	// Stop polling and wait for the poll goroutine to exit.
	close(stopPoll)
	<-pollDone

	if batchErr != nil {
		t.Errorf("migrateKeys error: %v", batchErr)
	}
	if migrated != len(dstKeys) {
		t.Errorf("migrated = %d, want %d", migrated, len(dstKeys))
	}

	// Verify only 1 TCP connection was ever active at a time.
	if maxConn > 1 {
		t.Errorf("max concurrent connections = %d, want at most 1 (single connection per rebalance)", maxConn)
	}

	// Verify all keys migrated.
	for _, key := range dstKeys {
		if _, err := srcCache.Get(key); err == nil {
			t.Errorf("source still has key %s after migration", key)
		}
	}
}

func TestDeadTargetMidRebalance(t *testing.T) {
	srcCache := newTestCache()
	dstCache := newTestCache()

	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		srcCache.Set(fmt.Sprintf("key:%05d", i), []byte(fmt.Sprintf("val%d", i)), 60*time.Second)
	}

	srcTopo := NewTopology()
	srcNode := NewNode("src", "127.0.0.1:0")
	srcTopo.AddNode(srcNode)
	srcRing := hashring.New(150)
	srcRing.AddNode("src")
	srcRing.AddNode("dst")

	dstSrv := network.NewServer(network.ServerConfig{Addr: "127.0.0.1:0"}, dstCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := dstSrv.Start(ctx); err != nil {
		t.Fatalf("target server start: %v", err)
	}

	dstAddr := dstSrv.Addr().String()
	dstNode := NewNode("dst", dstAddr)
	srcTopo.AddNode(dstNode)

	srcMgr := NewManager(srcNode, srcTopo, srcRing, srcCache)

	dstKeys := srcMgr.collectAffectedKeys("dst")
	if len(dstKeys) == 0 {
		t.Fatal("no keys hash to dst — cannot test mid-rebalance failure")
	}

	// Start rebalance in background, then kill the target quickly.
	status := srcMgr.rebalancer.StartRebalance("src", "dst", dstKeys)

	// Wait briefly so some keys migrate, then shut down the target.
	time.Sleep(10 * time.Millisecond)
	dstSrv.Shutdown()
	cancel()

	select {
	case <-status.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("rebalance did not complete within timeout")
	}

	// Some keys should still be on source (the ones that didn't make it).
	remainingOnSrc := 0
	for _, key := range dstKeys {
		if _, err := srcCache.Get(key); err == nil {
			remainingOnSrc++
		}
	}

	if remainingOnSrc == 0 {
		t.Error("expected some keys to remain on source after target died mid-rebalance")
	}

	t.Logf("migrated %d/%d keys, %d remaining on source after target killed",
		len(dstKeys)-remainingOnSrc, len(dstKeys), remainingOnSrc)
}

// TestMigrateKeys_TargetNotFoundInTopology verifies that if the target node
// is not in the topology (e.g. died between scheduling and execution),
// migrateKeys returns an error and keys stay on the source.
func TestMigrateKeys_TargetNotFoundInTopology(t *testing.T) {
	srcCache := newTestCache()
	srcTopo := NewTopology()
	srcNode := NewNode("src", "127.0.0.1:0")
	srcTopo.AddNode(srcNode)
	srcRing := hashring.New(150)
	srcRing.AddNode("src")

	srcMgr := NewManager(srcNode, srcTopo, srcRing, srcCache)
	srcCache.Set("orphan", []byte("data"), 0)

	migrated, err := srcMgr.migrateKeys([]string{"orphan"}, "dead-node")
	if err == nil {
		t.Error("expected error when target not in topology")
	}
	if migrated != 0 {
		t.Errorf("migrated = %d, want 0", migrated)
	}

	// Key stays on source.
	val, err := srcCache.Get("orphan")
	if err != nil {
		t.Fatalf("source should still have orphan: %v", err)
	}
	if string(val) != "data" {
		t.Errorf("source value = %q, want data", string(val))
	}
}

// TestMigrateKey_KeyEvictedBetweenScanAndMigrate simulates the race where
// a key expires or is evicted after collectAffectedKeys returns it but
// before migrateSingleKey reads it.
func TestMigrateKey_KeyEvictedBetweenScanAndMigrate(t *testing.T) {
	srcCache := newTestCache()
	dstCache := newTestCache()

	dstSrv := network.NewServer(network.ServerConfig{Addr: "127.0.0.1:0"}, dstCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := dstSrv.Start(ctx); err != nil {
		t.Fatalf("target server start: %v", err)
	}
	t.Cleanup(func() { dstSrv.Shutdown() })

	client := network.NewClient(dstSrv.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	srcTopo := NewTopology()
	srcNode := NewNode("src", "127.0.0.1:0")
	srcTopo.AddNode(srcNode)
	srcRing := hashring.New(150)
	srcRing.AddNode("src")
	srcTopo.AddNode(NewNode("dst", dstSrv.Addr().String()))

	srcMgr := NewManager(srcNode, srcTopo, srcRing, srcCache)

	// Set a key, then delete it before migration (simulates eviction race).
	srcCache.Set("volatile", []byte("data"), 0)
	srcCache.Delete("volatile")

	err := srcMgr.migrateSingleKey("volatile", client)
	if err == nil {
		t.Error("expected error when key was evicted between scan and migration")
	}

	// Target should not have the key.
	if _, err := dstCache.Get("volatile"); err == nil {
		t.Error("target should not have key that failed to migrate")
	}
}

// TestMigrateBatch_MixedValueSizes verifies that a single reused connection
// correctly frames RESP responses when migrating keys with wildly different
// value sizes in the same batch. This specifically exercises the io.ReadFull
// fix — a short read on a large value would desync framing for subsequent
// small-value keys if the client used bufio.Reader.Read() instead.
func TestMigrateBatch_MixedValueSizes(t *testing.T) {
	srcCache := newTestCache()
	dstCache := newTestCache()

	dstSrv := network.NewServer(network.ServerConfig{Addr: "127.0.0.1:0"}, dstCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := dstSrv.Start(ctx); err != nil {
		t.Fatalf("target server start: %v", err)
	}
	t.Cleanup(func() { dstSrv.Shutdown() })

	srcTopo := NewTopology()
	srcNode := NewNode("src", "127.0.0.1:0")
	srcTopo.AddNode(srcNode)
	srcRing := hashring.New(150)
	srcRing.AddNode("src")
	srcTopo.AddNode(NewNode("dst", dstSrv.Addr().String()))

	srcMgr := NewManager(srcNode, srcTopo, srcRing, srcCache)

	// Seed: 10 small keys, 1 large key (256 KiB), then 10 more small keys.
	// The large key in the middle is the most likely to expose a framing bug.
	smallVal := []byte("small")
	largeVal := make([]byte, 256*1024)
	for i := range largeVal {
		largeVal[i] = byte('A' + (i % 26))
	}

	keys := make([]string, 0, 21)
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("small-before:%02d", i)
		srcCache.Set(k, smallVal, 0)
		keys = append(keys, k)
	}
	srcCache.Set("LARGE", largeVal, 0)
	keys = append(keys, "LARGE")
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("small-after:%02d", i)
		srcCache.Set(k, smallVal, 0)
		keys = append(keys, k)
	}

	// Migrate via the batch function — single connection, sequential SETs.
	migrated, err := srcMgr.migrateKeys(keys, "dst")
	if err != nil {
		t.Fatalf("migrateKeys: %v", err)
	}
	if migrated != len(keys) {
		t.Fatalf("migrated = %d, want %d", migrated, len(keys))
	}

	// Verify every key landed on target with correct content.
	for _, k := range keys {
		got, err := dstCache.Get(k)
		if err != nil {
			t.Errorf("target missing key %q: %v", k, err)
			continue
		}
		var want []byte
		if k == "LARGE" {
			want = largeVal
		} else {
			want = smallVal
		}
		if len(got) != len(want) {
			t.Errorf("key %q: value len = %d, want %d", k, len(got), len(want))
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("key %q: value mismatch at byte %d", k, i)
				break
			}
		}
	}

	// Verify source is empty.
	for _, k := range keys {
		if _, err := srcCache.Get(k); err == nil {
			t.Errorf("source still has key %q after migration", k)
		}
	}
}

// TestMigrateKey_NaturallyExpiredTTL exercises the lazy-expiration path:
// a key's TTL expires naturally between the Keys() scan and the Get()
// during migrateSingleKey. This is distinct from the explicit-eviction
// test (which uses Delete()) — here IsExpired() triggers on the Entry.
func TestMigrateKey_NaturallyExpiredTTL(t *testing.T) {
	srcCache := newTestCache()
	dstCache := newTestCache()

	dstSrv := network.NewServer(network.ServerConfig{Addr: "127.0.0.1:0"}, dstCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := dstSrv.Start(ctx); err != nil {
		t.Fatalf("target server start: %v", err)
	}
	t.Cleanup(func() { dstSrv.Shutdown() })

	client := network.NewClient(dstSrv.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	srcTopo := NewTopology()
	srcNode := NewNode("src", "127.0.0.1:0")
	srcTopo.AddNode(srcNode)
	srcRing := hashring.New(150)
	srcRing.AddNode("src")
	srcTopo.AddNode(NewNode("dst", dstSrv.Addr().String()))

	srcMgr := NewManager(srcNode, srcTopo, srcRing, srcCache)

	// Set a key with a 1ms TTL. By the time migrateSingleKey reads it,
	// it will have expired. LocalCache.Get() checks IsExpired() and
	// returns "key expired" — the same error path as explicit Delete.
	srcCache.Set("expiring", []byte("gone-soon"), 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond) // ensure expiry

	err := srcMgr.migrateSingleKey("expiring", client)
	if err == nil {
		t.Error("expected error when key expired naturally between scan and migration")
	}

	// Target should not have the key.
	if _, err := dstCache.Get("expiring"); err == nil {
		t.Error("target should not have naturally-expired key")
	}
}
