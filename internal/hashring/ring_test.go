package hashring

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestHashRingAddNode(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	if ring.NodeCount() != 3 {
		t.Errorf("expected 3 nodes, got %d", ring.NodeCount())
	}
}

func TestHashRingGetNode(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	node := ring.GetNode("user:123")
	if node == "" {
		t.Error("expected a node for key")
	}
}

func TestHashRingDistribution(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	dist := ring.Distribution()
	total := 0
	for _, count := range dist {
		total += count
	}

	expected := 150 * 3
	if total != expected {
		t.Errorf("expected %d virtual nodes, got %d", expected, total)
	}
}

func TestHashRingRemoveNode(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	ring.RemoveNode("node-2")

	if ring.NodeCount() != 2 {
		t.Errorf("expected 2 nodes after removal, got %d", ring.NodeCount())
	}
}

func TestHashRingKeyLookup(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")

	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		node := ring.GetNode(key)
		seen[node] = true
	}

	if len(seen) < 2 {
		t.Errorf("expected keys distributed across at least 2 nodes, got %d", len(seen))
	}
}

func TestHashRingGetNodes(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	nodes := ring.GetNodes("test-key", 2)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}

	if nodes[0] == nodes[1] {
		t.Error("expected different nodes")
	}
}

func TestLocator(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	locator := NewLocator(ring, 2)

	primary := locator.PrimaryNode("test-key")
	if primary == "" {
		t.Error("expected a primary node")
	}

	replicas := locator.ReplicaNodes("test-key")
	if len(replicas) < 1 {
		t.Error("expected at least 1 replica")
	}
}

func TestHashRingConsistency(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")

	results1 := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		results1[key] = ring.GetNode(key)
	}

	ring.AddNode("node-3")

	moved := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		if results1[key] != ring.GetNode(key) {
			moved++
		}
	}

	if moved > 50 {
		t.Errorf("too many keys moved on node addition: %d/100", moved)
	}
}

func TestRebalanceStatus_Complete_Race(t *testing.T) {
	ring := New(150)
	rebalancer := NewRebalancer(ring, nil)

	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", i)
	}

	status := rebalancer.StartRebalance("src", "dst", keys)

	// Readers: check IsComplete and MigratedKeys concurrently via the live pointer.
	var wg sync.WaitGroup
	const readers = 20
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = status.IsComplete()
				_ = status.GetMigratedKeys()
			}
		}()
	}

	// Wait for rebalance to finish.
	deadline := time.After(10 * time.Second)
	for {
		if status.IsComplete() {
			break
		}
		select {
		case <-deadline:
			t.Fatal("rebalance did not complete in time")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	wg.Wait()

	if !status.IsComplete() {
		t.Error("rebalance should be complete")
	}
	if status.GetMigratedKeys() != int64(len(keys)) {
		t.Errorf("MigratedKeys = %d, want %d", status.GetMigratedKeys(), len(keys))
	}
}
