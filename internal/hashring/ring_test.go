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

func TestHashRingSuccessorAfterRemoval(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	succ := ring.SuccessorAfterRemoval("node-1")
	if succ == "" {
		t.Fatal("SuccessorAfterRemoval should return a node")
	}
	if succ == "node-1" {
		t.Error("successor should not be the node itself")
	}
	// node-1 is removed, successor must be one of the remaining nodes.
	if succ != "node-2" && succ != "node-3" {
		t.Errorf("successor = %q, want node-2 or node-3", succ)
	}
}

func TestHashRingSuccessorAfterRemoval_NotFound(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	succ := ring.SuccessorAfterRemoval("nonexistent")
	if succ != "" {
		t.Errorf("successor of nonexistent = %q, want empty", succ)
	}
}

func TestHashRingSuccessorAfterRemoval_Empty(t *testing.T) {
	ring := New(150)
	succ := ring.SuccessorAfterRemoval("node-1")
	if succ != "" {
		t.Errorf("successor on empty ring = %q, want empty", succ)
	}
}

func TestHashRingReplaceNode(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	// Verify all three nodes own keys.
	keys1 := 0
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "node-1" {
			keys1++
		}
	}
	if keys1 == 0 {
		t.Fatal("node-1 should own some keys before replacement")
	}

	// Replace node-1 with node-4.
	ring.ReplaceNode("node-1", "node-4")

	if ring.NodeCount() != 3 {
		t.Errorf("NodeCount = %d, want 3 after replacement", ring.NodeCount())
	}

	// node-1 should own zero keys now.
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "node-1" {
			t.Error("node-1 should own no keys after replacement")
			break
		}
	}

	// node-4 should now own the keys node-1 previously owned.
	keys4 := 0
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "node-4" {
			keys4++
		}
	}
	if keys4 != keys1 {
		t.Errorf("node-4 owns %d keys, want %d (same as node-1 before)", keys4, keys1)
	}
}

func TestHashRingReplaceNode_SameNode(t *testing.T) {
	ring := New(150)
	ring.AddNode("node-1")
	ring.AddNode("node-2")

	// Replacing a node with itself should be a no-op (keeps positions).
	ring.ReplaceNode("node-1", "node-1")

	if ring.NodeCount() != 2 {
		t.Errorf("NodeCount = %d, want 2", ring.NodeCount())
	}
}

func TestHashRingReplaceNode_PromotedNodeAlreadyInRing(t *testing.T) {
	ring := New(150)
	ring.AddNode("dead-primary")
	ring.AddNode("replica-a")
	ring.AddNode("replica-b")

	// Capture replica-a's OWN keys before replacement.
	keysA_before := 0
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "replica-a" {
			keysA_before++
		}
	}
	if keysA_before == 0 {
		t.Fatal("replica-a should own some keys before replacement")
	}

	// Capture dead-primary's keys before replacement.
	keysDead_before := 0
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "dead-primary" {
			keysDead_before++
		}
	}
	if keysDead_before == 0 {
		t.Fatal("dead-primary should own some keys before replacement")
	}

	// Replace dead-primary with replica-a (additive: replica-a keeps its own).
	ring.ReplaceNode("dead-primary", "replica-a")

	if ring.NodeCount() != 2 {
		t.Errorf("NodeCount = %d, want 2", ring.NodeCount())
	}

	// dead-primary should own no keys.
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "dead-primary" {
			t.Error("dead-primary should own no keys after replacement")
			break
		}
	}

	// replica-a should still own its ORIGINAL keys.
	keysA_after := 0
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "replica-a" {
			keysA_after++
		}
	}
	if keysA_after < keysA_before {
		t.Errorf("replica-a owns %d keys after, had %d before — original range lost",
			keysA_after, keysA_before)
	}

	// replica-a should now ALSO own dead-primary's former keys.
	if keysA_after < keysDead_before {
		t.Errorf("replica-a owns %d keys total, dead-primary had %d — not all transferred",
			keysA_after, keysDead_before)
	}

	// replica-b should still own its own keys (unchanged).
	keysB := 0
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "replica-b" {
			keysB++
		}
	}
	if keysB == 0 {
		t.Error("replica-b should still own keys")
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
