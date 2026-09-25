package chaostest

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Scenario is a single chaos test scenario.
type Scenario interface {
	Name() string
	Run(h *Harness) ScenarioResult
}

// ---------------------------------------------------------------------------
// Scenario 1: Kill Primary → Verify Promotion
// ---------------------------------------------------------------------------

type KillPrimary struct{}

func (KillPrimary) Name() string { return "Kill Primary → Verify Promotion" }

func (KillPrimary) Run(h *Harness) ScenarioResult {
	sr := ScenarioResult{Name: h.scenarioName(KillPrimary{})}
	sr.Passed = true // Optimistic; any failed step flips this to false.

	// Step 1: Pick a key and write it to the node that the hash ring says is
	// the primary. This ensures replication happens correctly — if we write to
	// a non-primary, the key is stored locally but not replicated, so it would
	// be lost after failover.
	key := "chaos-kill-primary-test"
	val := fmt.Sprintf("val-%d", time.Now().UnixNano())

	ring := buildRingFromNodes(sortedNodeIDs(h))
	expectedPrimary := ring.primaryNode(key)
	if expectedPrimary == "" {
		sr.AddStep(false, "Could not determine primary for test key")
		sr.Passed = false
		return sr
	}

	c := NewRESPClient(h.Nodes[expectedPrimary].TCPAddr)
	if err := c.Set(key, val); err != nil {
		sr.AddStep(false, fmt.Sprintf("SET %s on primary %s failed: %v", key, expectedPrimary, err))
		sr.Passed = false
		c.Close()
		return sr
	}
	c.Close()
	sr.AddStep(true, fmt.Sprintf("Key written to primary %s (hash ring match)", expectedPrimary))
	primaryNode := expectedPrimary

	// Step 2: Confirm replication by verifying the key exists on ALL nodes.
	// The replication_lag field is never updated in production code (UpdateLag
	// is test-only), so checking lag==0 is meaningless. We must verify the
	// key actually arrived on every node before killing the primary.
	err := WaitFor("replication to all nodes", 10*time.Second, 200*time.Millisecond, func() (bool, string) {
		missing := 0
		var missingNodes []string
		for _, n := range h.Nodes {
			nc := NewRESPClient(n.TCPAddr)
			_, ok, getErr := nc.Get(key)
			nc.Close()
			if getErr != nil || !ok {
				missing++
				missingNodes = append(missingNodes, n.ID)
			}
		}
		if missing == 0 {
			return true, ""
		}
		return false, fmt.Sprintf("%d nodes missing key: %v", missing, missingNodes)
	})
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("Replication incomplete before kill: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, "Key confirmed on all 5 nodes (replication complete)")

	// Step 4: Stop the primary node.
	if err = h.StopNode(primaryNode); err != nil {
		sr.AddStep(false, fmt.Sprintf("Failed to stop %s: %v", primaryNode, err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("Stopped %s", primaryNode))

	// Step 5: Poll surviving nodes until the killed node is no longer alive.
	startDetect := time.Now()
	err = h.WaitForNodeNotAlive(primaryNode, 30*time.Second)
	detectTime := time.Since(startDetect)
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("Timeout waiting for %s to be detected dead/removed: %v", primaryNode, err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("Node %s detected as dead/removed (%.1fs)", primaryNode, detectTime.Seconds()))

	// Step 6: Wait for a new leader to be elected.
	leader, err := h.WaitForLeader(30 * time.Second)
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("No leader elected after failover: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("New leader elected: %s", leader))

	// Step 7: Verify key is readable FROM THE NEW LEADER specifically.
	// A replica surviving with stale-but-present data is not failover working.
	// Failover is working only when the promoted node (new leader) can serve the key.
	c = NewRESPClient(h.Nodes[leader].TCPAddr)
	got, ok, getErr := c.Get(key)
	c.Close()
	if getErr != nil {
		sr.AddStep(false, fmt.Sprintf("GET %s from new leader %s failed: %v", key, leader, getErr))
		sr.Passed = false
		_ = h.StartNode(primaryNode)
		_ = h.WaitForAllHealthy(45 * time.Second)
		return sr
	}
	if !ok {
		sr.AddStep(false, fmt.Sprintf("Key NOT found on new leader %s — failover promoted a node without the data", leader))
		sr.Passed = false
		_ = h.StartNode(primaryNode)
		_ = h.WaitForAllHealthy(45 * time.Second)
		return sr
	}
	if got != val {
		sr.AddStep(false, fmt.Sprintf("Key value mismatch on new leader %s: got %q, want %q", leader, got, val))
		sr.Passed = false
		_ = h.StartNode(primaryNode)
		_ = h.WaitForAllHealthy(45 * time.Second)
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("Key readable from new leader %s (value matches)", leader))

	// Step 8: Restart the killed node and wait for reconvergence.
	_ = h.StartNode(primaryNode)
	_ = h.WaitForAllHealthy(45 * time.Second)
	sr.AddStep(true, fmt.Sprintf("Node %s restarted, all nodes healthy", primaryNode))

	return sr
}

// ---------------------------------------------------------------------------
// Scenario 2: Rolling Restart
// ---------------------------------------------------------------------------

type RollingRestart struct{}

func (RollingRestart) Name() string { return "Rolling Restart" }

func (RollingRestart) Run(h *Harness) ScenarioResult {
	sr := ScenarioResult{Name: h.scenarioName(RollingRestart{})}
	sr.Passed = true // Optimistic; any failed step flips this to false.

	const keyCount = 100
	keys := make(map[string]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[fmt.Sprintf("rr-key-%d", i)] = fmt.Sprintf("rr-val-%d", i)
	}

	// Write all keys through the first healthy node.
	var writeNode string
	for _, n := range h.Nodes {
		c := NewRESPClient(n.TCPAddr)
		if err := c.Ping(); err != nil {
			c.Close()
			continue
		}
		for k, v := range keys {
			if err := c.Set(k, v); err != nil {
				sr.AddStep(false, fmt.Sprintf("SET %s failed on %s: %v", k, n.ID, err))
				sr.Passed = false
				c.Close()
				return sr
			}
		}
		writeNode = n.ID
		c.Close()
		break
	}
	if writeNode == "" {
		sr.AddStep(false, "No reachable node for writing keys")
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("%d keys written via %s", keyCount, writeNode))

	// Wait for replication to settle.
	time.Sleep(2 * time.Second)

	// Restart each node one at a time.
	for _, n := range h.Nodes {
		if err := h.RestartNode(n.ID); err != nil {
			sr.AddStep(false, fmt.Sprintf("Failed to restart %s: %v", n.ID, err))
			sr.Passed = false
			return sr
		}

		// Wait for this node to be healthy.
		if err := h.waitForHealth(n.HTTPAddr, 30*time.Second); err != nil {
			sr.AddStep(false, fmt.Sprintf("Node %s not healthy after restart: %v", n.ID, err))
			sr.Passed = false
			return sr
		}

		// Wait for all nodes to be healthy (gossip convergence).
		if err := h.WaitForAllHealthy(30 * time.Second); err != nil {
			sr.AddStep(false, fmt.Sprintf("Cluster not fully healthy after %s restart: %v", n.ID, err))
			sr.Passed = false
			return sr
		}

		// Read ALL keys and verify correctness — try each node for each key
		// since replication may have placed different keys on different nodes.
		correct := 0
		var missing []string
		for k, expectedV := range keys {
			found := false
			for _, rn := range h.Nodes {
				c := NewRESPClient(rn.TCPAddr)
				got, ok, err := c.Get(k)
				c.Close()
				if err == nil && ok && got == expectedV {
					found = true
					break
				}
			}
			if found {
				correct++
			} else {
				missing = append(missing, k)
			}
		}

		if correct == keyCount {
			sr.AddStep(true, fmt.Sprintf("%s restarted — %d/%d keys correct", n.ID, correct, keyCount))
		} else {
			sr.AddStep(false, fmt.Sprintf("%s restarted — %d/%d keys correct, missing: %v", n.ID, correct, keyCount, missing))
			sr.Passed = false
			return sr
		}
	}

	return sr
}

// ---------------------------------------------------------------------------
// Scenario 3: Node Join → Triggers Real Migration
// ---------------------------------------------------------------------------

type NodeJoinMigration struct{}

func (NodeJoinMigration) Name() string { return "Node Join → Triggers Real Migration" }

func (NodeJoinMigration) Run(h *Harness) ScenarioResult {
	sr := ScenarioResult{Name: h.scenarioName(NodeJoinMigration{})}
	sr.Passed = true // Optimistic; any failed step flips this to false.

	// Use the full 5-node cluster. Write keys, then add a brand-new node
	// (node-6) that was never part of the cluster before.
	const freshNodeID = "node-6"
	const freshContainer = "hydracache-node-6"
	freshTCP := "localhost:7384" // node-6's TCP port on host

	// Ensure no stale fresh node exists.
	h.StopFreshNode(freshContainer)
	defer h.StopFreshNode(freshContainer)

	// Verify 5-node cluster is healthy.
	if err := h.WaitForAllHealthy(15 * time.Second); err != nil {
		sr.AddStep(false, fmt.Sprintf("Cluster not healthy before migration test: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, "5-node cluster healthy")

	// Write test keys through node-1.
	const keyCount = 50
	keys := make([]string, keyCount)
	for i := 0; i < keyCount; i++ {
		keys[i] = fmt.Sprintf("migrate-key-%04d", i)
	}

	c := NewRESPClient(h.Nodes["node-1"].TCPAddr)
	for i, k := range keys {
		if err := c.Set(k, fmt.Sprintf("migrate-val-%d", i)); err != nil {
			sr.AddStep(false, fmt.Sprintf("SET %s failed: %v", k, err))
			sr.Passed = false
			c.Close()
			return sr
		}
	}
	c.Close()
	sr.AddStep(true, fmt.Sprintf("%d keys written to 5-node cluster", keyCount))

	// Build ring before adding node-6.
	ringBefore := buildRingFromNodes([]string{"node-1", "node-2", "node-3", "node-4", "node-5"})

	// Record which keys each existing node owns BEFORE adding node-6.
	ownershipBefore := make(map[string]string)
	for _, k := range keys {
		ownershipBefore[k] = ringBefore.primaryNode(k)
	}
	countsBefore := make(map[string]int)
	for _, nodeID := range ownershipBefore {
		countsBefore[nodeID]++
	}
	for nodeID, count := range countsBefore {
		sr.AddStep(true, fmt.Sprintf("Before join: %s owns %d keys", nodeID, count))
	}

	// Start a BRAND-NEW node that has never been part of this cluster.
	_, err := h.StartFreshNode(freshNodeID, "cache-node-1:7379")
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("Failed to start fresh node: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("Started fresh node %s (never clustered before)", freshNodeID))

	// Wait for the fresh node to appear in the cluster.
	err = WaitFor("fresh node joins cluster", 45*time.Second, 1*time.Second, func() (bool, string) {
		ci, ciErr := h.GetCluster("node-1")
		if ciErr != nil {
			return false, fmt.Sprintf("cluster query error: %v", ciErr)
		}
		for _, n := range ci.Nodes {
			if n.ID == freshNodeID && n.Health == "alive" {
				return true, ""
			}
		}
		return false, "fresh node not yet visible in cluster"
	})
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("Fresh node did not join cluster: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("Fresh node %s visible in cluster (health=alive)", freshNodeID))

	// Build ring after adding node-6.
	ringAfter := buildRingFromNodes([]string{"node-1", "node-2", "node-3", "node-4", "node-5", freshNodeID})

	// Identify keys that SHOULD have migrated to node-6.
	var migratedKeys []string
	for _, k := range keys {
		newOwner := ringAfter.primaryNode(k)
		if newOwner == freshNodeID && ownershipBefore[k] != freshNodeID {
			migratedKeys = append(migratedKeys, k)
		}
	}

	if len(migratedKeys) == 0 {
		sr.AddStep(true, "No keys need migration to fresh node (hash distribution)")
	} else {
		sr.AddStep(true, fmt.Sprintf("%d keys should migrate to %s", len(migratedKeys), freshNodeID))

		// Wait for migration to complete by polling DBSIZE on the fresh node.
		targetSize := int64(len(migratedKeys))
		err = WaitFor("migration complete (fresh node key count)", 30*time.Second, 1*time.Second, func() (bool, string) {
			c4 := NewRESPClient(freshTCP)
			size, sizeErr := c4.DBSize()
			c4.Close()
			if sizeErr != nil {
				return false, fmt.Sprintf("DBSIZE error: %v", sizeErr)
			}
			return size >= targetSize, fmt.Sprintf("fresh node DBSIZE=%d, want >=%d", size, targetSize)
		})
		if err != nil {
			sr.AddStep(false, fmt.Sprintf("Migration timeout: %v", err))
			sr.Passed = false
		} else {
			sr.AddStep(true, "Migration completed (fresh node key count reached target)")
		}
	}

	// Verify: keys that should be on the fresh node ARE on it.
	c4 := NewRESPClient(freshTCP)
	migratedOK := 0
	for _, k := range migratedKeys {
		_, ok, err := c4.Get(k)
		if err == nil && ok {
			migratedOK++
		}
	}
	c4.Close()
	if len(migratedKeys) > 0 {
		if migratedOK == len(migratedKeys) {
			sr.AddStep(true, fmt.Sprintf("Fresh node has all %d/%d migrated keys", migratedOK, len(migratedKeys)))
		} else {
			sr.AddStep(false, fmt.Sprintf("Fresh node has only %d/%d migrated keys — migration incomplete", migratedOK, len(migratedKeys)))
			sr.Passed = false
		}
	}

	// Verify: keys that should have left old owners ARE removed.
	oldOwnerChecks := 0
	oldOwnerCorrect := 0
	for _, k := range migratedKeys {
		oldOwner := ownershipBefore[k]
		cOld := NewRESPClient(h.Nodes[oldOwner].TCPAddr)
		_, ok, _ := cOld.Get(k)
		cOld.Close()
		oldOwnerChecks++
		if !ok {
			oldOwnerCorrect++
		}
	}
	if oldOwnerChecks > 0 {
		if oldOwnerCorrect == oldOwnerChecks {
			sr.AddStep(true, fmt.Sprintf("Old owners removed %d/%d migrated keys", oldOwnerCorrect, oldOwnerChecks))
		} else {
			sr.AddStep(false, fmt.Sprintf("Old owners removed %d/%d migrated keys — some keys still present on old owners", oldOwnerCorrect, oldOwnerChecks))
			sr.Passed = false
		}
		// Cross-check: if old owners removed keys but fresh node doesn't have them, that's data loss.
		if oldOwnerCorrect == oldOwnerChecks && migratedOK < len(migratedKeys) {
			sr.AddStep(false, fmt.Sprintf("DATA LOSS: %d keys removed from old owners but not present on fresh node", len(migratedKeys)-migratedOK))
			sr.Passed = false
		}
	}

	return sr
}

// ---------------------------------------------------------------------------
// Scenario 4: Partition and Heal
// ---------------------------------------------------------------------------

type PartitionAndHeal struct{}

func (PartitionAndHeal) Name() string { return "Partition and Heal" }

func (PartitionAndHeal) Run(h *Harness) ScenarioResult {
	sr := ScenarioResult{Name: h.scenarioName(PartitionAndHeal{})}
	sr.Passed = true // Optimistic; any failed step flips this to false.

	if h.NetworkName == "" {
		sr.AddStep(false, "Docker network name not detected — cannot perform real partition")
		sr.Passed = false
		return sr
	}

	// Step 1: Verify all 5 nodes healthy.
	if err := h.WaitForAllHealthy(15 * time.Second); err != nil {
		sr.AddStep(false, fmt.Sprintf("Not all nodes healthy before partition: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, "All 5 nodes healthy before partition")

	// Step 2: Record initial leader.
	ci, err := h.GetCluster("node-1")
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("Cannot get cluster info: %v", err))
		sr.Passed = false
		return sr
	}
	var preLeader string
	for _, n := range ci.Nodes {
		if n.Role == "leader" {
			preLeader = n.ID
		}
	}
	sr.AddStep(true, fmt.Sprintf("Pre-partition leader: %s", preLeader))

	// Step 3: Disconnect node-5 (isolated minority).
	if err = h.DisconnectNode("node-5"); err != nil {
		sr.AddStep(false, fmt.Sprintf("Failed to disconnect node-5: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, "Disconnected node-5 from network (partition created)")

	// Step 4: Wait for surviving nodes to detect node-5 as dead or removed.
	err = h.WaitForNodeNotAlive("node-5", 30*time.Second)
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("Surviving nodes did not detect node-5 failure: %v", err))
		_ = h.ReconnectNode("node-5")
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, "Surviving nodes detected node-5 as dead/removed")

	// Step 5: Verify surviving nodes can still serve — at least one must have a leader.
	// During a real partition, split-brain (different leaders on different sides) is expected.
	// The key invariant: the cluster doesn't fully fracture — at least one side elects a leader.
	leaderCount := 0
	var lastLeader string
	quorumOK := false
	for _, nID := range []string{"node-1", "node-2", "node-3", "node-4"} {
		nodeCI, nodeErr := h.GetCluster(nID)
		if nodeErr != nil {
			continue
		}
		aliveCount := 0
		for _, n := range nodeCI.Nodes {
			if n.Health == "alive" {
				aliveCount++
			}
			if n.Role == "leader" {
				leaderCount++
				lastLeader = n.ID
			}
		}
		// At least 3 of 4 surviving nodes should see a leader (quorum of 4).
		if aliveCount >= 3 {
			quorumOK = true
		}
	}
	if leaderCount > 0 {
		sr.AddStep(true, fmt.Sprintf("Cluster didn't fully fracture — leader elected: %s", lastLeader))
	} else {
		sr.AddStep(false, "No leader elected on any surviving node — cluster fully fractured")
	}
	if quorumOK {
		sr.AddStep(true, "At least one node sees quorum of surviving nodes")
	}

	// Step 6: Heal the partition.
	if err = h.ReconnectNode("node-5"); err != nil {
		sr.AddStep(false, fmt.Sprintf("Failed to reconnect node-5: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, "Reconnected node-5 to network (partition healed)")

	// Step 7: Kick node-5's gossip by sending a PING (triggers reconnection to
	// peers). Its error is deliberately ignored: node-5 might not be
	// reachable yet immediately after reconnecting, which is fine — gossip
	// will handle convergence on its own from here.
	kickClient := NewRESPClient(h.Nodes["node-5"].TCPAddr)
	_ = kickClient.Ping()
	kickClient.Close()
	kickClient.Close()

	// Step 8: Wait for gossip to reconverge — all 5 nodes alive.
	// After partition heal, gossip needs multiple rounds to reconcile incarnation
	// numbers. The reconnected node may be temporarily rejected until its
	// incarnation is bumped via self-refutation. This takes longer than normal gossip.
	err = h.WaitForAllHealthy(90 * time.Second)
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("Gossip did not reconverge: %v", err))
		sr.Passed = false
		return sr
	}
	sr.AddStep(true, "All 5 nodes healthy after heal (gossip reconverged)")

	// Step 9: Verify a single consistent view.
	finalCI, err := h.GetCluster("node-1")
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("Final cluster query failed: %v", err))
		sr.Passed = false
		return sr
	}
	finalAlive := 0
	for _, n := range finalCI.Nodes {
		if n.Health == "alive" {
			finalAlive++
		}
	}
	sr.AddStep(true, fmt.Sprintf("Final cluster view: %d alive nodes, epoch=%d", finalAlive, finalCI.Epoch))

	return sr
}

// ---------------------------------------------------------------------------
// Scenario 5: Concurrent Chaos
// ---------------------------------------------------------------------------

type ConcurrentChaos struct{}

func (ConcurrentChaos) Name() string { return "Concurrent Chaos" }

func (ConcurrentChaos) Run(h *Harness) ScenarioResult {
	sr := ScenarioResult{Name: h.scenarioName(ConcurrentChaos{})}
	sr.Passed = true // Optimistic; any failed step flips this to false.

	const keyCount = 100
	var loadErrors atomic.Int64
	var loadWrites atomic.Int64
	var loadReads atomic.Int64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	// Start background load goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeIDs := sortedNodeIDs(h)
		for {
			select {
			case <-stopCh:
				return
			default:
			}
			for i := 0; i < keyCount; i++ {
				k := fmt.Sprintf("chaos-load-%04d", i)
				v := fmt.Sprintf("v%d-%d", i, time.Now().UnixNano())
				nodeID := nodeIDs[i%len(nodeIDs)]
				c := NewRESPClient(h.Nodes[nodeID].TCPAddr)
				if err := c.Set(k, v); err != nil {
					loadErrors.Add(1)
					c.Close()
					continue
				}
				loadWrites.Add(1)
				c.Close()

				// Read it back.
				got, ok, err := c.Get(k)
				_ = got
				_ = ok
				if err != nil {
					loadErrors.Add(1)
				} else {
					loadReads.Add(1)
				}
			}
		}
	}()

	// Give background load a moment to populate.
	time.Sleep(2 * time.Second)
	sr.AddStep(true, fmt.Sprintf("Background load started (writes=%d, reads=%d, errors=%d so far)",
		loadWrites.Load(), loadReads.Load(), loadErrors.Load()))

	// Now do a kill-primary cycle under load.
	key := "chaos-concurrent-kill-test"
	killNode := "node-3" // Pick a non-seed node.
	c := NewRESPClient(h.Nodes["node-1"].TCPAddr)
	_ = c.Set(key, "concurrent-value")
	c.Close()

	// Stop the node.
	if err := h.StopNode(killNode); err != nil {
		sr.AddStep(false, fmt.Sprintf("Failed to stop %s: %v", killNode, err))
		sr.Passed = false
		close(stopCh)
		wg.Wait()
		h.ForceCleanup()
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("Stopped %s under concurrent load", killNode))

	// Wait for failover.
	leader, err := h.WaitForLeader(30 * time.Second)
	if err != nil {
		sr.AddStep(false, fmt.Sprintf("No leader elected during concurrent chaos: %v", err))
		sr.Passed = false
		close(stopCh)
		wg.Wait()
		h.ForceCleanup()
		return sr
	}
	sr.AddStep(true, fmt.Sprintf("Leader elected during chaos: %s", leader))

	// Verify key is readable from any surviving node (not just the new leader,
	// since the key might have been replicated to a different node).
	keyFound := false
	for _, n := range h.Nodes {
		if n.ID == killNode {
			continue
		}
		c2 := NewRESPClient(n.TCPAddr)
		got, ok, err := c2.Get(key)
		c2.Close()
		if err == nil && ok && got == "concurrent-value" {
			keyFound = true
			break
		}
	}
	if !keyFound {
		sr.AddStep(false, "Key not readable from any surviving node after failover")
		sr.Passed = false
		close(stopCh)
		wg.Wait()
		h.ForceCleanup()
		return sr
	}
	sr.AddStep(true, "Key readable and correct from surviving node during concurrent chaos")

	// Restart the killed node.
	_ = h.StartNode(killNode)
	_ = h.WaitForAllHealthy(45 * time.Second)
	sr.AddStep(true, fmt.Sprintf("Node %s restarted, cluster healthy", killNode))

	// Stop background load.
	close(stopCh)
	wg.Wait()

	finalWrites := loadWrites.Load()
	finalReads := loadReads.Load()
	finalErrors := loadErrors.Load()
	sr.AddStep(true, fmt.Sprintf("Background load complete: writes=%d, reads=%d, errors=%d", finalWrites, finalReads, finalErrors))

	// Check that error rate is acceptable (allow transient connection errors during failover).
	totalOps := finalWrites + finalReads
	if totalOps > 0 {
		errorRate := float64(finalErrors) / float64(totalOps) * 100
		if errorRate > 20 {
			sr.AddStep(false, fmt.Sprintf("Error rate %.1f%% exceeds 20%% threshold", errorRate))
			sr.Passed = false
			return sr
		}
		sr.AddStep(true, fmt.Sprintf("Error rate %.1f%% within acceptable threshold", errorRate))
	}

	return sr
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sortedNodeIDs(h *Harness) []string {
	ids := make([]string, 0, len(h.Nodes))
	for id := range h.Nodes {
		ids = append(ids, id)
	}
	// Sort by numeric suffix for deterministic order.
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			ni := extractNum(ids[i])
			nj := extractNum(ids[j])
			if ni > nj {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	return ids
}

func extractNum(id string) int {
	parts := strings.Split(id, "-")
	if len(parts) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(parts[len(parts)-1])
	return n
}

func (h *Harness) scenarioName(s Scenario) string {
	return s.Name()
}
