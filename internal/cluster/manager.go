package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/hashring"
	"github.com/hydracache/hydracache/internal/heartbeat"
	"github.com/hydracache/hydracache/internal/network"
	"github.com/hydracache/hydracache/internal/replication"
)

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

type Manager struct {
	topology   *Topology
	selfNode   *Node
	ring       *hashring.HashRing
	rebalancer *hashring.Rebalancer
	gossip     *Gossip
	detector   *heartbeat.Detector
	transport  *heartbeat.Transport
	registry   *replication.ReplicaRegistry
	localCache cache.Cache
	mu         sync.RWMutex
	rebalMu    sync.Mutex
	syncMu     sync.Mutex
	syncing    map[string]struct{} // replica nodeIDs with in-flight sync
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func NewManager(selfNode *Node, topo *Topology, ring *hashring.HashRing, localCache cache.Cache) *Manager {
	m := &Manager{
		topology:   topo,
		selfNode:   selfNode,
		ring:       ring,
		localCache: localCache,
		registry:   replication.NewReplicaRegistry(),
		syncing:    make(map[string]struct{}),
		stopCh:     make(chan struct{}),
	}
	m.rebalancer = hashring.NewRebalancer(ring, m.migrateKeys)
	m.gossip = NewGossip(selfNode, topo)
	m.gossip.SetOnNewNode(func(node *Node) {
		_ = m.AddNode(node)
	})
	m.detector = heartbeat.NewDetector(selfNode.ID)

	// Build transport with callbacks to avoid import cycle
	m.transport = heartbeat.NewTransport(
		selfNode.ID,
		m.detector,
		m.pingPeer,
		m.alivePeers,
		1*time.Second,
	)

	return m
}

// pingPeer sends a PING to a peer and returns the round-trip time.
// Uses a short dial timeout to avoid blocking the entire ping loop
// when one peer is unreachable.
func (m *Manager) pingPeer(peerID, peerAddr string) (time.Duration, error) {
	client := network.NewClientWithTimeout(peerAddr, 500*time.Millisecond)
	start := time.Now()
	if err := client.Connect(); err != nil {
		return 0, err
	}
	defer client.Close()

	_, err := client.Send("PING")
	rtt := time.Since(start)
	if err != nil {
		return 0, err
	}
	return rtt, nil
}

// alivePeers returns the current alive peers for heartbeat transport.
func (m *Manager) alivePeers() []heartbeat.Peer {
	nodes := m.topology.AliveNodes()
	peers := make([]heartbeat.Peer, 0, len(nodes))
	for _, n := range nodes {
		peers = append(peers, heartbeat.Peer{ID: n.ID, Address: n.Address})
	}
	return peers
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.topology.AddNode(m.selfNode); err != nil {
		return fmt.Errorf("failed to add self to topology: %w", err)
	}

	// Register a ReplicaSet for this node (it's a primary for keys on the ring).
	// The primary's own entry holds the ReplicationStream used by replicateWrite.
	rs := replication.NewReplicaSet(m.selfNode.ID)
	rs.AddReplica(m.selfNode.ID, m.selfNode.Address)
	rs.SetStatus(m.selfNode.ID, replication.ReplicaActive) // primary's own entry is always active
	m.registry.Register(m.selfNode.ID, rs)

	// Wire Detector callbacks to Topology
	m.detector.OnNodeSuspect(func(nodeID string) {
		m.topology.SetNodeHealth(nodeID, HealthSuspect)
	})
	m.detector.OnNodeDead(func(nodeID string) {
		m.handleNodeDead(nodeID)
	})

	// Start failure detection and heartbeat transport
	m.detector.StartChecking(1 * time.Second)
	m.transport.Start()

	// Start gossip
	m.gossip.Start()

	m.wg.Add(1)
	go m.periodicStatus()

	log.Printf("[cluster] node %s started at %s", shortID(m.selfNode.ID), m.selfNode.Address)
	return nil
}

func (m *Manager) Bootstrap(addresses []string) error {
	if len(addresses) == 0 {
		return nil
	}
	connected := m.gossip.Bootstrap(addresses)
	if connected == 0 {
		log.Printf("[cluster] no seeds reachable, starting as single-node cluster")
	} else {
		log.Printf("[cluster] bootstrapped from %d seed(s)", connected)
	}
	return nil
}

func (m *Manager) Gossip() *Gossip {
	return m.gossip
}

func (m *Manager) Detector() *heartbeat.Detector {
	return m.detector
}

func (m *Manager) AddNode(node *Node) error {
	// Topology add may fail if gossip already added this node — that's fine,
	// we still need to register it on the ring and ReplicaRegistry.
	_ = m.topology.AddNode(node)

	m.rebalMu.Lock()
	defer m.rebalMu.Unlock()

	m.ring.AddNode(node.ID)
	log.Printf("[cluster] node %s added to ring", shortID(node.ID))

	// Register a ReplicaSet for the new primary, with all existing alive
	// nodes (including self) as replicas. This enables replicateWrite on
	// the new node and handleNodeDead promotion for it.
	rs := replication.NewReplicaSet(node.ID)
	rs.AddReplica(node.ID, node.Address)
	rs.SetStatus(node.ID, replication.ReplicaActive) // primary's own entry is always active
	for _, n := range m.topology.AliveNodes() {
		if n.ID != node.ID {
			rs.AddReplica(n.ID, n.Address)
			rs.SetStatus(n.ID, replication.ReplicaActive) // existing nodes don't need sync
		}
	}
	m.registry.Register(node.ID, rs)

	// Also add the new node as a replica to all existing primaries,
	// so their replicateWrite can fan out to this new node.
	// NOTE: Call site analysis — these existing nodes are already serving
	// their own key ranges independently. They do NOT need catch-up from
	// this new primary (or vice versa). The rebalance handles key migration
	// between them. This path is only reachable via gossip-triggered AddNode
	// (fresh join), NOT via the failover path (handleNodeDead manipulates
	// ring/topology/registry directly without calling AddNode). Setting
	// ReplicaActive immediately is correct here.
	for _, n := range m.topology.AliveNodes() {
		if n.ID == node.ID {
			continue
		}
		if existingRS, ok := m.registry.GetReplicaSet(n.ID); ok {
			existingRS.AddReplica(node.ID, node.Address)
			// ReplicaSyncing by default — initiate async catch-up handshake.
			m.wg.Add(1)
			go m.initiateReplicaSync(n.ID, node.ID, node.Address)
		}
	}

	m.launchRebalanceForNewNode(node.ID)
	return nil
}

func (m *Manager) launchRebalanceForNewNode(nodeID string) {
	_, affectedEnd := m.ring.GetAffectedRange(nodeID)
	if affectedEnd == 0 {
		return
	}

	affectedKeys := m.collectAffectedKeys(nodeID)
	if len(affectedKeys) == 0 {
		return
	}

	log.Printf("[cluster] rebalancing %d keys to %s (from %s)",
		len(affectedKeys), shortID(nodeID), shortID(m.selfNode.ID))
	m.rebalancer.StartRebalance(m.selfNode.ID, nodeID, affectedKeys)
}

func (m *Manager) collectAffectedKeys(nodeID string) []string {
	allKeys, err := m.localCache.Keys()
	if err != nil {
		log.Printf("[cluster] failed to list local keys: %v", err)
		return nil
	}
	var keys []string
	for _, key := range allKeys {
		if m.ring.GetNode(key) == nodeID {
			keys = append(keys, key)
		}
	}
	return keys
}

func (m *Manager) RemoveNode(nodeID string) error {
	node, ok := m.topology.GetNode(nodeID)
	if !ok {
		return fmt.Errorf("node %s not found", nodeID)
	}

	if node.GetHealth() == HealthDead {
		return m.removeDeadNode(nodeID)
	}
	return m.removeGraceful(nodeID)
}

func (m *Manager) removeDeadNode(nodeID string) error {
	m.rebalMu.Lock()
	defer m.rebalMu.Unlock()

	m.ring.RemoveNode(nodeID)
	_ = m.topology.RemoveNode(nodeID)
	log.Printf("[cluster] dead node %s removed from ring and topology", shortID(nodeID))
	return nil
}

// handleNodeDead is the OnNodeDead callback. If the dead node was a primary
// with a ReplicaSet, it triggers promotion constrained to the ring's structural
// successors (option (a)), then replaces the dead node in the ring so the
// promoted node takes over its virtual-node positions. If the dead node had
// no ReplicaSet, it falls through to the plain removeDeadNode path.
func (m *Manager) handleNodeDead(nodeID string) {
	rs, hasReplicas := m.registry.GetReplicaSet(nodeID)

	if !hasReplicas {
		_ = m.RemoveNode(nodeID)
		return
	}

	m.rebalMu.Lock()
	defer m.rebalMu.Unlock()

	// --- Failover: promote best ring-successor replica ---
	succ := m.ring.SuccessorAfterRemoval(nodeID)

	promo, _ := m.registry.GetPromotion(nodeID)
	var promotedNode string
	var promoErr error
	if promo != nil && succ != "" {
		promotedNode, promoErr = promo.PromoteBestReplicaFrom(succ)
	}
	if promotedNode == "" && promo != nil {
		// Fallback: no ring-successor match, promote lowest-lag overall
		// and accept the routing mismatch (degraded mode).
		promotedNode, promoErr = promo.PromoteBestReplica()
	}

	if promotedNode != "" && promoErr == nil {
		log.Printf("[failover] promoting %s to primary (was ring-successor=%v for dead %s)",
			shortID(promotedNode), succ == promotedNode, shortID(nodeID))
		// Remove promoted node from old primary's ReplicaSet.
		rs.RemoveReplica(promotedNode)
		// Replace dead primary in ring with the promoted node.
		m.ring.ReplaceNode(nodeID, promotedNode)
		// Update topology roles.
		m.topology.SetNodeRole(promotedNode, RoleLeader)
	} else {
		log.Printf("[failover] no replica available for promotion of %s: %v", shortID(nodeID), promoErr)
		m.ring.RemoveNode(nodeID)
	}

	_ = m.topology.RemoveNode(nodeID)
	m.registry.Unregister(nodeID)
	log.Printf("[failover] dead primary %s failover complete", shortID(nodeID))
}

func (m *Manager) removeGraceful(nodeID string) error {
	m.rebalMu.Lock()

	affectedKeys := m.collectAffectedKeys(nodeID)

	m.ring.RemoveNode(nodeID)
	m.topology.SetNodeHealth(nodeID, HealthLeft)
	log.Printf("[cluster] graceful leave: node %s removed from ring, rebalancing %d keys",
		shortID(nodeID), len(affectedKeys))
	m.rebalMu.Unlock()

	if len(affectedKeys) == 0 {
		_ = m.topology.RemoveNode(nodeID)
		return nil
	}

	newOwner := m.ring.GetNode(affectedKeys[0])
	if newOwner == "" || newOwner == nodeID {
		log.Printf("[cluster] no new owner found for rebalanced keys from %s", shortID(nodeID))
		_ = m.topology.RemoveNode(nodeID)
		return nil
	}

	status := m.rebalancer.StartRebalance(nodeID, newOwner, affectedKeys)

	select {
	case <-status.Done():
		log.Printf("[cluster] graceful leave rebalance complete for %s: %d/%d keys migrated",
			shortID(nodeID), status.GetMigratedKeys(), status.TotalKeys)
	case <-time.After(30 * time.Second):
		log.Printf("[cluster] WARNING: graceful leave rebalance timed out for %s: %d/%d keys migrated",
			shortID(nodeID), status.GetMigratedKeys(), status.TotalKeys)
	}

	_ = m.topology.RemoveNode(nodeID)
	return nil
}

// migrateKeys is the batch migration callback invoked by the Rebalancer.
// It opens a single network.Client to the target, migrates all keys, and
// closes the connection — amortizing TCP handshake cost across all keys.
func (m *Manager) migrateKeys(keys []string, targetNode string) (int, error) {
	node, ok := m.topology.GetNode(targetNode)
	if !ok {
		return 0, fmt.Errorf("target node %s not found in topology", targetNode)
	}

	client := network.NewClient(node.Address)
	if err := client.Connect(); err != nil {
		return 0, fmt.Errorf("failed to connect to %s at %s: %w", shortID(targetNode), node.Address, err)
	}
	defer client.Close()

	migrated := 0
	for _, key := range keys {
		if err := m.migrateSingleKey(key, client); err != nil {
			log.Printf("[migrate] key=%s → %s failed: %v", key, shortID(targetNode), err)
			continue
		}
		migrated++
	}
	return migrated, nil
}

// migrateSingleKey reads a key's value and TTL from the local cache, sends
// a SET to the target via the provided client, and on success deletes the
// key locally. Returns an error on any failure (the caller skips and continues).
func (m *Manager) migrateSingleKey(key string, client *network.Client) error {
	value, err := m.localCache.Get(key)
	if err != nil {
		return fmt.Errorf("read failed: %w", err)
	}

	ttl, err := m.localCache.TTL(key)
	if err != nil {
		return fmt.Errorf("TTL read failed: %w", err)
	}

	args := []interface{}{"SET", key, string(value)}
	if ttl > 0 {
		remainingMs := ttl.Milliseconds()
		if remainingMs < 1 {
			remainingMs = 1
		}
		args = append(args, "PX", fmt.Sprintf("%d", remainingMs))
	}

	resp, err := client.Send(args...)
	if err != nil {
		return fmt.Errorf("SET failed: %w", err)
	}
	if resp != "OK" {
		return fmt.Errorf("unexpected SET response: %s", resp)
	}

	if _, err := m.localCache.Delete(key); err != nil {
		log.Printf("[migrate] warning: failed to delete local key %s: %v", key, err)
	}

	return nil
}

func (m *Manager) PromoteReplica(replicaID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node, ok := m.topology.GetNode(replicaID)
	if !ok {
		return fmt.Errorf("node %s not found", replicaID)
	}
	if !node.IsReplica() {
		return fmt.Errorf("node %s is not a replica", replicaID)
	}

	m.topology.SetNodeRole(replicaID, RoleLeader)
	log.Printf("[cluster] promoted %s to leader", shortID(replicaID))
	return nil
}

func (m *Manager) Self() *Node {
	return m.selfNode
}

func (m *Manager) Topology() *Topology {
	return m.topology
}

func (m *Manager) Ring() *hashring.HashRing {
	return m.ring
}

func (m *Manager) Registry() *replication.ReplicaRegistry {
	return m.registry
}

// initiateReplicaSync pushes the primary's state to a newly-added replica
// and transitions it to ReplicaActive on success. Runs in a goroutine with
// bounded retry and exponential backoff. Respects the manager's stop channel
// for graceful shutdown.
//
// An in-flight guard (syncing map) prevents concurrent or duplicate sync
// attempts for the same replica — e.g. one from a retry backoff and another
// from a fresh gossip-triggered AddNode.
func (m *Manager) initiateReplicaSync(primaryID, replicaID, replicaAddr string) {
	defer m.wg.Done()

	// Check and claim the in-flight slot. If another goroutine is already
	// syncing this replica, exit immediately.
	m.syncMu.Lock()
	if _, inflight := m.syncing[replicaID]; inflight {
		m.syncMu.Unlock()
		log.Printf("[sync] replica %s already syncing — skipping duplicate", shortID(replicaID))
		return
	}
	m.syncing[replicaID] = struct{}{}
	m.syncMu.Unlock()
	defer func() {
		m.syncMu.Lock()
		delete(m.syncing, replicaID)
		m.syncMu.Unlock()
	}()

	const maxRetries = 3
	backoffs := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-m.stopCh:
			return
		default:
		}

		if err := m.doReplicaSync(primaryID, replicaID, replicaAddr); err != nil {
			log.Printf("[sync] attempt %d/%d failed: primary=%s replica=%s err=%v",
				attempt+1, maxRetries, shortID(primaryID), shortID(replicaID), err)
			select {
			case <-m.stopCh:
				return
			case <-time.After(backoffs[attempt]):
			}
			continue
		}

		// Sync succeeded — mark replica as Active.
		if rs, ok := m.registry.GetReplicaSet(primaryID); ok {
			rs.SetStatus(replicaID, replication.ReplicaActive)
			log.Printf("[sync] replica %s synced to primary %s — now active",
				shortID(replicaID), shortID(primaryID))
		}
		return
	}

	// All retries exhausted — leave as ReplicaSyncing (not Failed),
	// so a future topology event or manual re-add can retry.
	log.Printf("[sync] all %d retries exhausted for replica %s on primary %s — staying syncing",
		maxRetries, shortID(replicaID), shortID(primaryID))
}

// doReplicaSync performs a single sync attempt: pushes the primary's
// ReplicationStream (gap ops) or full cache snapshot to the replica.
func (m *Manager) doReplicaSync(primaryID, replicaID, replicaAddr string) error {
	rs, ok := m.registry.GetReplicaSet(primaryID)
	if !ok {
		return fmt.Errorf("no replica set for primary %s", primaryID)
	}

	primaryInfo, ok := rs.GetReplica(primaryID)
	if !ok || primaryInfo == nil || primaryInfo.Stream == nil {
		return fmt.Errorf("no replication stream for primary %s", primaryID)
	}

	stream := primaryInfo.Stream
	syncStartSeq := stream.LatestSeq()

	// Decide: gap sync (ops fit in buffer) or full sync (buffer truncated).
	bufStart := stream.BufferStartSeq()
	if bufStart > 0 {
		// Buffer doesn't cover from seq 0 — gap exceeds retention.
		return m.doFullSync(replicaAddr, primaryID, syncStartSeq)
	}

	ops := stream.GetSince(0)
	if len(ops) == 0 && syncStartSeq == 0 {
		// Brand new primary with no data — nothing to sync.
		return nil
	}

	return m.pushGapOps(replicaAddr, ops, syncStartSeq)
}

// pushGapOps sends historical replication operations to the replica via
// REPLICATE commands. After the initial batch, it catches up any ops that
// arrived during the push.
func (m *Manager) pushGapOps(replicaAddr string, ops []replication.Operation, syncStartSeq int64) error {
	client := network.NewClient(replicaAddr)
	if err := client.Connect(); err != nil {
		return fmt.Errorf("connect to replica failed: %w", err)
	}
	defer client.Close()

	for _, op := range ops {
		if err := sendReplicateOp(client, op); err != nil {
			return fmt.Errorf("push op seq=%d failed: %w", op.Seq, err)
		}
	}

	// Catch up any ops that arrived during the push.
	if rs, ok := m.registry.GetReplicaSet(m.selfNode.ID); ok {
		if pi, ok := rs.GetReplica(m.selfNode.ID); ok && pi.Stream != nil {
			remaining := pi.Stream.GetSince(syncStartSeq)
			for _, op := range remaining {
				if err := sendReplicateOp(client, op); err != nil {
					return fmt.Errorf("catch-up op seq=%d failed: %w", op.Seq, err)
				}
			}
		}
	}

	return nil
}

// doFullSync transfers the primary's entire cache snapshot to the replica
// using FLUSHALL + SET commands, then replays any ops that arrived during
// the transfer.
func (m *Manager) doFullSync(replicaAddr, primaryID string, syncStartSeq int64) error {
	lc, ok := m.localCache.(*cache.LocalCache)
	if !ok {
		return fmt.Errorf("local cache does not support Snapshot()")
	}

	snapshot := lc.Snapshot()

	client := network.NewClient(replicaAddr)
	if err := client.Connect(); err != nil {
		return fmt.Errorf("connect to replica for full sync failed: %w", err)
	}
	defer client.Close()

	// Clear replica state.
	if _, err := client.Send("FLUSHALL"); err != nil {
		return fmt.Errorf("FLUSHALL failed: %w", err)
	}

	// Push every entry.
	now := time.Now().UnixNano()
	for _, item := range snapshot {
		args := []interface{}{"SET", item.Key, string(item.Value)}
		if item.ExpiresAt > 0 {
			remainingMs := (item.ExpiresAt - now) / int64(time.Millisecond)
			if remainingMs > 0 {
				args = append(args, "PX", fmt.Sprintf("%d", remainingMs))
			}
		}
		if _, err := client.Send(args...); err != nil {
			return fmt.Errorf("SET %q failed: %w", item.Key, err)
		}
	}

	// Replay any ops that arrived during the snapshot push.
	if rs, ok := m.registry.GetReplicaSet(primaryID); ok {
		if pi, ok := rs.GetReplica(primaryID); ok && pi.Stream != nil {
			remaining := pi.Stream.GetSince(syncStartSeq)
			for _, op := range remaining {
				if err := sendReplicateOp(client, op); err != nil {
					return fmt.Errorf("post-snapshot op seq=%d failed: %w", op.Seq, err)
				}
			}
		}
	}

	return nil
}

// sendReplicateOp sends a single REPLICATE command to a replica.
func sendReplicateOp(client *network.Client, op replication.Operation) error {
	payload, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := client.Send("REPLICATE", string(payload))
	if err != nil {
		return err
	}
	if resp != "OK" {
		return fmt.Errorf("unexpected response: %s", resp)
	}
	return nil
}

func (m *Manager) Shutdown() {
	close(m.stopCh)
	m.transport.Stop()
	m.detector.Stop()
	m.gossip.Stop()
	m.wg.Wait()
}

func (m *Manager) IsLeader() bool {
	return m.selfNode.IsLeader()
}

func (m *Manager) GetLeaderNode() *Node {
	return m.topology.GetLeader()
}

func (m *Manager) String() string {
	return fmt.Sprintf("Manager[node=%s role=%s]", shortID(m.selfNode.ID), m.selfNode.GetRole())
}

func (m *Manager) periodicStatus() {
	defer m.wg.Done()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			alive := m.topology.AliveCount()
			dead := len(m.topology.DeadNodes())
			log.Printf("[cluster] status: %d alive, %d dead, epoch=%d",
				alive, dead, m.topology.Epoch())
		}
	}
}
