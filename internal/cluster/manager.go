package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

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
	mu         sync.RWMutex
	rebalMu    sync.Mutex
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func NewManager(selfNode *Node, topo *Topology, ring *hashring.HashRing) *Manager {
	m := &Manager{
		topology: topo,
		selfNode: selfNode,
		ring:     ring,
		registry: replication.NewReplicaRegistry(),
		stopCh:   make(chan struct{}),
	}
	m.rebalancer = hashring.NewRebalancer(ring, m.migrateKey)
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
	for _, n := range m.topology.AliveNodes() {
		if n.ID != node.ID {
			rs.AddReplica(n.ID, n.Address)
		}
	}
	m.registry.Register(node.ID, rs)

	// Also add the new node as a replica to all existing primaries,
	// so their replicateWrite can fan out to this new node.
	for _, n := range m.topology.AliveNodes() {
		if n.ID == node.ID {
			continue
		}
		if existingRS, ok := m.registry.GetReplicaSet(n.ID); ok {
			existingRS.AddReplica(node.ID, node.Address)
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

	prevNode := m.ring.GetNode(affectedKeys[0])
	if prevNode == "" || prevNode == nodeID {
		return
	}

	log.Printf("[cluster] rebalancing %d keys to %s (from %s)",
		len(affectedKeys), shortID(nodeID), shortID(prevNode))
	m.rebalancer.StartRebalance(prevNode, nodeID, affectedKeys)
}

func (m *Manager) collectAffectedKeys(nodeID string) []string {
	var keys []string
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("__rebalance:%s:%d", nodeID, i)
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

func (m *Manager) migrateKey(key, targetNode string) error {
	log.Printf("[migrate] key=%s → %s (no-op, data transfer not yet implemented)", key, shortID(targetNode))
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
