package cluster

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

type Manager struct {
	topology *Topology
	selfNode *Node
	mu       sync.RWMutex
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewManager(selfNode *Node, topo *Topology) *Manager {
	return &Manager{
		topology: topo,
		selfNode: selfNode,
		stopCh:   make(chan struct{}),
	}
}

func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.topology.AddNode(m.selfNode); err != nil {
		return fmt.Errorf("failed to add self to topology: %w", err)
	}

	log.Printf("[cluster] node %s started at %s", shortID(m.selfNode.ID), m.selfNode.Address)
	return nil
}

func (m *Manager) Bootstrap(addresses []string) error {
	for _, addr := range addresses {
		log.Printf("[cluster] discovering node at %s", addr)
	}
	return nil
}

func (m *Manager) AddNode(node *Node) error {
	return m.topology.AddNode(node)
}

func (m *Manager) RemoveNode(nodeID string) error {
	return m.topology.RemoveNode(nodeID)
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

func (m *Manager) Shutdown() {
	close(m.stopCh)
	m.wg.Wait()
}

func (m *Manager) IsLeader() bool {
	return m.selfNode.IsLeader()
}

func (m *Manager) GetLeaderNode() *Node {
	return m.topology.GetLeader()
}

func (m *Manager) String() string {
	return fmt.Sprintf("Manager[node=%s role=%s]", shortID(m.selfNode.ID), m.selfNode.Role)
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
