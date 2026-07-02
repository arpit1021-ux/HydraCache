package cluster

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type Topology struct {
	mu        sync.RWMutex
	nodes     map[string]*Node
	epoch     uint64
	updatedAt time.Time
	listeners []func(TopologyEvent)
}

type TopologyEvent struct {
	Type      string
	NodeID    string
	Epoch     uint64
	Timestamp time.Time
}

func NewTopology() *Topology {
	return &Topology{
		nodes:     make(map[string]*Node),
		updatedAt: time.Now(),
	}
}

func (t *Topology) AddNode(node *Node) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.nodes[node.ID]; exists {
		return fmt.Errorf("node %s already exists", node.ID)
	}
	t.nodes[node.ID] = node
	t.epoch++
	t.updatedAt = time.Now()
	t.notify(TopologyEvent{Type: "node_added", NodeID: node.ID, Epoch: t.epoch, Timestamp: t.updatedAt})
	return nil
}

func (t *Topology) RemoveNode(nodeID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.nodes[nodeID]; !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}
	delete(t.nodes, nodeID)
	t.epoch++
	t.updatedAt = time.Now()
	t.notify(TopologyEvent{Type: "node_removed", NodeID: nodeID, Epoch: t.epoch, Timestamp: t.updatedAt})
	return nil
}

func (t *Topology) GetNode(nodeID string) (*Node, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	node, ok := t.nodes[nodeID]
	return node, ok
}

func (t *Topology) AllNodes() []*Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	nodes := make([]*Node, 0, len(t.nodes))
	for _, n := range t.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (t *Topology) AliveNodes() []*Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var nodes []*Node
	for _, n := range t.nodes {
		if n.IsAlive() {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (t *Topology) DeadNodes() []*Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var nodes []*Node
	for _, n := range t.nodes {
		if n.Health == HealthDead {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func (t *Topology) GetLeader() *Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, n := range t.nodes {
		if n.IsLeader() && n.IsAlive() {
			return n
		}
	}
	return nil
}

func (t *Topology) GetReplicas() []*Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var replicas []*Node
	for _, n := range t.nodes {
		if n.IsReplica() && n.IsAlive() {
			replicas = append(replicas, n)
		}
	}
	return replicas
}

func (t *Topology) NodeCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.nodes)
}

func (t *Topology) AliveCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	count := 0
	for _, n := range t.nodes {
		if n.IsAlive() {
			count++
		}
	}
	return count
}

func (t *Topology) Epoch() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.epoch
}

func (t *Topology) SetNodeHealth(nodeID string, health Health) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if node, ok := t.nodes[nodeID]; ok {
		node.Health = health
		node.LastSeen = time.Now()
		t.epoch++
		t.notify(TopologyEvent{Type: "health_changed", NodeID: nodeID, Epoch: t.epoch, Timestamp: time.Now()})
	}
}

func (t *Topology) SetNodeRole(nodeID string, role Role) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if node, ok := t.nodes[nodeID]; ok {
		node.Role = role
		t.epoch++
	}
}

func (t *Topology) OnChange(fn func(TopologyEvent)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.listeners = append(t.listeners, fn)
}

func (t *Topology) notify(event TopologyEvent) {
	for _, fn := range t.listeners {
		go fn(event)
	}
}

func (t *Topology) MarshalJSON() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return json.Marshal(struct {
		Nodes     []*Node   `json:"nodes"`
		Epoch     uint64    `json:"epoch"`
		UpdatedAt time.Time `json:"updated_at"`
	}{
		Nodes:     t.AllNodes(),
		Epoch:     t.epoch,
		UpdatedAt: t.updatedAt,
	})
}
