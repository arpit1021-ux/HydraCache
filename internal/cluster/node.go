package cluster

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

type Role int32

const (
	RolePeer Role = iota
	RoleLeader
	RoleReplica
)

func (r Role) String() string {
	switch r {
	case RoleLeader:
		return "leader"
	case RoleReplica:
		return "replica"
	default:
		return "peer"
	}
}

type Health int32

const (
	HealthAlive Health = iota
	HealthSuspect
	HealthDead
	HealthLeft
)

func (h Health) String() string {
	switch h {
	case HealthAlive:
		return "alive"
	case HealthSuspect:
		return "suspect"
	case HealthDead:
		return "dead"
	case HealthLeft:
		return "left"
	default:
		return "unknown"
	}
}

type Node struct {
	ID          string `json:"id"`
	Address     string `json:"address"`
	role        atomic.Int32
	health      atomic.Int32
	Region      string    `json:"region"`
	Version     string    `json:"version"`
	Load        float64   `json:"load"`
	MemoryMB    int64     `json:"memory_mb"`
	LastSeen    time.Time `json:"last_seen"`
	JoinedAt    time.Time `json:"joined_at"`
	Epoch       uint64    `json:"epoch"`
	IsReplicaOf string    `json:"is_replica_of,omitempty"`
}

func NewNode(id, address string) *Node {
	n := &Node{
		ID:       id,
		Address:  address,
		Version:  "1.0.0",
		JoinedAt: time.Now(),
		LastSeen: time.Now(),
	}
	n.role.Store(int32(RolePeer))
	n.health.Store(int32(HealthAlive))
	return n
}

func (n *Node) GetHealth() Health  { return Health(n.health.Load()) }
func (n *Node) SetHealth(h Health) { n.health.Store(int32(h)) }
func (n *Node) GetRole() Role      { return Role(n.role.Load()) }
func (n *Node) SetRole(r Role)     { n.role.Store(int32(r)) }

func (n *Node) String() string {
	return fmt.Sprintf("%s@%s [%s/%s]", shortID(n.ID), n.Address, n.GetRole(), n.GetHealth())
}

func (n *Node) IsAlive() bool {
	return n.GetHealth() == HealthAlive
}

func (n *Node) IsLeader() bool {
	return n.GetRole() == RoleLeader
}

func (n *Node) IsReplica() bool {
	return n.GetRole() == RoleReplica
}

func (n *Node) MarshalJSON() ([]byte, error) {
	type nodeAlias struct {
		ID          string    `json:"id"`
		Address     string    `json:"address"`
		Role        Role      `json:"role"`
		Health      Health    `json:"health"`
		Region      string    `json:"region"`
		Version     string    `json:"version"`
		Load        float64   `json:"load"`
		MemoryMB    int64     `json:"memory_mb"`
		LastSeen    time.Time `json:"last_seen"`
		JoinedAt    time.Time `json:"joined_at"`
		Epoch       uint64    `json:"epoch"`
		IsReplicaOf string    `json:"is_replica_of,omitempty"`
	}
	return json.Marshal(nodeAlias{
		ID:          n.ID,
		Address:     n.Address,
		Role:        n.GetRole(),
		Health:      n.GetHealth(),
		Region:      n.Region,
		Version:     n.Version,
		Load:        n.Load,
		MemoryMB:    n.MemoryMB,
		LastSeen:    n.LastSeen,
		JoinedAt:    n.JoinedAt,
		Epoch:       n.Epoch,
		IsReplicaOf: n.IsReplicaOf,
	})
}

func (n *Node) UnmarshalJSON(data []byte) error {
	type nodeAlias struct {
		ID          string    `json:"id"`
		Address     string    `json:"address"`
		Role        Role      `json:"role"`
		Health      Health    `json:"health"`
		Region      string    `json:"region"`
		Version     string    `json:"version"`
		Load        float64   `json:"load"`
		MemoryMB    int64     `json:"memory_mb"`
		LastSeen    time.Time `json:"last_seen"`
		JoinedAt    time.Time `json:"joined_at"`
		Epoch       uint64    `json:"epoch"`
		IsReplicaOf string    `json:"is_replica_of,omitempty"`
	}
	var alias nodeAlias
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	n.ID = alias.ID
	n.Address = alias.Address
	n.SetRole(alias.Role)
	n.SetHealth(alias.Health)
	n.Region = alias.Region
	n.Version = alias.Version
	n.Load = alias.Load
	n.MemoryMB = alias.MemoryMB
	n.LastSeen = alias.LastSeen
	n.JoinedAt = alias.JoinedAt
	n.Epoch = alias.Epoch
	n.IsReplicaOf = alias.IsReplicaOf
	return nil
}
