package cluster

import (
	"fmt"
	"time"
)

type Role int

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

type Health int

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

func NewNode(id, address string) *Node {
	return &Node{
		ID:       id,
		Address:  address,
		Role:     RolePeer,
		Health:   HealthAlive,
		Version:  "1.0.0",
		JoinedAt: time.Now(),
		LastSeen: time.Now(),
	}
}

func (n *Node) String() string {
	return fmt.Sprintf("%s@%s [%s/%s]", shortID(n.ID), n.Address, n.Role, n.Health)
}

func (n *Node) IsAlive() bool {
	return n.Health == HealthAlive
}

func (n *Node) IsLeader() bool {
	return n.Role == RoleLeader
}

func (n *Node) IsReplica() bool {
	return n.Role == RoleReplica
}
