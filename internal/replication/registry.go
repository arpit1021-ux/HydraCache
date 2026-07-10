package replication

import "sync"

// ReplicaRegistry maps primary node IDs to their ReplicaSets and Promotions.
// It is the single source of truth for "which primary owns which replicas"
// and is used during failover to look up the ReplicaSet for a dead primary.
type ReplicaRegistry struct {
	mu          sync.RWMutex
	replicaSets map[string]*ReplicaSet // primaryNodeID → ReplicaSet
	promotions  map[string]*Promotion  // primaryNodeID → Promotion
}

func NewReplicaRegistry() *ReplicaRegistry {
	return &ReplicaRegistry{
		replicaSets: make(map[string]*ReplicaSet),
		promotions:  make(map[string]*Promotion),
	}
}

func (rr *ReplicaRegistry) Register(primaryNodeID string, rs *ReplicaSet) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	rr.replicaSets[primaryNodeID] = rs
	rr.promotions[primaryNodeID] = NewPromotion(rs)
}

func (rr *ReplicaRegistry) Unregister(primaryNodeID string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	delete(rr.replicaSets, primaryNodeID)
	delete(rr.promotions, primaryNodeID)
}

func (rr *ReplicaRegistry) GetReplicaSet(primaryNodeID string) (*ReplicaSet, bool) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	rs, ok := rr.replicaSets[primaryNodeID]
	return rs, ok
}

func (rr *ReplicaRegistry) GetPromotion(primaryNodeID string) (*Promotion, bool) {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	p, ok := rr.promotions[primaryNodeID]
	return p, ok
}

// FindPrimaryForReplica returns the primary node ID that owns the given replica,
// or "" if no primary claims this replica.
func (rr *ReplicaRegistry) FindPrimaryForReplica(replicaNodeID string) string {
	rr.mu.RLock()
	defer rr.mu.RUnlock()
	for primaryID, rs := range rr.replicaSets {
		if _, ok := rs.GetReplica(replicaNodeID); ok {
			return primaryID
		}
	}
	return ""
}
