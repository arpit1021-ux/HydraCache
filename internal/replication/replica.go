package replication

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

type ReplicaSet struct {
	mu         sync.RWMutex
	primaryID  string
	replicas   map[string]*ReplicaInfo
	lagTracker *LagTracker
}

type ReplicaInfo struct {
	NodeID   string
	Address  string
	Status   ReplicaStatus
	LagSeq   int64
	LastSync time.Time
	Stream   *ReplicationStream
}

type ReplicaStatus int

const (
	ReplicaSyncing ReplicaStatus = iota
	ReplicaActive
	ReplicaLagging
	ReplicaFailed
)

func (s ReplicaStatus) String() string {
	switch s {
	case ReplicaSyncing:
		return "syncing"
	case ReplicaActive:
		return "active"
	case ReplicaLagging:
		return "lagging"
	case ReplicaFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func NewReplicaSet(primaryID string) *ReplicaSet {
	return &ReplicaSet{
		primaryID:  primaryID,
		replicas:   make(map[string]*ReplicaInfo),
		lagTracker: NewLagTracker(),
	}
}

func (rs *ReplicaSet) AddReplica(nodeID, address string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.replicas[nodeID] = &ReplicaInfo{
		NodeID:  nodeID,
		Address: address,
		Status:  ReplicaSyncing,
		Stream:  NewReplicationStream(10000),
	}
	log.Printf("[replication] added replica %s to primary %s", shortID(nodeID), shortID(rs.primaryID))
}

func (rs *ReplicaSet) RemoveReplica(nodeID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.replicas, nodeID)
}

func (rs *ReplicaSet) GetReplica(nodeID string) (*ReplicaInfo, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	r, ok := rs.replicas[nodeID]
	return r, ok
}

func (rs *ReplicaSet) ActiveReplicas() []*ReplicaInfo {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	var active []*ReplicaInfo
	for _, r := range rs.replicas {
		if r.Status == ReplicaActive {
			active = append(active, r)
		}
	}
	return active
}

func (rs *ReplicaSet) BestReplica() *ReplicaInfo {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	var best *ReplicaInfo
	for _, r := range rs.replicas {
		if r.Status == ReplicaFailed {
			continue
		}
		if best == nil || r.LagSeq < best.LagSeq {
			best = r
		}
	}
	return best
}

func (rs *ReplicaSet) UpdateLag(nodeID string, lag int64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if r, ok := rs.replicas[nodeID]; ok {
		r.LagSeq = lag
		r.LastSync = time.Now()
		rs.lagTracker.Record(nodeID, lag)

		if lag > 100 {
			r.Status = ReplicaLagging
		} else if r.Status == ReplicaLagging {
			r.Status = ReplicaActive
		}
	}
}

func (rs *ReplicaSet) ReplicaCount() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.replicas)
}

func (rs *ReplicaSet) LagInfo() map[string]int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	lags := make(map[string]int64, len(rs.replicas))
	for id, r := range rs.replicas {
		lags[id] = atomic.LoadInt64(&r.LagSeq)
	}
	return lags
}
