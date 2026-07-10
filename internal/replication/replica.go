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
	status   atomic.Int32
	lagSeq   atomic.Int64
	lastSync atomic.Int64 // UnixNano
	Stream   *ReplicationStream
}

func (r *ReplicaInfo) GetStatus() ReplicaStatus  { return ReplicaStatus(r.status.Load()) }
func (r *ReplicaInfo) SetStatus(s ReplicaStatus) { r.status.Store(int32(s)) }
func (r *ReplicaInfo) GetLagSeq() int64          { return r.lagSeq.Load() }
func (r *ReplicaInfo) SetLagSeq(v int64)         { r.lagSeq.Store(v) }
func (r *ReplicaInfo) GetLastSync() time.Time    { return time.Unix(0, r.lastSync.Load()) }
func (r *ReplicaInfo) SetLastSync(t time.Time)   { r.lastSync.Store(t.UnixNano()) }

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
	info := &ReplicaInfo{
		NodeID:  nodeID,
		Address: address,
		Stream:  NewReplicationStream(10000),
	}
	info.SetStatus(ReplicaSyncing)
	rs.replicas[nodeID] = info
	log.Printf("[replication] added replica %s to primary %s", shortID(nodeID), shortID(rs.primaryID))
}

func (rs *ReplicaSet) RemoveReplica(nodeID string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.replicas, nodeID)
}

// SetStatus transitions a replica's status through the ReplicaSet's own mutex,
// ensuring no cross-lock races with UpdateLag or external callers.
func (rs *ReplicaSet) SetStatus(nodeID string, status ReplicaStatus) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if r, ok := rs.replicas[nodeID]; ok {
		r.SetStatus(status)
	}
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
		if r.GetStatus() == ReplicaActive {
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
		if r.GetStatus() == ReplicaFailed {
			continue
		}
		if best == nil || r.GetLagSeq() < best.GetLagSeq() {
			best = r
		}
	}
	return best
}

// BestReplicaFrom selects the lowest-lag non-failed replica whose NodeID
// matches ringSuccessor. Returns nil if no such replica exists in the set.
func (rs *ReplicaSet) BestReplicaFrom(ringSuccessor string) *ReplicaInfo {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	r, ok := rs.replicas[ringSuccessor]
	if !ok || r.GetStatus() == ReplicaFailed {
		return nil
	}
	return r
}

func (rs *ReplicaSet) UpdateLag(nodeID string, lag int64) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if r, ok := rs.replicas[nodeID]; ok {
		r.SetLagSeq(lag)
		r.SetLastSync(time.Now())
		rs.lagTracker.Record(nodeID, lag)

		if lag > 100 {
			r.SetStatus(ReplicaLagging)
		} else if r.GetStatus() == ReplicaLagging {
			r.SetStatus(ReplicaActive)
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
		lags[id] = r.GetLagSeq()
	}
	return lags
}
