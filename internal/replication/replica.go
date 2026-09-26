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
	mu           sync.RWMutex
	primaryID    string
	replicas     map[string]*ReplicaInfo
	lagTracker   *LagTracker
	maxSeenEpoch atomic.Uint64

	// applyMu serializes gap-detection + catch-up + apply for this shard
	// on a receiving replica, so two concurrently-arriving REPLICATE ops
	// for the same shard (each op opens its own connection, so the wire
	// gives no ordering guarantee) can never race on lastAppliedSeq or
	// trigger duplicate concurrent catch-ups.
	applyMu        sync.Mutex
	lastAppliedSeq atomic.Int64
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

// ReplicaStatus is int32, not int, specifically so storing it in the
// atomic.Int32 above (status) is a same-width conversion, not a
// narrowing one — matching the same pattern internal/cluster/node.go's
// Role and Health types already use for their own atomic-backed enums.
type ReplicaStatus int32

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
	// Status defaults to ReplicaSyncing (iota 0). The caller is
	// responsible for transitioning to ReplicaActive once the sync
	// handshake completes — or immediately for entries that don't
	// need catch-up (e.g. the primary's own entry).
	rs.replicas[nodeID] = info
	log.Printf("[replication] added replica %s to primary %s (status=syncing)", shortID(nodeID), shortID(rs.primaryID))
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
		switch r.GetStatus() {
		case ReplicaFailed, ReplicaSyncing:
			continue
		}
		if best == nil || r.GetLagSeq() < best.GetLagSeq() {
			best = r
		}
	}
	return best
}

// BestReplicaFrom selects the lowest-lag non-failed, non-syncing replica
// whose NodeID matches ringSuccessor. Returns nil if no such replica exists.
func (rs *ReplicaSet) BestReplicaFrom(ringSuccessor string) *ReplicaInfo {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	r, ok := rs.replicas[ringSuccessor]
	if !ok {
		return nil
	}
	switch r.GetStatus() {
	case ReplicaFailed, ReplicaSyncing:
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

// CheckAndAdvanceEpoch validates an incoming write's fencing epoch against
// the highest epoch this shard has seen so far. It returns true (accept)
// when epoch is at or ahead of the current watermark, atomically advancing
// it; false (reject as stale) when epoch is behind — meaning the sender
// last knew about this shard's ownership before a subsequent
// membership/role change, i.e. it may no longer be the legitimate primary.
func (rs *ReplicaSet) CheckAndAdvanceEpoch(epoch uint64) bool {
	for {
		cur := rs.maxSeenEpoch.Load()
		if epoch < cur {
			return false
		}
		if rs.maxSeenEpoch.CompareAndSwap(cur, epoch) {
			return true
		}
	}
}

// LockApply acquires the shard's apply lock and returns the unlock
// function. Hold it across gap-detection, catch-up, and application of a
// single incoming REPLICATE op so concurrent arrivals for this shard are
// serialized end to end.
func (rs *ReplicaSet) LockApply() func() {
	rs.applyMu.Lock()
	return rs.applyMu.Unlock
}

// LastAppliedSeq returns the highest op sequence number this node has
// applied for this shard (0 if none yet, or if this node has never
// tracked sequencing for it).
func (rs *ReplicaSet) LastAppliedSeq() int64 {
	return rs.lastAppliedSeq.Load()
}

// SetLastAppliedSeq advances the applied-seq watermark. Monotonic: a
// lower or equal value is a no-op, so late-arriving stale updates can
// never move it backwards.
func (rs *ReplicaSet) SetLastAppliedSeq(seq int64) {
	for {
		cur := rs.lastAppliedSeq.Load()
		if seq <= cur {
			return
		}
		if rs.lastAppliedSeq.CompareAndSwap(cur, seq) {
			return
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
