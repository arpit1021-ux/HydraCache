package hashring

import (
	"log"
	"sync"
	"sync/atomic"
)

type RebalanceStatus struct {
	SourceNode   string `json:"source_node"`
	TargetNode   string `json:"target_node"`
	TotalKeys    int    `json:"total_keys"`
	migratedKeys int64
	complete     atomic.Bool
	done         chan struct{}
}

func (s *RebalanceStatus) IsComplete() bool       { return s.complete.Load() }
func (s *RebalanceStatus) MarkComplete()          { s.complete.Store(true) }
func (s *RebalanceStatus) GetMigratedKeys() int64 { return atomic.LoadInt64(&s.migratedKeys) }

// Done returns a channel that is closed when the rebalance finishes
// (successfully or not). Safe to select on from any goroutine.
func (s *RebalanceStatus) Done() <-chan struct{} { return s.done }

type Rebalancer struct {
	ring    *HashRing
	status  map[string]*RebalanceStatus
	mu      sync.RWMutex
	onBatch func(keys []string, targetNode string) (int, error)
}

func NewRebalancer(ring *HashRing, onBatch func(keys []string, targetNode string) (int, error)) *Rebalancer {
	return &Rebalancer{
		ring:    ring,
		status:  make(map[string]*RebalanceStatus),
		onBatch: onBatch,
	}
}

func (r *Rebalancer) StartRebalance(sourceNode, targetNode string, keys []string) *RebalanceStatus {
	r.mu.Lock()
	status := &RebalanceStatus{
		SourceNode: sourceNode,
		TargetNode: targetNode,
		TotalKeys:  len(keys),
		done:       make(chan struct{}),
	}
	r.status[sourceNode+":"+targetNode] = status
	r.mu.Unlock()

	go r.executeRebalance(status, keys)
	return status
}

func (r *Rebalancer) executeRebalance(status *RebalanceStatus, keys []string) {
	defer close(status.done)

	if r.onBatch != nil {
		migrated, err := r.onBatch(keys, status.TargetNode)
		if err != nil {
			sID := status.SourceNode
			if len(sID) > 8 {
				sID = sID[:8]
			}
			tID := status.TargetNode
			if len(tID) > 8 {
				tID = tID[:8]
			}
			log.Printf("[rebalance] migration batch failed for %s → %s: %v", sID, tID, err)
		}
		atomic.AddInt64(&status.migratedKeys, int64(migrated))
	}

	status.MarkComplete()
	sID := status.SourceNode
	if len(sID) > 8 {
		sID = sID[:8]
	}
	tID := status.TargetNode
	if len(tID) > 8 {
		tID = tID[:8]
	}
	log.Printf("[rebalance] completed: %s → %s (%d/%d keys)",
		sID, tID, status.GetMigratedKeys(), status.TotalKeys)
}

func (r *Rebalancer) GetStatus(source, target string) *RebalanceStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status[source+":"+target]
}

func (r *Rebalancer) GetAllStatuses() []*RebalanceStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	statuses := make([]*RebalanceStatus, 0, len(r.status))
	for _, s := range r.status {
		statuses = append(statuses, s)
	}
	return statuses
}
