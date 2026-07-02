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
	MigratedKeys int64  `json:"migrated_keys"`
	Complete     bool   `json:"complete"`
}

type Rebalancer struct {
	ring   *HashRing
	status map[string]*RebalanceStatus
	mu     sync.RWMutex
	onKey  func(key, targetNode string) error
}

func NewRebalancer(ring *HashRing, onKey func(key, targetNode string) error) *Rebalancer {
	return &Rebalancer{
		ring:   ring,
		status: make(map[string]*RebalanceStatus),
		onKey:  onKey,
	}
}

func (r *Rebalancer) StartRebalance(sourceNode, targetNode string, keys []string) *RebalanceStatus {
	r.mu.Lock()
	status := &RebalanceStatus{
		SourceNode: sourceNode,
		TargetNode: targetNode,
		TotalKeys:  len(keys),
	}
	r.status[sourceNode+":"+targetNode] = status
	r.mu.Unlock()

	go r.executeRebalance(status, keys)
	return status
}

func (r *Rebalancer) executeRebalance(status *RebalanceStatus, keys []string) {
	for _, key := range keys {
		if r.onKey != nil {
			if err := r.onKey(key, status.TargetNode); err != nil {
				log.Printf("[rebalance] failed to migrate key %s: %v", key, err)
				continue
			}
		}
		atomic.AddInt64(&status.MigratedKeys, 1)
	}
	status.Complete = true
	sID := status.SourceNode
	if len(sID) > 8 {
		sID = sID[:8]
	}
	tID := status.TargetNode
	if len(tID) > 8 {
		tID = tID[:8]
	}
	log.Printf("[rebalance] completed: %s → %s (%d keys)",
		sID, tID, status.TotalKeys)
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
