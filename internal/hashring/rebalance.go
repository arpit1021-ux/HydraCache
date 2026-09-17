package hashring

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// redirectTTL bounds how long a "key moved to X" redirect is honored and,
// via the periodic purge in MarkKeyMigrated, how long it's retained at
// all — without this, `outgoing` would grow for the lifetime of the
// process, one entry per key ever migrated away.
const redirectTTL = 5 * time.Minute

type redirectEntry struct {
	target    string
	expiresAt time.Time
}

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

	// outgoing tracks, per key, the node a key has already been migrated
	// TO from this node. A request for that key arriving here afterward
	// (e.g. a client whose routing view hasn't caught up with the new
	// topology yet) can be redirected to the real owner instead of
	// returning a false miss for data this node no longer holds. Entries
	// expire after redirectTTL — see MarkKeyMigrated.
	outgoing  map[string]redirectEntry
	markCount uint64
}

func NewRebalancer(ring *HashRing, onBatch func(keys []string, targetNode string) (int, error)) *Rebalancer {
	return &Rebalancer{
		ring:     ring,
		status:   make(map[string]*RebalanceStatus),
		outgoing: make(map[string]redirectEntry),
		onBatch:  onBatch,
	}
}

// MarkKeyMigrated records that key has been successfully moved to
// targetNode, so a subsequent local lookup for it can be redirected for
// up to redirectTTL. Periodically sweeps expired entries so `outgoing`
// doesn't grow for the lifetime of the process.
func (r *Rebalancer) MarkKeyMigrated(key, targetNode string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outgoing[key] = redirectEntry{target: targetNode, expiresAt: time.Now().Add(redirectTTL)}
	r.markCount++
	if r.markCount%1000 == 0 {
		r.purgeExpiredLocked()
	}
}

func (r *Rebalancer) purgeExpiredLocked() {
	now := time.Now()
	for k, v := range r.outgoing {
		if now.After(v.expiresAt) {
			delete(r.outgoing, k)
		}
	}
}

// RedirectTarget reports the node a key was migrated away to, if the
// redirect is still within its TTL.
func (r *Rebalancer) RedirectTarget(key string) (string, bool) {
	r.mu.RLock()
	entry, ok := r.outgoing[key]
	r.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		return "", false
	}
	return entry.target, true
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
