package replication

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type Promotion struct {
	mu              sync.Mutex
	replicaSet      *ReplicaSet
	promoted        bool
	promotedNode    string
	lossyPromotions atomic.Int64
}

func NewPromotion(rs *ReplicaSet) *Promotion {
	return &Promotion{
		replicaSet: rs,
	}
}

func (p *Promotion) PromoteBestReplica() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	best := p.replicaSet.BestReplica()
	if best == nil {
		return "", ErrNoReplicaAvailable
	}

	p.replicaSet.SetStatus(best.NodeID, ReplicaActive)
	p.promoted = true
	p.promotedNode = best.NodeID

	log.Printf("[promotion] promoted replica %s to primary (lag=%d)", shortID(best.NodeID), best.GetLagSeq())
	return best.NodeID, nil
}

// PromoteBestReplicaFrom selects the best replica ONLY from the given
// ring-successor candidate set (the nodes the ring would naturally route to
// after the dead primary is removed). This guarantees the promoted node
// matches the ring's structural routing, avoiding a split-brain where
// replication bookkeeping and client routing disagree.
func (p *Promotion) PromoteBestReplicaFrom(ringSuccessor string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	best := p.replicaSet.BestReplicaFrom(ringSuccessor)
	if best == nil {
		return "", ErrNoReplicaAvailable
	}

	p.replicaSet.SetStatus(best.NodeID, ReplicaActive)
	p.promoted = true
	p.promotedNode = best.NodeID

	log.Printf("[promotion] promoted replica %s to primary (lag=%d, ring-successor match)",
		shortID(best.NodeID), best.GetLagSeq())
	return best.NodeID, nil
}

// PromoteBestReplicaFromWithGate promotes the best ring-successor replica
// only once its lag is at or below maxLag, polling every pollInterval
// until ctx is done. If the gate never opens before ctx's deadline, it
// falls back to promoting the least-bad available candidate anyway
// (favoring availability over waiting indefinitely) and reports lossy=true
// so the caller can log/alert on the data-loss risk instead of silently
// treating it as a clean promotion.
func (p *Promotion) PromoteBestReplicaFromWithGate(ctx context.Context, ringSuccessor string, maxLag int64, pollInterval time.Duration) (nodeID string, lossy bool, err error) {
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		if n, ok := p.tryPromoteWithinLag(ringSuccessor, maxLag); ok {
			return n, false, nil
		}

		select {
		case <-ctx.Done():
			p.mu.Lock()
			defer p.mu.Unlock()
			best := p.replicaSet.BestReplicaFrom(ringSuccessor)
			if best == nil {
				return "", false, ErrNoReplicaAvailable
			}
			p.replicaSet.SetStatus(best.NodeID, ReplicaActive)
			p.promoted = true
			p.promotedNode = best.NodeID
			p.lossyPromotions.Add(1)
			log.Printf("[promotion] promotion gate timed out for %s: promoting %s anyway at lag=%d (> max %d) — replicated writes at risk of loss",
				shortID(ringSuccessor), shortID(best.NodeID), best.GetLagSeq(), maxLag)
			return best.NodeID, true, nil
		case <-ticker.C:
		}
	}
}

func (p *Promotion) tryPromoteWithinLag(ringSuccessor string, maxLag int64) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	best := p.replicaSet.BestReplicaFrom(ringSuccessor)
	if best == nil || best.GetLagSeq() > maxLag {
		return "", false
	}
	p.replicaSet.SetStatus(best.NodeID, ReplicaActive)
	p.promoted = true
	p.promotedNode = best.NodeID
	log.Printf("[promotion] promoted replica %s to primary (lag=%d <= max %d, ring-successor match)",
		shortID(best.NodeID), best.GetLagSeq(), maxLag)
	return best.NodeID, true
}

// LossyPromotions reports how many times this Promotion's gated path had
// to promote a replica whose lag exceeded the configured threshold
// because the gate timed out first.
func (p *Promotion) LossyPromotions() int64 {
	return p.lossyPromotions.Load()
}

func (p *Promotion) IsPromoted() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.promoted
}

func (p *Promotion) PromotedNode() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.promotedNode
}

var ErrNoReplicaAvailable = &ReplicaError{"no replica available for promotion"}

type ReplicaError struct {
	msg string
}

func (e *ReplicaError) Error() string {
	return e.msg
}
