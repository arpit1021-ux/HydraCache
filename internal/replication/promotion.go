package replication

import (
	"log"
	"sync"
)

type Promotion struct {
	mu           sync.Mutex
	replicaSet   *ReplicaSet
	promoted     bool
	promotedNode string
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
