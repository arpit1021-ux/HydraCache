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

	best.Status = ReplicaActive
	p.promoted = true
	p.promotedNode = best.NodeID

	log.Printf("[promotion] promoted replica %s to primary (lag=%d)", shortID(best.NodeID), best.LagSeq)
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
