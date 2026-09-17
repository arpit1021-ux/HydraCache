package election

import (
	"context"
	"fmt"
	"sync"
)

// Peer describes a cluster member this node can send election RPCs to.
type Peer struct {
	ID      string
	Address string
}

// Transport sends election RPCs to a specific peer over the network.
// Implementations must respect ctx cancellation/deadline and must always
// return within it — a hung peer must never stall an election or a
// leader's lease renewal beyond the caller's configured RPC timeout.
type Transport interface {
	RequestVote(ctx context.Context, peer Peer, req VoteRequest) (VoteResponse, error)
	SendHeartbeat(ctx context.Context, peer Peer, req HeartbeatRequest) (HeartbeatResponse, error)
}

// localHub is the shared switchboard behind every LocalTransport in one
// simulated cluster, so that a partition cut by one node's transport is
// visible to all of them (a real network link has two ends).
type localHub struct {
	mu   sync.RWMutex
	subs map[string]*Election
	cut  map[[2]string]bool
}

func newLocalHub() *localHub {
	return &localHub{subs: make(map[string]*Election), cut: make(map[[2]string]bool)}
}

func (h *localHub) register(id string, e *Election) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.subs[id] = e
}

func (h *localHub) partition(a, b string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cut[[2]string{a, b}] = true
	h.cut[[2]string{b, a}] = true
}

func (h *localHub) heal(a, b string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.cut, [2]string{a, b})
	delete(h.cut, [2]string{b, a})
}

func (h *localHub) linkUp(a, b string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return !h.cut[[2]string{a, b}]
}

func (h *localHub) peer(id string) (*Election, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	e, ok := h.subs[id]
	return e, ok
}

// LocalTransport is an in-memory Transport used for deterministic
// simulation tests: it wires a set of in-process Election instances
// together by ID and dispatches RPCs as direct method calls, with no real
// network I/O. Links between any two node IDs can be individually cut to
// simulate a network partition — see NewLocalCluster.
type LocalTransport struct {
	self string
	hub  *localHub
}

// NewLocalCluster builds a fully-connected simulated cluster of n nodes
// named "node-0".."node-(n-1)" sharing one switchboard, and returns a
// LocalTransport per node plus a shared PartitionController to cut/heal
// links between them. Callers construct each node's Election with the
// matching Peer/Transport values and must call Wire once every Election
// exists (RPCs sent before Wire will fail with "unknown peer").
func NewLocalCluster(ids []string) (map[string]*LocalTransport, *PartitionController) {
	hub := newLocalHub()
	transports := make(map[string]*LocalTransport, len(ids))
	for _, id := range ids {
		transports[id] = &LocalTransport{self: id, hub: hub}
	}
	return transports, &PartitionController{hub: hub}
}

// Wire registers this node's Election instance so peers can route RPCs to
// it. Must be called once the Election exists, before it starts ticking.
func (t *LocalTransport) Wire(e *Election) {
	t.hub.register(t.self, e)
}

func (t *LocalTransport) RequestVote(ctx context.Context, peer Peer, req VoteRequest) (VoteResponse, error) {
	if !t.hub.linkUp(t.self, peer.ID) {
		return VoteResponse{}, fmt.Errorf("election: no route to %s (partitioned)", peer.ID)
	}
	e, ok := t.hub.peer(peer.ID)
	if !ok {
		return VoteResponse{}, fmt.Errorf("election: unknown peer %s", peer.ID)
	}
	select {
	case <-ctx.Done():
		return VoteResponse{}, ctx.Err()
	default:
	}
	return e.HandleVoteRequest(req), nil
}

func (t *LocalTransport) SendHeartbeat(ctx context.Context, peer Peer, req HeartbeatRequest) (HeartbeatResponse, error) {
	if !t.hub.linkUp(t.self, peer.ID) {
		return HeartbeatResponse{}, fmt.Errorf("election: no route to %s (partitioned)", peer.ID)
	}
	e, ok := t.hub.peer(peer.ID)
	if !ok {
		return HeartbeatResponse{}, fmt.Errorf("election: unknown peer %s", peer.ID)
	}
	select {
	case <-ctx.Done():
		return HeartbeatResponse{}, ctx.Err()
	default:
	}
	return e.HandleHeartbeat(req), nil
}

// PartitionController cuts and heals links in a simulated cluster built by
// NewLocalCluster.
type PartitionController struct {
	hub *localHub
}

func (p *PartitionController) Partition(a, b string) { p.hub.partition(a, b) }
func (p *PartitionController) Heal(a, b string)      { p.hub.heal(a, b) }
