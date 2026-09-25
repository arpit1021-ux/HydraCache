package chaostest

import (
	"fmt"
	"sort"
)

// Minimal FNV-1a hash ring replica for external key-ownership verification.
// This mirrors internal/hashring exactly: 150 virtual nodes per physical node,
// FNV-1a 32-bit, binary search for clockwise lookup.
type ring struct {
	positions    []uint32
	posToNode    map[uint32]string
	virtualNodes int
}

func newRing(vnodes int) *ring {
	if vnodes <= 0 {
		vnodes = 150
	}
	return &ring{
		positions:    make([]uint32, 0, vnodes*8),
		posToNode:    make(map[uint32]string),
		virtualNodes: vnodes,
	}
}

func fnv1a(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

func (r *ring) addNode(nodeID string) {
	for i := 0; i < r.virtualNodes; i++ {
		pos := fnv1a(fmt.Sprintf("%s#%d", nodeID, i))
		r.positions = append(r.positions, pos)
		r.posToNode[pos] = nodeID
	}
	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})
}

// primaryNode returns the node that should own the given key.
func (r *ring) primaryNode(key string) string {
	if len(r.positions) == 0 {
		return ""
	}
	h := fnv1a(key)
	idx := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= h
	})
	if idx >= len(r.positions) {
		idx = 0
	}
	return r.posToNode[r.positions[idx]]
}

// buildRingFromNodes creates a ring from a list of node IDs, matching
// the state the cluster would have with those nodes.
func buildRingFromNodes(nodeIDs []string) *ring {
	r := newRing(150)
	for _, id := range nodeIDs {
		r.addNode(id)
	}
	return r
}
