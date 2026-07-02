package hashring

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

const defaultVirtualNodes = 150

type HashRing struct {
	mu           sync.RWMutex
	ring         []uint32
	nodes        map[uint32]string
	virtualNodes int
}

func New(virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = defaultVirtualNodes
	}
	return &HashRing{
		ring:         make([]uint32, 0, virtualNodes*8),
		nodes:        make(map[uint32]string),
		virtualNodes: virtualNodes,
	}
}

func (hr *HashRing) hash(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	for i := 0; i < hr.virtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeID, i)
		position := hr.hash(virtualKey)
		hr.ring = append(hr.ring, position)
		hr.nodes[position] = nodeID
	}
	sort.Slice(hr.ring, func(i, j int) bool {
		return hr.ring[i] < hr.ring[j]
	})
}

func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	newRing := hr.ring[:0]
	for _, pos := range hr.ring {
		if hr.nodes[pos] != nodeID {
			newRing = append(newRing, pos)
		} else {
			delete(hr.nodes, pos)
		}
	}
	hr.ring = newRing
}

func (hr *HashRing) GetNode(key string) string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return ""
	}

	h := hr.hash(key)
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i] >= h
	})

	if idx >= len(hr.ring) {
		idx = 0
	}

	return hr.nodes[hr.ring[idx]]
}

func (hr *HashRing) GetNodes(key string, count int) []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) == 0 {
		return nil
	}

	h := hr.hash(key)
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i] >= h
	})

	seen := make(map[string]bool)
	var result []string

	for i := 0; len(result) < count && i < len(hr.ring); i++ {
		pos := (idx + i) % len(hr.ring)
		nodeID := hr.nodes[hr.ring[pos]]
		if !seen[nodeID] {
			seen[nodeID] = true
			result = append(result, nodeID)
		}
	}

	return result
}

func (hr *HashRing) GetAffectedRange(addedNodeID string) (uint32, uint32) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ring) <= hr.virtualNodes {
		return 0, math.MaxUint32
	}

	firstPos := hr.hash(fmt.Sprintf("%s#0", addedNodeID))
	idx := sort.Search(len(hr.ring), func(i int) bool {
		return hr.ring[i] >= firstPos
	})

	var prevPos uint32
	if idx == 0 {
		prevPos = hr.ring[len(hr.ring)-1]
	} else {
		prevPos = hr.ring[idx-1]
	}

	return prevPos, firstPos
}

func (hr *HashRing) NodeCount() int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()
	seen := make(map[string]bool)
	for _, nodeID := range hr.nodes {
		seen[nodeID] = true
	}
	return len(seen)
}

func (hr *HashRing) Distribution() map[string]int {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	dist := make(map[string]int)
	for _, nodeID := range hr.nodes {
		dist[nodeID]++
	}
	return dist
}

func (hr *HashRing) Visualize() string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	result := "Hash Ring:\n"
	for _, pos := range hr.ring {
		nodeID := hr.nodes[pos]
		if len(nodeID) > 8 {
			nodeID = nodeID[:8]
		}
		result += fmt.Sprintf("  [%08X] → %s\n", pos, nodeID)
	}
	return result
}
