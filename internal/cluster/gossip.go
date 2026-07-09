package cluster

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hydracache/hydracache/internal/network"
)

// GossipMember is a single member record in a gossip exchange.
type GossipMember struct {
	ID          string    `json:"id"`
	Address     string    `json:"address"`
	State       string    `json:"state"` // "alive", "suspect", "dead", "left"
	Incarnation uint64    `json:"incarnation"`
	Role        Role      `json:"role"`
	LastSeen    time.Time `json:"last_seen"`
}

// GossipMessage is the JSON payload exchanged via the GOSSIP command.
type GossipMessage struct {
	Members []GossipMember `json:"members"`
}

// GossipTable is the local membership table.
type GossipTable struct {
	mu      sync.RWMutex
	members map[string]GossipMember
}

func NewGossipTable() *GossipTable {
	return &GossipTable{
		members: make(map[string]GossipMember),
	}
}

func (gt *GossipTable) Get(id string) (GossipMember, bool) {
	gt.mu.RLock()
	defer gt.mu.RUnlock()
	m, ok := gt.members[id]
	return m, ok
}

func (gt *GossipTable) Set(m GossipMember) {
	gt.mu.Lock()
	defer gt.mu.Unlock()
	gt.members[m.ID] = m
}

func (gt *GossipTable) All() []GossipMember {
	gt.mu.RLock()
	defer gt.mu.RUnlock()
	result := make([]GossipMember, 0, len(gt.members))
	for _, m := range gt.members {
		result = append(result, m)
	}
	return result
}

func (gt *GossipTable) Snapshot() map[string]GossipMember {
	gt.mu.RLock()
	defer gt.mu.RUnlock()
	snap := make(map[string]GossipMember, len(gt.members))
	for k, v := range gt.members {
		snap[k] = v
	}
	return snap
}

// Gossip implements periodic anti-entropy membership propagation with
// incarnation-based merge and self-refutation.
type Gossip struct {
	selfID      string
	selfNode    *Node
	topology    *Topology
	table       *GossipTable
	incarnation atomic.Uint64
	mu          sync.Mutex // protects topology mutations during merge

	// config
	interval      time.Duration
	peersPerRound int

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewGossip(selfNode *Node, topo *Topology) *Gossip {
	g := &Gossip{
		selfID:        selfNode.ID,
		selfNode:      selfNode,
		topology:      topo,
		table:         NewGossipTable(),
		interval:      5 * time.Second,
		peersPerRound: 2,
		stopCh:        make(chan struct{}),
	}
	g.incarnation.Store(1)

	// Add self to gossip table
	g.table.Set(GossipMember{
		ID:          selfNode.ID,
		Address:     selfNode.Address,
		State:       "alive",
		Incarnation: 1,
		Role:        selfNode.GetRole(),
		LastSeen:    time.Now(),
	})

	return g
}

func (g *Gossip) Incarnation() uint64 {
	return g.incarnation.Load()
}

// Merge processes an incoming gossip table. For each member record:
//   - If the record is about self: apply self-refutation if needed.
//   - Otherwise: apply the cross-node merge rule.
//
// Returns true if self-refutation occurred (caller may want to propagate sooner).
func (g *Gossip) Merge(incoming []GossipMember) bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	selfRefuted := false

	for _, m := range incoming {
		if m.ID == g.selfID {
			if g.selfRefute(m) {
				selfRefuted = true
			}
			continue
		}
		g.mergeMember(m)
	}

	return selfRefuted
}

// selfRefute checks if an incoming record about self requires refutation.
// Returns true if refutation occurred.
func (g *Gossip) selfRefute(incoming GossipMember) bool {
	currentInc := g.incarnation.Load()

	// Someone claims I'm dead or left — I'm alive, refute it
	if incoming.State == "dead" || incoming.State == "left" {
		newInc := incoming.Incarnation
		if newInc < currentInc {
			newInc = currentInc
		}
		g.incarnation.Store(newInc + 1)
		g.table.Set(GossipMember{
			ID:          g.selfID,
			Address:     g.selfNode.Address,
			State:       "alive",
			Incarnation: newInc + 1,
			Role:        g.selfNode.GetRole(),
			LastSeen:    time.Now(),
		})
		g.topology.SetNodeHealth(g.selfID, HealthAlive)
		log.Printf("[gossip] self-refutation: peer claimed %s at inc=%d, bumped to inc=%d",
			incoming.State, incoming.Incarnation, newInc+1)
		return true
	}

	// Stale record about me (lower incarnation) — ignore
	if incoming.Incarnation < currentInc {
		return false
	}

	// Same incarnation, same alive — redundant, ignore
	if incoming.Incarnation == currentInc && incoming.State == "alive" {
		return false
	}

	// Higher incarnation — shouldn't happen (only I increment my own),
	// but be defensive: adopt it
	if incoming.Incarnation > currentInc {
		g.incarnation.Store(incoming.Incarnation)
		g.table.Set(GossipMember{
			ID:          g.selfID,
			Address:     g.selfNode.Address,
			State:       "alive",
			Incarnation: incoming.Incarnation,
			Role:        g.selfNode.GetRole(),
			LastSeen:    time.Now(),
		})
		return false
	}

	return false
}

// mergeMember applies the cross-node merge rule for a remote member.
func (g *Gossip) mergeMember(incoming GossipMember) {
	existing, exists := g.table.Get(incoming.ID)

	if !exists {
		g.table.Set(incoming)
		g.applyToTopology(incoming)
		return
	}

	// Higher incarnation always wins
	if incoming.Incarnation > existing.Incarnation {
		g.table.Set(incoming)
		g.applyToTopology(incoming)
		return
	}

	// Same incarnation: terminal state (dead/left) wins over alive/suspect
	if incoming.Incarnation == existing.Incarnation {
		if isTerminal(incoming.State) && !isTerminal(existing.State) {
			g.table.Set(incoming)
			g.applyToTopology(incoming)
			return
		}
		// Both terminal or both non-terminal: existing wins (no-op)
	}
	// Lower incarnation: incoming is stale, ignore
}

func isTerminal(state string) bool {
	return state == "dead" || state == "left"
}

// applyToTopology updates local Topology based on a gossip member state.
func (g *Gossip) applyToTopology(m GossipMember) {
	switch m.State {
	case "alive":
		node, ok := g.topology.GetNode(m.ID)
		if !ok {
			// New node — add to topology
			n := NewNode(m.ID, m.Address)
			n.SetRole(m.Role)
			g.topology.AddNode(n)
			log.Printf("[gossip] discovered new node %s at %s", shortID(m.ID), m.Address)
		} else {
			// Existing node — ensure alive
			if node.GetHealth() != HealthAlive {
				g.topology.SetNodeHealth(m.ID, HealthAlive)
			}
		}
	case "suspect":
		g.topology.SetNodeHealth(m.ID, HealthSuspect)
	case "dead":
		node, ok := g.topology.GetNode(m.ID)
		if ok && node.GetHealth() != HealthDead {
			g.topology.SetNodeHealth(m.ID, HealthDead)
		}
	case "left":
		node, ok := g.topology.GetNode(m.ID)
		if ok && node.GetHealth() != HealthLeft {
			g.topology.SetNodeHealth(m.ID, HealthLeft)
		}
	}
}

// Start begins periodic anti-entropy gossip.
func (g *Gossip) Start() {
	g.wg.Add(1)
	go g.gossipLoop()
}

func (g *Gossip) Stop() {
	close(g.stopCh)
	g.wg.Wait()
}

func (g *Gossip) gossipLoop() {
	defer g.wg.Done()
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()
	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.gossipRound()
		}
	}
}

// gossipRound picks random peers and exchanges full tables.
func (g *Gossip) gossipRound() {
	peers := g.topology.AliveNodes()
	if len(peers) < 1 {
		return
	}

	// Filter out self
	var targets []*Node
	for _, p := range peers {
		if p.ID != g.selfID {
			targets = append(targets, p)
		}
	}
	if len(targets) == 0 {
		return
	}

	// Pick min(peersPerRound, len(targets)) random peers
	n := g.peersPerRound
	if n > len(targets) {
		n = len(targets)
	}
	rand.Shuffle(len(targets), func(i, j int) {
		targets[i], targets[j] = targets[j], targets[i]
	})
	targets = targets[:n]

	localTable := g.table.Snapshot()
	localMembers := make([]GossipMember, 0, len(localTable))
	for _, m := range localTable {
		localMembers = append(localMembers, m)
	}
	payload, err := json.Marshal(GossipMessage{Members: localMembers})
	if err != nil {
		log.Printf("[gossip] failed to marshal table: %v", err)
		return
	}

	for _, peer := range targets {
		g.exchangeWithPeer(peer, payload)
	}
}

func (g *Gossip) exchangeWithPeer(peer *Node, payload []byte) {
	client := network.NewClient(peer.Address)
	if err := client.Connect(); err != nil {
		log.Printf("[gossip] failed to connect to %s: %v", shortID(peer.ID), err)
		return
	}
	defer client.Close()

	resp, err := client.Send("GOSSIP", string(payload))
	if err != nil {
		log.Printf("[gossip] failed to send to %s: %v", shortID(peer.ID), err)
		return
	}

	var msg GossipMessage
	if err := json.Unmarshal([]byte(resp), &msg); err != nil {
		log.Printf("[gossip] failed to parse response from %s: %v", shortID(peer.ID), err)
		return
	}

	g.Merge(msg.Members)
}

// HandleGossip processes an incoming GOSSIP command and returns this
// node's full membership table.
func (g *Gossip) HandleGossip(payload string) (string, error) {
	var msg GossipMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return "", err
	}

	g.Merge(msg.Members)

	// Respond with our full table
	localTable := g.table.Snapshot()
	members := make([]GossipMember, 0, len(localTable))
	for _, m := range localTable {
		members = append(members, m)
	}
	resp, err := json.Marshal(GossipMessage{Members: members})
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

// Bootstrap connects to seed addresses, pulls their membership tables,
// and merges them. Returns the number of seeds successfully contacted.
func (g *Gossip) Bootstrap(addresses []string) int {
	connected := 0
	for _, addr := range addresses {
		client := network.NewClient(addr)
		if err := client.Connect(); err != nil {
			log.Printf("[gossip] seed %s unreachable: %v", addr, err)
			continue
		}

		// Send empty gossip to pull the seed's table
		resp, err := client.Send("GOSSIP", "{}")
		client.Close()
		if err != nil {
			log.Printf("[gossip] failed to pull from seed %s: %v", addr, err)
			continue
		}

		var msg GossipMessage
		if err := json.Unmarshal([]byte(resp), &msg); err != nil {
			log.Printf("[gossip] bad response from seed %s: %v", addr, err)
			continue
		}

		g.Merge(msg.Members)
		connected++
		log.Printf("[gossip] bootstrapped from seed %s (%d members)", addr, len(msg.Members))
	}
	return connected
}
