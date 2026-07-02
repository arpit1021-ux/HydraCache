package heartbeat

import (
	"log"
	"sync"
	"time"
)

func shortID2(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

type Membership struct {
	mu       sync.RWMutex
	members  map[string]*Member
	onChange func(MembershipEvent)
}

type Member struct {
	NodeID    string
	Address   string
	State     MemberState
	UpdatedAt time.Time
}

type MemberState int

const (
	MemberAlive MemberState = iota
	MemberSuspect
	MemberDead
	MemberLeft
)

type MembershipEvent struct {
	NodeID    string
	OldState  MemberState
	NewState  MemberState
	Timestamp time.Time
}

func NewMembership() *Membership {
	return &Membership{
		members: make(map[string]*Member),
	}
}

func (m *Membership) OnChange(fn func(MembershipEvent)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onChange = fn
}

func (m *Membership) AddMember(nodeID, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldState := MemberAlive
	if existing, ok := m.members[nodeID]; ok {
		oldState = existing.State
		existing.State = MemberAlive
		existing.UpdatedAt = time.Now()
	} else {
		m.members[nodeID] = &Member{
			NodeID:    nodeID,
			Address:   address,
			State:     MemberAlive,
			UpdatedAt: time.Now(),
		}
	}

	if m.onChange != nil {
		go m.onChange(MembershipEvent{
			NodeID:    nodeID,
			OldState:  oldState,
			NewState:  MemberAlive,
			Timestamp: time.Now(),
		})
	}
}

func (m *Membership) SetState(nodeID string, state MemberState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	member, ok := m.members[nodeID]
	if !ok {
		return
	}

	oldState := member.State
	member.State = state
	member.UpdatedAt = time.Now()

	log.Printf("[membership] %s: %v → %v", shortID2(nodeID), oldState, state)

	if m.onChange != nil {
		go m.onChange(MembershipEvent{
			NodeID:    nodeID,
			OldState:  oldState,
			NewState:  state,
			Timestamp: time.Now(),
		})
	}
}

func (m *Membership) GetMember(nodeID string) (*Member, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	member, ok := m.members[nodeID]
	return member, ok
}

func (m *Membership) AliveMembers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var alive []*Member
	for _, member := range m.members {
		if member.State == MemberAlive {
			alive = append(alive, member)
		}
	}
	return alive
}

func (m *Membership) DeadMembers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var dead []*Member
	for _, member := range m.members {
		if member.State == MemberDead {
			dead = append(dead, member)
		}
	}
	return dead
}

func (m *Membership) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.members)
}

func (m *Membership) AliveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, member := range m.members {
		if member.State == MemberAlive {
			count++
		}
	}
	return count
}
