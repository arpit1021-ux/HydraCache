package election

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	StateFollower State = iota
	StateCandidate
	StateLeader
)

func (s State) String() string {
	switch s {
	case StateFollower:
		return "follower"
	case StateCandidate:
		return "candidate"
	case StateLeader:
		return "leader"
	default:
		return "unknown"
	}
}

type Election struct {
	mu                sync.RWMutex
	selfID            string
	state             State
	term              uint64
	votedFor          string
	votesReceived     map[string]bool
	quorum            int
	clusterSize       int
	lastHeartbeat     time.Time
	electionTimeout   time.Duration
	heartbeatInterval time.Duration
	onBecomeLeader    func()
	onLoseLeadership  func()
	stopCh            chan struct{}
}

func New(selfID string, clusterSize int) *Election {
	if clusterSize < 1 {
		clusterSize = 1
	}
	e := &Election{
		selfID:            selfID,
		state:             StateFollower,
		term:              0,
		quorum:            clusterSize/2 + 1,
		clusterSize:       clusterSize,
		lastHeartbeat:     time.Now(),
		electionTimeout:   randomTimeout(),
		heartbeatInterval: 50 * time.Millisecond,
		votesReceived:     make(map[string]bool),
		stopCh:            make(chan struct{}),
	}
	return e
}

func randomTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (e *Election) OnBecomeLeader(fn func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onBecomeLeader = fn
}

func (e *Election) OnLoseLeadership(fn func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.onLoseLeadership = fn
}

func (e *Election) Start() {
	go e.electionLoop()
}

func (e *Election) electionLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.tick()
		}
	}
}

func (e *Election) tick() {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch e.state {
	case StateFollower:
		if time.Since(e.lastHeartbeat) > e.electionTimeout {
			e.startElection()
		}
	case StateCandidate:
		if time.Since(e.lastHeartbeat) > e.electionTimeout {
			e.startElection()
		}
	case StateLeader:
		e.sendHeartbeat()
	}
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func (e *Election) startElection() {
	e.term++
	e.state = StateCandidate
	e.votedFor = e.selfID
	e.votesReceived = map[string]bool{e.selfID: true}
	e.lastHeartbeat = time.Now()
	e.electionTimeout = randomTimeout()

	log.Printf("[election] node %s started election for term %d (quorum=%d)",
		shortID(e.selfID), e.term, e.quorum)

	if len(e.votesReceived) >= e.quorum {
		e.becomeLeader()
	}
}

func (e *Election) becomeLeader() {
	e.state = StateLeader
	log.Printf("[election] node %s became leader for term %d", shortID(e.selfID), e.term)
	if e.onBecomeLeader != nil {
		go e.onBecomeLeader()
	}
}

func (e *Election) HandleVoteRequest(candidateID string, term uint64) (bool, uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if term > e.term {
		e.term = term
		e.state = StateFollower
		e.votedFor = ""
	}

	if term < e.term {
		return false, e.term
	}

	if e.votedFor == "" || e.votedFor == candidateID {
		e.votedFor = candidateID
		e.lastHeartbeat = time.Now()
		return true, e.term
	}

	return false, e.term
}

func (e *Election) HandleHeartbeat(leaderID string, term uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if term >= e.term {
		e.term = term
		e.state = StateFollower
		e.votedFor = leaderID
		e.lastHeartbeat = time.Now()
	}
}

func (e *Election) ReceiveVote(voterID string, term uint64, granted bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state != StateCandidate || term != e.term {
		return
	}

	if granted {
		e.votesReceived[voterID] = true
		log.Printf("[election] received vote from %s (total=%d/%d)",
			shortID(voterID), len(e.votesReceived), e.quorum)
		if len(e.votesReceived) >= e.quorum {
			e.becomeLeader()
		}
	}
}

func (e *Election) sendHeartbeat() {
	e.lastHeartbeat = time.Now()
}

func (e *Election) State() State {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state
}

func (e *Election) Term() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.term
}

func (e *Election) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state == StateLeader
}

func (e *Election) Quorum() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.quorum
}

func (e *Election) Stop() {
	close(e.stopCh)
}

func (e *Election) UpdateClusterSize(size int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.clusterSize = size
	e.quorum = size/2 + 1
}
