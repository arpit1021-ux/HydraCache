// Package election implements a Raft-style leader election group: durable
// term/vote persistence, a pre-vote phase to stop disruptive candidates,
// quorum computed from live cluster membership, and a leader lease that
// makes a partitioned-away leader step down instead of continuing to act
// as leader.
//
// One Election instance represents one voting group. HydraCache's data
// path is sharded by consistent hashing rather than owned by a single
// whole-cluster leader, so this package is a general-purpose primitive:
// the node-level group wired in cmd/server/main.go answers "does this
// node currently hold cluster-coordinator status," and its term is not
// used as the per-write fencing token — see internal/replication for the
// per-shard epoch fencing that protects the actual data path.
package election

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
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

// Config configures an Election. SelfID, Store, and Transport are
// required; PeersFunc defaults to "no peers" (single-node cluster).
type Config struct {
	SelfID    string
	Store     TermStore
	Transport Transport
	// PeersFunc returns the current set of OTHER known cluster members
	// (never including SelfID). It is called fresh before every election
	// attempt and every heartbeat round, so membership changes take
	// effect immediately. It should return the full known membership
	// (including currently-unreachable nodes), not just reachable ones —
	// quorum must be computed against total known cluster size or a
	// minority partition could reach "quorum" among just its own
	// reachable members.
	PeersFunc func() []Peer

	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	// LeaseTimeout is how long a leader will keep acting as leader after
	// its last quorum-acknowledged heartbeat round before stepping down.
	// Must be well above HeartbeatInterval to tolerate jitter/GC pauses.
	LeaseTimeout time.Duration
	// RPCTimeout bounds every individual vote/heartbeat RPC so a single
	// unreachable or hung peer can never stall an election or lease
	// renewal beyond this duration.
	RPCTimeout time.Duration
}

func (c *Config) setDefaults() {
	if c.ElectionTimeoutMax <= 0 {
		c.ElectionTimeoutMax = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = c.ElectionTimeoutMax / 2
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.LeaseTimeout <= 0 {
		c.LeaseTimeout = 1 * time.Second
	}
	if c.RPCTimeout <= 0 {
		c.RPCTimeout = 200 * time.Millisecond
	}
	if c.PeersFunc == nil {
		c.PeersFunc = func() []Peer { return nil }
	}
}

type Election struct {
	mu               sync.RWMutex
	selfID           string
	state            State
	term             uint64
	votedFor         string
	lastHeartbeat    time.Time // follower/candidate: last contact with a legitimate leader/candidate
	lastQuorumAck    time.Time // leader: last time a heartbeat round reached quorum
	electionTimeout  time.Duration
	onBecomeLeader   func()
	onLoseLeadership func()

	timeoutMin        time.Duration
	timeoutMax        time.Duration
	heartbeatInterval time.Duration
	leaseTimeout      time.Duration
	rpcTimeout        time.Duration

	store     TermStore
	transport Transport
	peersFunc func() []Peer

	electing     atomic.Bool
	heartbeating atomic.Bool

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// New constructs an Election, loading any previously persisted term/vote
// from cfg.Store so a restarted node never forgets a vote it already cast.
func New(cfg Config) (*Election, error) {
	if cfg.SelfID == "" {
		return nil, errors.New("election: SelfID is required")
	}
	if cfg.Store == nil {
		return nil, errors.New("election: Store is required (use NewMemoryTermStore for non-durable tests)")
	}
	if cfg.Transport == nil {
		return nil, errors.New("election: Transport is required (use a LocalTransport for single-process tests)")
	}
	cfg.setDefaults()

	persisted, err := cfg.Store.Load()
	if err != nil {
		return nil, fmt.Errorf("election: load persisted state: %w", err)
	}

	now := time.Now()
	e := &Election{
		selfID:            cfg.SelfID,
		state:             StateFollower,
		term:              persisted.Term,
		votedFor:          persisted.VotedFor,
		lastHeartbeat:     now,
		lastQuorumAck:     now,
		timeoutMin:        cfg.ElectionTimeoutMin,
		timeoutMax:        cfg.ElectionTimeoutMax,
		heartbeatInterval: cfg.HeartbeatInterval,
		leaseTimeout:      cfg.LeaseTimeout,
		rpcTimeout:        cfg.RPCTimeout,
		store:             cfg.Store,
		transport:         cfg.Transport,
		peersFunc:         cfg.PeersFunc,
		stopCh:            make(chan struct{}),
	}
	e.electionTimeout = e.randomTimeout()
	return e, nil
}

func (e *Election) randomTimeout() time.Duration {
	span := e.timeoutMax - e.timeoutMin
	if span <= 0 {
		return e.timeoutMax
	}
	return e.timeoutMin + time.Duration(rand.Int63n(int64(span)))
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

// Start begins the election loop. It stops when ctx is canceled or Stop
// is called, whichever comes first.
func (e *Election) Start(ctx context.Context) {
	e.wg.Add(1)
	go e.electionLoop(ctx)
}

func (e *Election) electionLoop(ctx context.Context) {
	defer e.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.tick()
		}
	}
}

func (e *Election) tick() {
	e.mu.RLock()
	state := e.state
	sinceHeartbeat := time.Since(e.lastHeartbeat)
	timeout := e.electionTimeout
	sinceQuorumAck := time.Since(e.lastQuorumAck)
	lease := e.leaseTimeout
	e.mu.RUnlock()

	switch state {
	case StateFollower, StateCandidate:
		if sinceHeartbeat > timeout && e.electing.CompareAndSwap(false, true) {
			e.wg.Add(1)
			go e.runElectionAttempt()
		}
	case StateLeader:
		if sinceQuorumAck > lease {
			e.stepDown("lease expired: lost contact with quorum", 0)
			return
		}
		if e.heartbeating.CompareAndSwap(false, true) {
			e.wg.Add(1)
			go e.sendHeartbeats()
		}
	}
}

// runElectionAttempt performs one full pre-vote + vote cycle. It always
// terminates: every RPC it issues is bounded by e.rpcTimeout via ctx, and
// it never blocks on anything unbounded.
func (e *Election) runElectionAttempt() {
	defer e.wg.Done()
	defer e.electing.Store(false)

	e.mu.RLock()
	alreadyLeader := e.state == StateLeader
	candidateTerm := e.term + 1
	e.mu.RUnlock()
	if alreadyLeader {
		return
	}

	peers := e.peersFunc()
	total := len(peers) + 1
	quorum := total/2 + 1

	// Phase 1: Pre-Vote (non-binding). A node that has been partitioned
	// away keeps timing out and would otherwise keep bumping its term
	// every cycle, forcing every other node into a pointless re-election
	// as soon as it reconnects. Requiring pre-vote quorum first means it
	// can never even begin a disruptive real election: the healthy
	// majority (still hearing the real leader's heartbeats) will refuse
	// pre-votes to anyone while their own election timeout hasn't fired.
	ctx, cancel := context.WithTimeout(context.Background(), e.rpcTimeout)
	preGranted := e.collectVotes(ctx, peers, candidateTerm, true)
	cancel()
	if preGranted+1 < quorum {
		return
	}

	// Phase 2: real election. Persist term+vote BEFORE issuing any real
	// vote request or returning from this function — a crash after this
	// point and before winning must never be able to vote again in this
	// same term on restart.
	e.mu.Lock()
	if e.state == StateLeader || candidateTerm <= e.term {
		e.mu.Unlock()
		return // overtaken while pre-voting
	}
	e.term = candidateTerm
	e.state = StateCandidate
	e.votedFor = e.selfID
	e.lastHeartbeat = time.Now()
	e.electionTimeout = e.randomTimeout()
	toPersist := PersistedState{Term: e.term, VotedFor: e.votedFor}
	e.mu.Unlock()

	if err := e.store.Save(toPersist); err != nil {
		log.Printf("[election] %s: failed to persist candidacy for term %d, aborting: %v", shortID(e.selfID), candidateTerm, err)
		return
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), e.rpcTimeout)
	granted := e.collectVotes(ctx2, peers, candidateTerm, false)
	cancel2()

	e.mu.Lock()
	if e.state != StateCandidate || e.term != candidateTerm {
		e.mu.Unlock()
		return // superseded while votes were in flight
	}
	if granted+1 >= quorum {
		e.becomeLeaderLocked()
	}
	e.mu.Unlock()
}

// collectVotes fans out a vote (or pre-vote) request to every peer
// concurrently and returns the number granted. Every spawned goroutine
// exits when its RPC returns, which the transport contract bounds to ctx.
func (e *Election) collectVotes(ctx context.Context, peers []Peer, term uint64, preVote bool) int {
	if len(peers) == 0 {
		return 0
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	granted := 0
	for _, p := range peers {
		p := p
		wg.Add(1)
		e.wg.Add(1)
		go func() {
			defer wg.Done()
			defer e.wg.Done()
			resp, err := e.transport.RequestVote(ctx, p, VoteRequest{
				CandidateID: e.selfID,
				Term:        term,
				PreVote:     preVote,
			})
			if err != nil || !resp.Granted {
				return
			}
			mu.Lock()
			granted++
			mu.Unlock()
		}()
	}
	wg.Wait()
	return granted
}

func (e *Election) becomeLeaderLocked() {
	e.state = StateLeader
	e.lastQuorumAck = time.Now()
	log.Printf("[election] %s became leader for term %d", shortID(e.selfID), e.term)
	fn := e.onBecomeLeader
	if fn != nil {
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			fn()
		}()
	}
}

func (e *Election) fireLoseLeadership() {
	e.mu.RLock()
	fn := e.onLoseLeadership
	e.mu.RUnlock()
	if fn == nil {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		fn()
	}()
}

// sendHeartbeats runs one heartbeat round as leader. If it fails to reach
// quorum, it does NOT step down immediately (a single missed round can be
// ordinary jitter) — tick() steps down once lastQuorumAck has been stale
// for a full leaseTimeout, which is what actually enforces the lease.
func (e *Election) sendHeartbeats() {
	defer e.wg.Done()
	defer e.heartbeating.Store(false)

	e.mu.RLock()
	term := e.term
	e.mu.RUnlock()
	peers := e.peersFunc()

	if len(peers) == 0 {
		// Single-node cluster: no one to ack, and no one who could
		// dispute leadership either.
		e.mu.Lock()
		if e.state == StateLeader && e.term == term {
			e.lastQuorumAck = time.Now()
		}
		e.mu.Unlock()
		return
	}

	total := len(peers) + 1
	quorum := total/2 + 1

	ctx, cancel := context.WithTimeout(context.Background(), e.rpcTimeout)
	defer cancel()

	var wg sync.WaitGroup
	var mu sync.Mutex
	acks := 1 // self
	var higherTerm uint64
	for _, p := range peers {
		p := p
		wg.Add(1)
		e.wg.Add(1)
		go func() {
			defer wg.Done()
			defer e.wg.Done()
			resp, err := e.transport.SendHeartbeat(ctx, p, HeartbeatRequest{LeaderID: e.selfID, Term: term})
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if resp.Success {
				acks++
			} else if resp.Term > higherTerm {
				higherTerm = resp.Term
			}
		}()
	}
	wg.Wait()

	if higherTerm > term {
		e.stepDown(fmt.Sprintf("observed higher term %d while sending heartbeats", higherTerm), higherTerm)
		return
	}

	if acks >= quorum {
		e.mu.Lock()
		if e.state == StateLeader && e.term == term {
			e.lastQuorumAck = time.Now()
		}
		e.mu.Unlock()
	}
}

// stepDown demotes this node to follower. newTerm > 0 additionally adopts
// that (higher) term and clears the vote; newTerm == 0 means "step down
// without changing term" (used for lease expiry, where we haven't
// observed anyone else's term, only lost contact with our own quorum).
func (e *Election) stepDown(reason string, newTerm uint64) {
	e.mu.Lock()
	wasLeader := e.state == StateLeader
	if newTerm > e.term {
		e.term = newTerm
		e.votedFor = ""
	}
	e.state = StateFollower
	e.lastHeartbeat = time.Now()
	e.electionTimeout = e.randomTimeout()
	toPersist := PersistedState{Term: e.term, VotedFor: e.votedFor}
	e.mu.Unlock()

	log.Printf("[election] %s stepping down (term %d): %s", shortID(e.selfID), toPersist.Term, reason)

	if err := e.store.Save(toPersist); err != nil {
		log.Printf("[election] %s: failed to persist stepdown state: %v", shortID(e.selfID), err)
	}
	if wasLeader {
		e.fireLoseLeadership()
	}
}

// HandlePreVoteRequest answers a non-binding pre-vote probe. It never
// mutates or persists term/vote state. A follower that has heard from a
// leader within its own election timeout refuses every pre-vote — this
// is what stops a reconnecting, previously-partitioned node from ever
// collecting enough pre-votes to disrupt a healthy leader.
func (e *Election) handlePreVoteRequest(term uint64) VoteResponse {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if term < e.term {
		return VoteResponse{VoterID: e.selfID, Term: e.term, Granted: false}
	}
	if e.state == StateLeader {
		return VoteResponse{VoterID: e.selfID, Term: e.term, Granted: false}
	}
	if e.state == StateFollower && time.Since(e.lastHeartbeat) < e.electionTimeout {
		return VoteResponse{VoterID: e.selfID, Term: e.term, Granted: false}
	}
	return VoteResponse{VoterID: e.selfID, Term: e.term, Granted: true}
}

// HandleVoteRequest answers a real vote request (or dispatches a pre-vote
// probe to handlePreVoteRequest). Per the durability guarantee, any state
// change is persisted to stable storage before this returns to the
// caller — a crash immediately after responding can never cause a second,
// conflicting vote to be granted in the same term after restart.
func (e *Election) HandleVoteRequest(req VoteRequest) VoteResponse {
	if req.PreVote {
		return e.handlePreVoteRequest(req.Term)
	}

	e.mu.Lock()
	if req.Term > e.term {
		e.term = req.Term
		e.state = StateFollower
		e.votedFor = ""
	}
	if req.Term < e.term {
		term := e.term
		e.mu.Unlock()
		return VoteResponse{VoterID: e.selfID, Term: term, Granted: false}
	}

	grant := e.votedFor == "" || e.votedFor == req.CandidateID
	if grant {
		e.votedFor = req.CandidateID
		e.lastHeartbeat = time.Now()
	}
	toPersist := PersistedState{Term: e.term, VotedFor: e.votedFor}
	e.mu.Unlock()

	if grant {
		if err := e.store.Save(toPersist); err != nil {
			log.Printf("[election] %s: failed to persist vote for %s, denying: %v", shortID(e.selfID), shortID(req.CandidateID), err)
			return VoteResponse{VoterID: e.selfID, Term: toPersist.Term, Granted: false}
		}
	}
	return VoteResponse{VoterID: e.selfID, Term: toPersist.Term, Granted: grant}
}

// HandleHeartbeat answers a heartbeat from a claimed leader. Term/vote are
// persisted before returning only when they actually change (a new term
// or a new recognized leader), not on every heartbeat, since heartbeats
// arrive far more often than term changes.
func (e *Election) HandleHeartbeat(req HeartbeatRequest) HeartbeatResponse {
	e.mu.Lock()
	if req.Term < e.term {
		term := e.term
		e.mu.Unlock()
		return HeartbeatResponse{Term: term, Success: false}
	}

	wasLeaderOrCandidate := e.state == StateLeader || e.state == StateCandidate
	changed := req.Term > e.term || e.votedFor != req.LeaderID
	e.term = req.Term
	e.state = StateFollower
	e.votedFor = req.LeaderID
	e.lastHeartbeat = time.Now()
	e.electionTimeout = e.randomTimeout()
	toPersist := PersistedState{Term: e.term, VotedFor: e.votedFor}
	e.mu.Unlock()

	if changed {
		if err := e.store.Save(toPersist); err != nil {
			log.Printf("[election] %s: failed to persist heartbeat term: %v", shortID(e.selfID), err)
		}
	}
	if wasLeaderOrCandidate {
		e.fireLoseLeadership()
	}
	return HeartbeatResponse{Term: toPersist.Term, Success: true}
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

// LeaderTerm atomically reports whether this node is currently the leader
// and, if so, its term — avoiding a check-then-read race between separate
// IsLeader()/Term() calls.
func (e *Election) LeaderTerm() (isLeader bool, term uint64) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state == StateLeader, e.term
}

// Quorum reports the quorum size computed from current live peer count.
func (e *Election) Quorum() int {
	total := len(e.peersFunc()) + 1
	return total/2 + 1
}

// Stop halts the election loop and waits for every goroutine this
// instance spawned (heartbeat rounds, vote fan-outs, callback invocations)
// to finish. Safe to call more than once.
func (e *Election) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
	e.wg.Wait()
}

// Resign immediately steps down from leadership if this node currently
// holds it, firing OnLoseLeadership right away instead of leaving
// dependent state (e.g. this node's advertised role in the cluster
// topology) stale until a lease timeout expires or a peer's heartbeat
// eventually reveals the term changed. Intended for graceful shutdown: a
// node that's about to exit shouldn't keep being advertised as leader for
// however long is left before the process actually stops. No-op if this
// node isn't currently leader.
func (e *Election) Resign() {
	if !e.IsLeader() {
		return
	}
	e.stepDown("graceful resignation", 0)
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
