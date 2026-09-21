package election

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func testCfg(id string, transport Transport, peers func() []Peer) Config {
	return Config{
		SelfID:             id,
		Store:              NewMemoryTermStore(),
		Transport:          transport,
		PeersFunc:          peers,
		ElectionTimeoutMin: 40 * time.Millisecond,
		ElectionTimeoutMax: 80 * time.Millisecond,
		HeartbeatInterval:  10 * time.Millisecond,
		LeaseTimeout:       200 * time.Millisecond,
		RPCTimeout:         30 * time.Millisecond,
	}
}

func noPeers() []Peer { return nil }

// noopTransport answers nothing (single-node tests never dial a peer since
// PeersFunc returns none, but New requires a non-nil Transport).
type noopTransport struct{}

func (noopTransport) RequestVote(ctx context.Context, p Peer, r VoteRequest) (VoteResponse, error) {
	return VoteResponse{}, context.DeadlineExceeded
}
func (noopTransport) SendHeartbeat(ctx context.Context, p Peer, r HeartbeatRequest) (HeartbeatResponse, error) {
	return HeartbeatResponse{}, context.DeadlineExceeded
}

func TestNew_LoadsPersistedState(t *testing.T) {
	store := NewMemoryTermStore()
	if err := store.Save(PersistedState{Term: 7, VotedFor: "node-x"}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	cfg := testCfg("node-1", noopTransport{}, noPeers)
	cfg.Store = store
	e, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if e.Term() != 7 {
		t.Errorf("expected term 7 loaded from store, got %d", e.Term())
	}
}

func TestDurableVote_NoDoubleVoteAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	store1, err := NewFileTermStore(filepath.Join(dir, "n1"))
	if err != nil {
		t.Fatalf("NewFileTermStore: %v", err)
	}

	cfg := testCfg("node-1", noopTransport{}, noPeers)
	cfg.Store = store1
	e1, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	resp := e1.HandleVoteRequest(VoteRequest{CandidateID: "candidate-A", Term: 1})
	if !resp.Granted {
		t.Fatal("expected first vote in term 1 to be granted")
	}

	// Simulate a crash and restart: a brand-new Election instance backed
	// by the SAME on-disk store must remember the vote already cast.
	store2, err := NewFileTermStore(filepath.Join(dir, "n1"))
	if err != nil {
		t.Fatalf("NewFileTermStore (reopen): %v", err)
	}
	cfg2 := testCfg("node-1", noopTransport{}, noPeers)
	cfg2.Store = store2
	e2, err := New(cfg2)
	if err != nil {
		t.Fatalf("New (restart): %v", err)
	}
	if e2.Term() != 1 {
		t.Fatalf("expected restarted node to recall term 1, got %d", e2.Term())
	}

	resp2 := e2.HandleVoteRequest(VoteRequest{CandidateID: "candidate-B", Term: 1})
	if resp2.Granted {
		t.Error("restarted node granted a second vote in the same term — durability broken")
	}
}

func TestHandleVoteRequest_OneVotePerTerm(t *testing.T) {
	e, err := New(testCfg("node-1", noopTransport{}, noPeers))
	if err != nil {
		t.Fatal(err)
	}
	r1 := e.HandleVoteRequest(VoteRequest{CandidateID: "node-2", Term: 1})
	if !r1.Granted {
		t.Fatal("expected first vote granted")
	}
	r2 := e.HandleVoteRequest(VoteRequest{CandidateID: "node-3", Term: 1})
	if r2.Granted {
		t.Error("expected second vote in same term to be denied")
	}
	r3 := e.HandleVoteRequest(VoteRequest{CandidateID: "node-2", Term: 1})
	if !r3.Granted {
		t.Error("expected repeat vote for the SAME candidate in the same term to be granted (idempotent)")
	}
}

func TestHandleVoteRequest_HigherTermResets(t *testing.T) {
	e, err := New(testCfg("node-1", noopTransport{}, noPeers))
	if err != nil {
		t.Fatal(err)
	}
	e.HandleVoteRequest(VoteRequest{CandidateID: "node-2", Term: 1})
	resp := e.HandleVoteRequest(VoteRequest{CandidateID: "node-3", Term: 5})
	if !resp.Granted || resp.Term != 5 {
		t.Errorf("expected vote granted for higher term 5, got granted=%v term=%d", resp.Granted, resp.Term)
	}
	if e.Term() != 5 {
		t.Errorf("expected local term to advance to 5, got %d", e.Term())
	}
}

func TestPreVote_RejectsWhenLeaderRecentlyHeardFrom(t *testing.T) {
	e, err := New(testCfg("node-1", noopTransport{}, noPeers))
	if err != nil {
		t.Fatal(err)
	}
	// Freshly constructed: lastHeartbeat is "now", so within electionTimeout.
	resp := e.HandleVoteRequest(VoteRequest{CandidateID: "disruptor", Term: 1, PreVote: true})
	if resp.Granted {
		t.Error("expected pre-vote to be rejected while a leader has been heard from recently")
	}
	if e.Term() != 0 {
		t.Error("pre-vote must never mutate term")
	}
}

func TestHandleHeartbeat_DemotesLeaderAndFiresCallback(t *testing.T) {
	e, err := New(testCfg("node-1", noopTransport{}, noPeers))
	if err != nil {
		t.Fatal(err)
	}
	// Force into leader state via the internal path exercised by becomeLeaderLocked.
	e.mu.Lock()
	e.state = StateLeader
	e.term = 3
	e.mu.Unlock()

	lost := make(chan struct{}, 1)
	e.OnLoseLeadership(func() { lost <- struct{}{} })

	resp := e.HandleHeartbeat(HeartbeatRequest{LeaderID: "node-2", Term: 4})
	if !resp.Success {
		t.Error("expected heartbeat with higher term to be accepted")
	}
	if e.State() != StateFollower {
		t.Errorf("expected follower after heartbeat from higher-term leader, got %v", e.State())
	}
	select {
	case <-lost:
	case <-time.After(time.Second):
		t.Error("expected onLoseLeadership to fire when demoted by a heartbeat")
	}
}

func TestSingleNodeCluster_BecomesLeader(t *testing.T) {
	e, err := New(testCfg("solo", noopTransport{}, noPeers))
	if err != nil {
		t.Fatal(err)
	}
	leaderCh := make(chan struct{}, 1)
	e.OnBecomeLeader(func() { leaderCh <- struct{}{} })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx)
	defer e.Stop()

	select {
	case <-leaderCh:
		if !e.IsLeader() {
			t.Error("expected IsLeader true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for single-node election")
	}
}

// ---- Multi-node deterministic simulation ----

func peersExcept(all []string, self string) func() []Peer {
	return func() []Peer {
		peers := make([]Peer, 0, len(all)-1)
		for _, id := range all {
			if id != self {
				peers = append(peers, Peer{ID: id, Address: id})
			}
		}
		return peers
	}
}

type simCluster struct {
	ids       []string
	elections map[string]*Election
	ctl       *PartitionController
	cancel    context.CancelFunc
}

func newSimCluster(t *testing.T, ids []string) *simCluster {
	t.Helper()
	transports, ctl := NewLocalCluster(ids)
	elections := make(map[string]*Election, len(ids))
	for _, id := range ids {
		e, err := New(testCfg(id, transports[id], peersExcept(ids, id)))
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}
		transports[id].Wire(e)
		elections[id] = e
	}
	ctx, cancel := context.WithCancel(context.Background())
	sc := &simCluster{ids: ids, elections: elections, ctl: ctl, cancel: cancel}
	for _, e := range elections {
		e.Start(ctx)
	}
	return sc
}

func (sc *simCluster) stop() {
	sc.cancel()
	for _, e := range sc.elections {
		e.Stop()
	}
}

// assertNoSameTermDoubleLeader is the core safety check: it must never be
// possible to observe two distinct nodes both claiming leadership for the
// identical term, at the same instant. This holds by construction (each
// node grants at most one vote per term, and any two quorums intersect),
// so this test would fail immediately if that invariant were broken.
func (sc *simCluster) assertNoSameTermDoubleLeader(t *testing.T) {
	t.Helper()
	leadersByTerm := make(map[uint64]string)
	for id, e := range sc.elections {
		isLeader, term := e.LeaderTerm()
		if !isLeader {
			continue
		}
		if other, exists := leadersByTerm[term]; exists && other != id {
			t.Fatalf("SPLIT BRAIN: both %s and %s claim leadership for term %d", other, id, term)
		}
		leadersByTerm[term] = id
	}
}

func (sc *simCluster) currentLeader() (string, bool) {
	for id, e := range sc.elections {
		if e.IsLeader() {
			return id, true
		}
	}
	return "", false
}

func TestNoTwoLeadersInSameTerm_UnderPartition(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	sc := newSimCluster(t, ids)
	defer sc.stop()

	deadline := time.Now().Add(3 * time.Second)
	var leader string
	for time.Now().Before(deadline) {
		sc.assertNoSameTermDoubleLeader(t)
		if l, ok := sc.currentLeader(); ok {
			leader = l
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if leader == "" {
		t.Fatal("no leader elected within deadline")
	}

	// Isolate the leader from both followers: it becomes a minority of
	// one, unable to reach quorum (2 of 3) for lease renewal.
	for _, id := range ids {
		if id != leader {
			sc.ctl.Partition(leader, id)
		}
	}

	// While partitioned, continuously verify the same-term safety
	// invariant, and separately confirm the isolated ex-leader's lease
	// actually expires (the fencing property: a partitioned leader
	// cannot indefinitely continue believing it holds leadership).
	stepDownDeadline := time.Now().Add(2 * time.Second)
	steppedDown := false
	for time.Now().Before(stepDownDeadline) {
		sc.assertNoSameTermDoubleLeader(t)
		if !sc.elections[leader].IsLeader() {
			steppedDown = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !steppedDown {
		t.Fatalf("isolated leader %s never stepped down after losing quorum (lease not enforced)", leader)
	}

	// The healthy majority (the two non-isolated nodes) must still be
	// able to elect a leader among themselves.
	majorityDeadline := time.Now().Add(3 * time.Second)
	found := false
	for time.Now().Before(majorityDeadline) {
		sc.assertNoSameTermDoubleLeader(t)
		for _, id := range ids {
			if id == leader {
				continue
			}
			if sc.elections[id].IsLeader() {
				found = true
			}
		}
		if found {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !found {
		t.Fatal("majority partition failed to elect a new leader")
	}
}

func TestMinorityPartition_NeverElectsAlone(t *testing.T) {
	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	sc := newSimCluster(t, ids)
	defer sc.stop()

	isolated := "n1"
	for _, id := range ids {
		if id != isolated {
			sc.ctl.Partition(isolated, id)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sc.assertNoSameTermDoubleLeader(t)
		if sc.elections[isolated].IsLeader() {
			t.Fatal("a permanently isolated single node (1 of 5) must never become leader")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The other four nodes form a majority (4 of 5, quorum=3) and must
	// still elect a leader.
	found := false
	for _, id := range ids {
		if id != isolated && sc.elections[id].IsLeader() {
			found = true
		}
	}
	if !found {
		t.Fatal("majority side (4 of 5) failed to elect a leader while one node was isolated")
	}
}

func TestResign_NoOpWhenNotLeader(t *testing.T) {
	e, err := New(testCfg("node-1", noopTransport{}, noPeers))
	if err != nil {
		t.Fatal(err)
	}
	fired := false
	e.OnLoseLeadership(func() { fired = true })

	e.Resign() // never became leader

	if fired {
		t.Error("expected OnLoseLeadership not to fire when Resign is called on a non-leader")
	}
	if e.State() != StateFollower {
		t.Errorf("expected state to remain Follower, got %v", e.State())
	}
}

func TestResign_StepsDownAndFiresCallbackWhenLeader(t *testing.T) {
	// Force leader state directly (as TestHandleHeartbeat_DemotesLeaderAndFiresCallback
	// does above) rather than starting the ticking election loop — a solo
	// node with no peers would just re-elect itself again within one
	// electionTimeout, racing the very assertion this test makes.
	e, err := New(testCfg("solo", noopTransport{}, noPeers))
	if err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.state = StateLeader
	e.term = 7
	e.mu.Unlock()

	lost := make(chan struct{}, 1)
	e.OnLoseLeadership(func() { lost <- struct{}{} })

	e.Resign()

	select {
	case <-lost:
	case <-time.After(time.Second):
		t.Fatal("expected OnLoseLeadership to fire after Resign")
	}
	if e.IsLeader() {
		t.Error("expected IsLeader() false immediately after Resign")
	}
	if e.Term() != 7 {
		t.Errorf("expected Resign not to bump the term (newTerm=0 means no change), got %d want 7", e.Term())
	}
}
