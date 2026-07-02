package election

import (
	"testing"
	"time"
)

func TestNewElection(t *testing.T) {
	e := New("node-1", 3)
	if e.State() != StateFollower {
		t.Errorf("expected follower state, got %v", e.State())
	}
	if e.Term() != 0 {
		t.Errorf("expected term 0, got %d", e.Term())
	}
}

func TestElectionQuorum(t *testing.T) {
	e := New("node-1", 5)
	if e.Quorum() != 3 {
		t.Errorf("expected quorum 3, got %d", e.Quorum())
	}

	e2 := New("node-1", 3)
	if e2.Quorum() != 2 {
		t.Errorf("expected quorum 2, got %d", e2.Quorum())
	}
}

func TestHandleVoteRequest(t *testing.T) {
	e := New("node-1", 3)

	granted, term := e.HandleVoteRequest("node-2", 1)
	if !granted {
		t.Error("expected vote to be granted")
	}
	if term != 1 {
		t.Errorf("expected term 1, got %d", term)
	}

	granted, _ = e.HandleVoteRequest("node-3", 1)
	if granted {
		t.Error("expected vote to be denied (already voted in this term)")
	}
}

func TestHandleVoteHigherTerm(t *testing.T) {
	e := New("node-1", 3)

	granted, term := e.HandleVoteRequest("node-2", 5)
	if !granted {
		t.Error("expected vote to be granted for higher term")
	}
	if term != 5 {
		t.Errorf("expected term 5, got %d", term)
	}
	if e.Term() != 5 {
		t.Errorf("expected election term to update to 5, got %d", e.Term())
	}
}

func TestHandleHeartbeat(t *testing.T) {
	e := New("node-1", 3)
	e.HandleHeartbeat("leader-1", 1)

	if e.State() != StateFollower {
		t.Errorf("expected follower after heartbeat, got %v", e.State())
	}
}

func TestBecomeLeader(t *testing.T) {
	e := New("node-1", 1)
	leaderCh := make(chan bool, 1)
	e.OnBecomeLeader(func() {
		leaderCh <- true
	})

	e.Start()
	defer e.Stop()

	select {
	case <-leaderCh:
		if !e.IsLeader() {
			t.Error("expected to be leader")
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for election")
	}
}
