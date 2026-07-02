package election

import (
	"sync"
)

type VoteRequest struct {
	CandidateID string
	Term        uint64
}

type VoteResponse struct {
	VoterID string
	Term    uint64
	Granted bool
}

type HeartbeatRequest struct {
	LeaderID string
	Term     uint64
}

type VoteTracker struct {
	mu    sync.RWMutex
	votes map[string]map[string]VoteResponse
}

func NewVoteTracker() *VoteTracker {
	return &VoteTracker{
		votes: make(map[string]map[string]VoteResponse),
	}
}

func (vt *VoteTracker) RecordVote(candidateID string, resp VoteResponse) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	if vt.votes[candidateID] == nil {
		vt.votes[candidateID] = make(map[string]VoteResponse)
	}
	vt.votes[candidateID][resp.VoterID] = resp
}

func (vt *VoteTracker) VotesFor(candidateID string) (granted, denied int) {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	for _, resp := range vt.votes[candidateID] {
		if resp.Granted {
			granted++
		} else {
			denied++
		}
	}
	return
}

func (vt *VoteTracker) Clear(term uint64) {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	vt.votes = make(map[string]map[string]VoteResponse)
}
