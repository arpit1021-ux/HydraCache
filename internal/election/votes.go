package election

// VoteRequest is sent by a candidate to a peer, either as a non-binding
// PreVote probe (PreVote=true, does not require the responder to persist
// or mutate any state) or as a real, binding vote request.
type VoteRequest struct {
	CandidateID string
	Term        uint64
	PreVote     bool
}

// VoteResponse is a peer's answer to a VoteRequest.
type VoteResponse struct {
	VoterID string
	Term    uint64
	Granted bool
}

// HeartbeatRequest is sent by the current leader to assert its term and
// renew its lease with followers.
type HeartbeatRequest struct {
	LeaderID string
	Term     uint64
}

// HeartbeatResponse tells the leader whether the follower accepted the
// heartbeat (Success) and, if not, the higher term the follower is
// already aware of so the leader can step down promptly.
type HeartbeatResponse struct {
	Term    uint64
	Success bool
}
