package network

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hydracache/hydracache/internal/election"
)

// ElectionTransport implements election.Transport over the existing TCP
// wire protocol (ELECTION_VOTE / ELECTION_HEARTBEAT commands), reusing
// Client for connection handling. Every call is bounded by ctx's
// deadline, defaulting to 2s if the caller supplied none, so a hung or
// unreachable peer can never stall an election or lease renewal.
type ElectionTransport struct{}

func NewElectionTransport() *ElectionTransport { return &ElectionTransport{} }

func (t *ElectionTransport) RequestVote(ctx context.Context, peer election.Peer, req election.VoteRequest) (election.VoteResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return election.VoteResponse{}, fmt.Errorf("election transport: marshal vote request: %w", err)
	}
	raw, err := t.call(ctx, peer.Address, "ELECTION_VOTE", string(payload))
	if err != nil {
		return election.VoteResponse{}, err
	}
	var resp election.VoteResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return election.VoteResponse{}, fmt.Errorf("election transport: unmarshal vote response from %s: %w", peer.ID, err)
	}
	return resp, nil
}

func (t *ElectionTransport) SendHeartbeat(ctx context.Context, peer election.Peer, req election.HeartbeatRequest) (election.HeartbeatResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return election.HeartbeatResponse{}, fmt.Errorf("election transport: marshal heartbeat: %w", err)
	}
	raw, err := t.call(ctx, peer.Address, "ELECTION_HEARTBEAT", string(payload))
	if err != nil {
		return election.HeartbeatResponse{}, err
	}
	var resp election.HeartbeatResponse
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		return election.HeartbeatResponse{}, fmt.Errorf("election transport: unmarshal heartbeat response from %s: %w", peer.ID, err)
	}
	return resp, nil
}

func (t *ElectionTransport) call(ctx context.Context, addr, cmd, payload string) (string, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Second)
	}
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return "", ctx.Err()
	}

	client := NewClientWithTimeout(addr, timeout)
	if err := client.Connect(); err != nil {
		return "", fmt.Errorf("election transport: dial %s: %w", addr, err)
	}
	defer client.Close()

	if err := client.SetDeadline(deadline); err != nil {
		return "", fmt.Errorf("election transport: set deadline for %s: %w", addr, err)
	}

	resp, err := client.Send(cmd, payload)
	if err != nil {
		return "", fmt.Errorf("election transport: %s to %s: %w", cmd, addr, err)
	}
	return resp, nil
}
