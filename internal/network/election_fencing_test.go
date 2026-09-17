package network

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/hydracache/hydracache/internal/election"
	"github.com/hydracache/hydracache/internal/protocol"
	"github.com/hydracache/hydracache/internal/replication"
)

// noopElectionTransport never reaches a peer; used where a test only
// exercises the RPC-receiving side (Handler dispatch), not real election
// networking.
type noopElectionTransport struct{}

func (noopElectionTransport) RequestVote(ctx context.Context, p election.Peer, r election.VoteRequest) (election.VoteResponse, error) {
	return election.VoteResponse{}, errors.New("noopElectionTransport: no peers")
}

func (noopElectionTransport) SendHeartbeat(ctx context.Context, p election.Peer, r election.HeartbeatRequest) (election.HeartbeatResponse, error) {
	return election.HeartbeatResponse{}, errors.New("noopElectionTransport: no peers")
}

// bulkStringJSON decodes the JSON payload out of a RESP bulk-string
// response ("$N\r\n<json>\r\n"), matching the format handleElectionVote /
// handleElectionHeartbeat / handleGossip encode their replies in.
func bulkStringJSON(t *testing.T, data []byte, v interface{}) {
	t.Helper()
	s := string(data)
	idx := strings.Index(s, "\r\n")
	if idx < 0 {
		t.Fatalf("malformed bulk string response: %q", s)
	}
	body := strings.TrimSuffix(s[idx+2:], "\r\n")
	if err := json.Unmarshal([]byte(body), v); err != nil {
		t.Fatalf("unmarshal response body %q: %v", body, err)
	}
}

func TestHandler_ElectionVote_NotConfigured(t *testing.T) {
	h := NewHandler(newTestCache())
	resp := h.Handle(&protocol.Command{Name: "ELECTION_VOTE", Args: []string{"{}"}})
	if resp.err == nil {
		t.Fatal("expected error when election is not configured")
	}
}

func TestHandler_ElectionVote_GrantsAndPersistsRealVote(t *testing.T) {
	h := NewHandler(newTestCache())
	e, err := election.New(election.Config{
		SelfID:    "node-1",
		Store:     election.NewMemoryTermStore(),
		Transport: noopElectionTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetElection(e)

	req := election.VoteRequest{CandidateID: "node-2", Term: 1}
	payload, _ := json.Marshal(req)
	resp := h.Handle(&protocol.Command{Name: "ELECTION_VOTE", Args: []string{string(payload)}})
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}

	var vr election.VoteResponse
	bulkStringJSON(t, resp.data, &vr)
	if !vr.Granted {
		t.Error("expected vote to be granted")
	}
	if e.Term() != 1 {
		t.Errorf("expected election term advanced to 1, got %d", e.Term())
	}
}

func TestHandler_ElectionHeartbeat_Dispatch(t *testing.T) {
	h := NewHandler(newTestCache())
	e, err := election.New(election.Config{
		SelfID:    "node-1",
		Store:     election.NewMemoryTermStore(),
		Transport: noopElectionTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.SetElection(e)

	req := election.HeartbeatRequest{LeaderID: "node-2", Term: 3}
	payload, _ := json.Marshal(req)
	resp := h.Handle(&protocol.Command{Name: "ELECTION_HEARTBEAT", Args: []string{string(payload)}})
	if resp.err != nil {
		t.Fatalf("unexpected error: %v", resp.err)
	}

	var hr election.HeartbeatResponse
	bulkStringJSON(t, resp.data, &hr)
	if !hr.Success {
		t.Error("expected heartbeat to succeed")
	}
	if e.State() != election.StateFollower {
		t.Errorf("expected follower state after heartbeat, got %v", e.State())
	}
}

func TestHandler_Replicate_RejectsStaleEpoch(t *testing.T) {
	c := newTestCache()
	registry := replication.NewReplicaRegistry()
	rs := replication.NewReplicaSet("primary-1")
	registry.Register("primary-1", rs)

	h := NewHandler(c)
	h.SetReplication("replica-1", registry, nil)

	fresh := replication.Operation{Command: "SET", Args: []string{"k", "v"}, NodeID: "primary-1", Epoch: 5}
	payload, _ := json.Marshal(fresh)
	resp := h.Handle(&protocol.Command{Name: "REPLICATE", Args: []string{string(payload)}})
	if resp.err != nil {
		t.Fatalf("expected epoch 5 to be accepted, got error: %v", resp.err)
	}

	stale := replication.Operation{Command: "SET", Args: []string{"k", "stale"}, NodeID: "primary-1", Epoch: 3}
	payload2, _ := json.Marshal(stale)
	resp2 := h.Handle(&protocol.Command{Name: "REPLICATE", Args: []string{string(payload2)}})
	if resp2.err == nil {
		t.Fatal("expected stale epoch 3 to be rejected after epoch 5 was already seen")
	}

	val, err := c.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "v" {
		t.Errorf("stale REPLICATE op must not have applied; got %q, want %q", val, "v")
	}
}

func TestHandler_Replicate_UnknownShardIsAllowedThrough(t *testing.T) {
	c := newTestCache()
	registry := replication.NewReplicaRegistry() // no shard registered at all
	h := NewHandler(c)
	h.SetReplication("replica-1", registry, nil)

	op := replication.Operation{Command: "SET", Args: []string{"k", "v"}, NodeID: "unknown-primary", Epoch: 0}
	payload, _ := json.Marshal(op)
	resp := h.Handle(&protocol.Command{Name: "REPLICATE", Args: []string{string(payload)}})
	if resp.err != nil {
		t.Fatalf("expected op for an unregistered shard to pass through, got error: %v", resp.err)
	}
}
