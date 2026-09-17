package network

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/hashring"
	"github.com/hydracache/hydracache/internal/replication"
)

// newReplPair wires a primary and a replica Server together over real TCP
// the way cmd/server/main.go does, so REPLICATE/REPLICA_SYNC exercise the
// actual wire protocol rather than in-process method calls.
func newReplPair(t *testing.T) (primary, replica *Server, rsPrimary *replication.ReplicaSet, replicaCache cache.Cache, primaryClient *Client, replicaAddr string) {
	t.Helper()

	primaryCache := newTestCache()
	replicaCache = newTestCache()

	ring := hashring.New(4)
	ring.AddNode("primary-1")
	locator := hashring.NewLocator(ring, 2)

	primary = NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10}, primaryCache)
	if err := primary.Start(context.Background()); err != nil {
		t.Fatalf("primary start: %v", err)
	}
	t.Cleanup(primary.Shutdown)

	replica = NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10}, replicaCache)
	if err := replica.Start(context.Background()); err != nil {
		t.Fatalf("replica start: %v", err)
	}
	t.Cleanup(replica.Shutdown)

	primaryAddr := primary.Addr().String()
	replicaAddr = replica.Addr().String()

	primaryRegistry := replication.NewReplicaRegistry()
	rsPrimary = replication.NewReplicaSet("primary-1")
	rsPrimary.AddReplica("primary-1", primaryAddr)
	rsPrimary.SetStatus("primary-1", replication.ReplicaActive)
	rsPrimary.AddReplica("replica-1", replicaAddr)
	rsPrimary.SetStatus("replica-1", replication.ReplicaActive)
	primaryRegistry.Register("primary-1", rsPrimary)
	primary.SetReplication("primary-1", primaryRegistry, locator)

	replicaRegistry := replication.NewReplicaRegistry()
	rsReplica := replication.NewReplicaSet("primary-1")
	rsReplica.AddReplica("primary-1", primaryAddr)
	rsReplica.SetStatus("primary-1", replication.ReplicaActive)
	replicaRegistry.Register("primary-1", rsReplica)
	replica.SetReplication("replica-1", replicaRegistry, nil)

	primaryClient = NewClient(primaryAddr)
	if err := primaryClient.Connect(); err != nil {
		t.Fatalf("primary client connect: %v", err)
	}
	t.Cleanup(func() { primaryClient.Close() })

	return primary, replica, rsPrimary, replicaCache, primaryClient, replicaAddr
}

func TestReplication_SyncMode_WaitsForReplicaAckAndTracksLag(t *testing.T) {
	primary, _, rsPrimary, replicaCache, pc, _ := newReplPair(t)
	primary.SetReplicationMode(ReplicationModeSync, 1, 2*time.Second)

	resp, err := pc.Send("SET", "k1", "v1")
	if err != nil {
		t.Fatalf("SET: %v", err)
	}
	if resp != "OK" {
		t.Fatalf("SET response = %q, want OK", resp)
	}

	val, err := replicaCache.Get("k1")
	if err != nil {
		t.Fatalf("replica should already have k1 by the time a sync SET returns: %v", err)
	}
	if string(val) != "v1" {
		t.Errorf("replica value = %q, want v1", val)
	}

	info, ok := rsPrimary.GetReplica("replica-1")
	if !ok {
		t.Fatal("replica-1 not found in primary's replica set")
	}
	if info.GetLagSeq() != 0 {
		t.Errorf("expected lag 0 immediately after a fully-acked sync write, got %d", info.GetLagSeq())
	}
}

func TestReplication_SyncMode_FailsWhenReplicaUnreachable(t *testing.T) {
	primary, replica, _, _, pc, _ := newReplPair(t)
	primary.SetReplicationMode(ReplicationModeSync, 1, 500*time.Millisecond)
	replica.Shutdown()

	_, err := pc.Send("SET", "k1", "v1")
	if err == nil {
		t.Fatal("expected sync SET to fail (as an error to the client) when the replica is unreachable")
	}
}

func TestReplication_AsyncMode_SucceedsImmediatelyEvenIfReplicaDown(t *testing.T) {
	primary, replica, _, _, pc, _ := newReplPair(t)
	_ = primary // default mode is async — SetReplicationMode never called
	replica.Shutdown()

	start := time.Now()
	resp, err := pc.Send("SET", "k1", "v1")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("async SET should succeed locally even if the replica is down: %v", err)
	}
	if resp != "OK" {
		t.Fatalf("resp = %q, want OK", resp)
	}
	if elapsed > time.Second {
		t.Errorf("async SET should return without waiting on replication, took %v", elapsed)
	}
}

func TestReplication_GapDetection_CatchesUpMissingOp(t *testing.T) {
	_, _, rsPrimary, replicaCache, _, replicaAddr := newReplPair(t)

	primaryInfo, ok := rsPrimary.GetReplica("primary-1")
	if !ok || primaryInfo.Stream == nil {
		t.Fatal("primary's own stream entry not found")
	}
	stream := primaryInfo.Stream

	// Seq 1 establishes the replica's applied-seq baseline (the very
	// first op for a shard is never itself treated as "a gap" — that
	// initial-sync boundary is a separate, already-existing mechanism).
	op1 := replication.Operation{Command: "SET", Args: []string{"k0", "v0"}, NodeID: "primary-1"}
	op1.Seq = stream.Append(op1)
	// Seq 2 is "lost" — appended to the primary's stream (so REPLICA_SYNC
	// can serve it) but never delivered directly to the replica.
	op2 := replication.Operation{Command: "SET", Args: []string{"k1", "v1"}, NodeID: "primary-1"}
	op2.Seq = stream.Append(op2)
	// Seq 3 is delivered directly, skipping straight past the gap at 2.
	op3 := replication.Operation{Command: "SET", Args: []string{"k2", "v2"}, NodeID: "primary-1"}
	op3.Seq = stream.Append(op3)

	rc := NewClient(replicaAddr)
	if err := rc.Connect(); err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	deliver := func(op replication.Operation) {
		t.Helper()
		payload, _ := json.Marshal(op)
		resp, err := rc.Send("REPLICATE", string(payload))
		if err != nil {
			t.Fatalf("REPLICATE seq=%d: %v", op.Seq, err)
		}
		if resp != "OK" {
			t.Fatalf("REPLICATE seq=%d resp = %q, want OK", op.Seq, resp)
		}
	}

	deliver(op1) // establishes baseline seq=1
	deliver(op3) // skips seq=2 — must trigger gap catch-up for op2

	v0, err := replicaCache.Get("k0")
	if err != nil || string(v0) != "v0" {
		t.Errorf("expected baseline k0=v0 applied, got val=%q err=%v", v0, err)
	}
	v1, err := replicaCache.Get("k1")
	if err != nil || string(v1) != "v1" {
		t.Errorf("expected gap catch-up to have pulled and applied k1=v1 (seq=2), got val=%q err=%v", v1, err)
	}
	v2, err := replicaCache.Get("k2")
	if err != nil || string(v2) != "v2" {
		t.Errorf("expected k2=v2 (seq=3) applied directly, got val=%q err=%v", v2, err)
	}
}

func TestReplication_StaleOutOfOrderOpNeverClobbersNewerValue(t *testing.T) {
	_, _, rsPrimary, replicaCache, _, replicaAddr := newReplPair(t)

	primaryInfo, _ := rsPrimary.GetReplica("primary-1")
	stream := primaryInfo.Stream

	op1 := replication.Operation{Command: "SET", Args: []string{"k", "old"}, NodeID: "primary-1"}
	op1.Seq = stream.Append(op1)
	op2 := replication.Operation{Command: "SET", Args: []string{"k", "new"}, NodeID: "primary-1"}
	op2.Seq = stream.Append(op2)

	rc := NewClient(replicaAddr)
	if err := rc.Connect(); err != nil {
		t.Fatal(err)
	}
	defer rc.Close()

	// Deliver the NEWER op first, then the older one out of order.
	p2, _ := json.Marshal(op2)
	if _, err := rc.Send("REPLICATE", string(p2)); err != nil {
		t.Fatalf("REPLICATE op2: %v", err)
	}
	p1, _ := json.Marshal(op1)
	if _, err := rc.Send("REPLICATE", string(p1)); err != nil {
		t.Fatalf("REPLICATE op1: %v", err)
	}

	val, err := replicaCache.Get("k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "new" {
		t.Errorf("stale out-of-order op1 must not clobber the newer op2; got %q, want %q", val, "new")
	}
}

func TestReplication_FlushAllDoesNotPanicWithReplicationConfigured(t *testing.T) {
	_, _, _, _, pc, _ := newReplPair(t)
	if _, err := pc.Send("FLUSHALL"); err != nil {
		t.Fatalf("FLUSHALL: %v", err)
	}
}
