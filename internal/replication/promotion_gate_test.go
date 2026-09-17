package replication

import (
	"context"
	"testing"
	"time"
)

func TestPromoteBestReplicaFromWithGate_PromotesImmediatelyUnderThreshold(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.SetStatus("r1", ReplicaActive)
	rs.UpdateLag("r1", 5)

	p := NewPromotion(rs)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	node, lossy, err := p.PromoteBestReplicaFromWithGate(ctx, "r1", 100, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node != "r1" {
		t.Errorf("expected r1 promoted, got %s", node)
	}
	if lossy {
		t.Error("expected a clean (non-lossy) promotion when lag is under threshold")
	}
	if p.LossyPromotions() != 0 {
		t.Errorf("expected 0 lossy promotions, got %d", p.LossyPromotions())
	}
}

func TestPromoteBestReplicaFromWithGate_WaitsThenPromotesCleanlyOnceLagDrops(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.SetStatus("r1", ReplicaActive)
	rs.UpdateLag("r1", 500) // starts well above threshold

	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(60 * time.Millisecond)
		rs.UpdateLag("r1", 5) // catches up before the gate times out
	}()
	t.Cleanup(func() { <-done })

	p := NewPromotion(rs)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	node, lossy, err := p.PromoteBestReplicaFromWithGate(ctx, "r1", 100, 10*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node != "r1" {
		t.Errorf("expected r1 promoted, got %s", node)
	}
	if lossy {
		t.Error("expected a clean promotion once lag dropped under threshold")
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("expected the gate to actually wait for lag to drop, only waited %v", elapsed)
	}
}

func TestPromoteBestReplicaFromWithGate_TimesOutAndPromotesLossy(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.SetStatus("r1", ReplicaActive)
	rs.UpdateLag("r1", 9999) // never drops below threshold

	p := NewPromotion(rs)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	node, lossy, err := p.PromoteBestReplicaFromWithGate(ctx, "r1", 100, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if node != "r1" {
		t.Errorf("expected r1 promoted anyway (only candidate), got %s", node)
	}
	if !lossy {
		t.Error("expected a lossy promotion when the gate times out with lag still over threshold")
	}
	if p.LossyPromotions() != 1 {
		t.Errorf("expected 1 lossy promotion recorded, got %d", p.LossyPromotions())
	}
}

func TestPromoteBestReplicaFromWithGate_NoCandidateEverErrors(t *testing.T) {
	rs := NewReplicaSet("primary-1") // no replicas at all
	p := NewPromotion(rs)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, err := p.PromoteBestReplicaFromWithGate(ctx, "r1", 100, 10*time.Millisecond)
	if err != ErrNoReplicaAvailable {
		t.Errorf("expected ErrNoReplicaAvailable, got %v", err)
	}
}
