package replication

import (
	"testing"
)

func TestReplicaRegistry_RegisterAndGet(t *testing.T) {
	rr := NewReplicaRegistry()
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")

	rr.Register("primary-1", rs)

	got, ok := rr.GetReplicaSet("primary-1")
	if !ok {
		t.Fatal("GetReplicaSet should find registered primary")
	}
	if got.ReplicaCount() != 2 {
		t.Errorf("ReplicaCount = %d, want 2", got.ReplicaCount())
	}

	promo, ok := rr.GetPromotion("primary-1")
	if !ok {
		t.Fatal("GetPromotion should find promotion for registered primary")
	}
	if promo == nil {
		t.Fatal("Promotion should not be nil")
	}
	if promo.IsPromoted() {
		t.Error("should not be promoted initially")
	}
}

func TestReplicaRegistry_Unregister(t *testing.T) {
	rr := NewReplicaRegistry()
	rs := NewReplicaSet("primary-1")
	rr.Register("primary-1", rs)
	rr.Unregister("primary-1")

	_, ok := rr.GetReplicaSet("primary-1")
	if ok {
		t.Error("GetReplicaSet should return false after Unregister")
	}
}

func TestReplicaRegistry_FindPrimaryForReplica(t *testing.T) {
	rr := NewReplicaRegistry()
	rs1 := NewReplicaSet("primary-1")
	rs1.AddReplica("r1", "addr1")
	rs1.AddReplica("r2", "addr2")
	rr.Register("primary-1", rs1)

	rs2 := NewReplicaSet("primary-2")
	rs2.AddReplica("r3", "addr3")
	rr.Register("primary-2", rs2)

	if got := rr.FindPrimaryForReplica("r1"); got != "primary-1" {
		t.Errorf("FindPrimaryForReplica(r1) = %q, want primary-1", got)
	}
	if got := rr.FindPrimaryForReplica("r3"); got != "primary-2" {
		t.Errorf("FindPrimaryForReplica(r3) = %q, want primary-2", got)
	}
	if got := rr.FindPrimaryForReplica("nonexistent"); got != "" {
		t.Errorf("FindPrimaryForReplica(nonexistent) = %q, want empty", got)
	}
}

func TestReplicaRegistry_GetReplicaSet_NotFound(t *testing.T) {
	rr := NewReplicaRegistry()
	_, ok := rr.GetReplicaSet("nonexistent")
	if ok {
		t.Error("GetReplicaSet should return false for unregistered primary")
	}
}

func TestReplicaRegistry_Overwrite(t *testing.T) {
	rr := NewReplicaRegistry()
	rs1 := NewReplicaSet("primary-1")
	rs1.AddReplica("r1", "addr1")
	rr.Register("primary-1", rs1)

	rs2 := NewReplicaSet("primary-1")
	rs2.AddReplica("r2", "addr2")
	rs2.AddReplica("r3", "addr3")
	rr.Register("primary-1", rs2)

	got, ok := rr.GetReplicaSet("primary-1")
	if !ok {
		t.Fatal("GetReplicaSet should find overwritten primary")
	}
	if got.ReplicaCount() != 2 {
		t.Errorf("ReplicaCount = %d, want 2 (overwritten)", got.ReplicaCount())
	}
}

func TestBestReplicaFrom(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")
	rs.AddReplica("r3", "addr3")

	rs.UpdateLag("r1", 5)
	rs.UpdateLag("r2", 1)
	rs.UpdateLag("r3", 50)

	// r2 has lowest lag but r1 is the ring-successor candidate.
	best := rs.BestReplicaFrom("r1")
	if best == nil {
		t.Fatal("BestReplicaFrom should return a replica")
	}
	if best.NodeID != "r1" {
		t.Errorf("BestReplicaFrom = %q, want r1 (the ring-successor candidate)", best.NodeID)
	}

	// If the ring-successor is not in the set, return nil.
	best = rs.BestReplicaFrom("nonexistent")
	if best != nil {
		t.Errorf("BestReplicaFrom(nonexistent) = %v, want nil", best)
	}
}

func TestBestReplicaFrom_FailedReplica(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	r1, _ := rs.GetReplica("r1")
	r1.SetStatus(ReplicaFailed)

	best := rs.BestReplicaFrom("r1")
	if best != nil {
		t.Errorf("BestReplicaFrom should return nil for failed replica, got %v", best)
	}
}

func TestPromoteBestReplicaFrom(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")
	rs.UpdateLag("r1", 50)
	rs.UpdateLag("r2", 5)

	p := NewPromotion(rs)
	// r1 is the ring-successor, even though r2 has lower lag.
	node, err := p.PromoteBestReplicaFrom("r1")
	if err != nil {
		t.Fatalf("PromoteBestReplicaFrom: %v", err)
	}
	if node != "r1" {
		t.Errorf("promoted = %q, want r1 (ring-successor)", node)
	}
	if !p.IsPromoted() {
		t.Error("should be promoted")
	}
}
