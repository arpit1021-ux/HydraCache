package replication

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hydracache/hydracache/internal/hashring"
)

func TestReplicaStatus_String(t *testing.T) {
	tests := []struct {
		s    ReplicaStatus
		want string
	}{
		{ReplicaSyncing, "syncing"},
		{ReplicaActive, "active"},
		{ReplicaLagging, "lagging"},
		{ReplicaFailed, "failed"},
		{ReplicaStatus(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("ReplicaStatus(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}

func TestNewReplicaSet(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	if rs.primaryID != "primary-1" {
		t.Errorf("primaryID = %q, want primary-1", rs.primaryID)
	}
	if rs.ReplicaCount() != 0 {
		t.Errorf("ReplicaCount = %d, want 0", rs.ReplicaCount())
	}
}

func TestReplicaSet_AddAndGetReplica(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("replica-1", "10.0.0.1:7000")

	r, ok := rs.GetReplica("replica-1")
	if !ok {
		t.Fatal("GetReplica should find added replica")
	}
	if r.NodeID != "replica-1" {
		t.Errorf("NodeID = %q", r.NodeID)
	}
	if r.Address != "10.0.0.1:7000" {
		t.Errorf("Address = %q", r.Address)
	}
	if r.GetStatus() != ReplicaSyncing {
		t.Errorf("Status = %v, want ReplicaSyncing (default before sync completes)", r.GetStatus())
	}
	if r.Stream == nil {
		t.Error("Stream should be initialized")
	}
}

func TestReplicaSet_RemoveReplica(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")
	rs.RemoveReplica("r1")

	if rs.ReplicaCount() != 1 {
		t.Errorf("ReplicaCount = %d, want 1", rs.ReplicaCount())
	}
	_, ok := rs.GetReplica("r1")
	if ok {
		t.Error("removed replica should not be found")
	}
}

func TestReplicaSet_GetReplica_NotFound(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	_, ok := rs.GetReplica("nonexistent")
	if ok {
		t.Error("GetReplica should return false for missing replica")
	}
}

func TestReplicaSet_RemoveReplica_Nonexistent(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.RemoveReplica("ghost")
	if rs.ReplicaCount() != 0 {
		t.Error("removing nonexistent replica should be no-op")
	}
}

func TestReplicaSet_ActiveReplicas(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")
	rs.AddReplica("r3", "addr3")

	r1, _ := rs.GetReplica("r1")
	r1.SetStatus(ReplicaActive)
	r2, _ := rs.GetReplica("r2")
	r2.SetStatus(ReplicaLagging)
	r3, _ := rs.GetReplica("r3")
	r3.SetStatus(ReplicaSyncing) // explicitly set to non-active for this test

	active := rs.ActiveReplicas()
	if len(active) != 1 {
		t.Errorf("ActiveReplicas len = %d, want 1", len(active))
	}
	if len(active) > 0 && active[0].NodeID != "r1" {
		t.Errorf("active replica = %q, want r1", active[0].NodeID)
	}
}

func TestReplicaSet_BestReplica(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")
	rs.AddReplica("r3", "addr3")
	// Simulate post-sync: mark active before UpdateLag.
	rs.SetStatus("r1", ReplicaActive)
	rs.SetStatus("r2", ReplicaActive)
	rs.SetStatus("r3", ReplicaActive)

	rs.UpdateLag("r1", 50)
	rs.UpdateLag("r2", 10)
	rs.UpdateLag("r3", 100)

	best := rs.BestReplica()
	if best == nil {
		t.Fatal("BestReplica should not be nil")
	}
	if best.NodeID != "r2" {
		t.Errorf("BestReplica = %q, want r2 (lowest lag)", best.NodeID)
	}
}

func TestReplicaSet_BestReplica_ExcludesFailed(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")
	rs.SetStatus("r1", ReplicaActive) // post-sync

	rs.UpdateLag("r1", 5)

	r2, _ := rs.GetReplica("r2")
	r2.SetStatus(ReplicaFailed)

	best := rs.BestReplica()
	if best == nil || best.NodeID != "r1" {
		t.Errorf("BestReplica should skip failed, got %v", best)
	}
}

func TestReplicaSet_BestReplica_AllFailed(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	r1, _ := rs.GetReplica("r1")
	r1.SetStatus(ReplicaFailed)

	if rs.BestReplica() != nil {
		t.Error("BestReplica should return nil when all replicas are failed")
	}
}

func TestReplicaSet_BestReplica_Empty(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	if rs.BestReplica() != nil {
		t.Error("BestReplica on empty set should return nil")
	}
}

func TestReplicaSet_UpdateLag_TransitionsStatus(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.SetStatus("r1", ReplicaActive) // post-sync: mark active before testing lag transitions

	rs.UpdateLag("r1", 10)
	r, _ := rs.GetReplica("r1")
	if r.GetStatus() != ReplicaActive {
		t.Errorf("after small lag, status = %v, want ReplicaActive", r.GetStatus())
	}

	rs.UpdateLag("r1", 200)
	r, _ = rs.GetReplica("r1")
	if r.GetStatus() != ReplicaLagging {
		t.Errorf("after large lag, status = %v, want ReplicaLagging", r.GetStatus())
	}

	rs.UpdateLag("r1", 50)
	r, _ = rs.GetReplica("r1")
	if r.GetStatus() != ReplicaActive {
		t.Errorf("after lag recovery, status = %v, want ReplicaActive", r.GetStatus())
	}
}

func TestReplicaSet_UpdateLag_Nonexistent(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.UpdateLag("ghost", 100)
	if rs.ReplicaCount() != 0 {
		t.Error("UpdateLag on nonexistent should be no-op")
	}
}

func TestReplicaSet_UpdateLag_RecordsTimestamp(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")

	before := time.Now()
	rs.UpdateLag("r1", 10)
	after := time.Now()

	r, _ := rs.GetReplica("r1")
	lastSync := r.GetLastSync()
	if lastSync.Before(before) || lastSync.After(after) {
		t.Errorf("LastSync = %v not between %v and %v", lastSync, before, after)
	}
}

func TestReplicaSet_LagInfo(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")

	rs.UpdateLag("r1", 10)
	rs.UpdateLag("r2", 20)

	lags := rs.LagInfo()
	if lags["r1"] != 10 {
		t.Errorf("lag[r1] = %d, want 10", lags["r1"])
	}
	if lags["r2"] != 20 {
		t.Errorf("lag[r2] = %d, want 20", lags["r2"])
	}
}

func TestReplicaSet_LagInfo_Empty(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	lags := rs.LagInfo()
	if len(lags) != 0 {
		t.Errorf("LagInfo should be empty, got %v", lags)
	}
}

func TestReplicaSet_ReplicaCount(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	if rs.ReplicaCount() != 0 {
		t.Errorf("initial count = %d", rs.ReplicaCount())
	}
	rs.AddReplica("r1", "a1")
	rs.AddReplica("r2", "a2")
	if rs.ReplicaCount() != 2 {
		t.Errorf("after adds, count = %d", rs.ReplicaCount())
	}
	rs.RemoveReplica("r1")
	if rs.ReplicaCount() != 1 {
		t.Errorf("after remove, count = %d", rs.ReplicaCount())
	}
}

func TestReplicaSet_SetStatus(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")

	r, _ := rs.GetReplica("r1")
	if r.GetStatus() != ReplicaSyncing {
		t.Errorf("initial status = %v, want ReplicaSyncing", r.GetStatus())
	}

	rs.SetStatus("r1", ReplicaActive)
	if r.GetStatus() != ReplicaActive {
		t.Errorf("after SetStatus, status = %v, want ReplicaActive", r.GetStatus())
	}

	rs.SetStatus("r1", ReplicaFailed)
	if r.GetStatus() != ReplicaFailed {
		t.Errorf("after SetStatus(Failed), status = %v, want ReplicaFailed", r.GetStatus())
	}
}

func TestReplicaSet_SetStatus_Nonexistent(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.SetStatus("ghost", ReplicaActive) // no-op, no panic
}

func TestReplicaSet_ConcurrentAddRemoveAndQuery(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 4)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			rs.AddReplica("r", "addr")
		}(i)
		go func(id int) {
			defer wg.Done()
			rs.RemoveReplica("r")
		}(i)
		go func() {
			defer wg.Done()
			_ = rs.ReplicaCount()
		}()
		go func() {
			defer wg.Done()
			_ = rs.ActiveReplicas()
		}()
	}
	wg.Wait()
}

func TestReplicaSet_ConcurrentUpdateLag(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			rs.UpdateLag("r1", int64(id))
		}(i)
	}
	wg.Wait()

	lags := rs.LagInfo()
	if lags["r1"] < 0 || lags["r1"] >= int64(goroutines) {
		t.Errorf("final lag = %d, out of expected range", lags["r1"])
	}
}

func TestReplicaSet_ConcurrentUpdateLagAndPromote(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")

	p := NewPromotion(rs)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Half the goroutines update lag (which writes Status, LagSeq, LastSync under rs.mu)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				rs.UpdateLag("r1", int64(id+j))
				rs.UpdateLag("r2", int64(id+j+50))
			}
		}(i)
	}

	// The other half promote (which calls SetStatus under p.mu → rs.mu)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				p.PromoteBestReplica()
			}
		}()
	}

	wg.Wait()

	// After all goroutines finish, the promoted node must be Active
	// (either through UpdateLag's status transitions or Promotion's SetStatus).
	if !p.IsPromoted() {
		t.Error("should be promoted after concurrent UpdateLag+Promote")
	}
	promoted := p.PromotedNode()
	r, ok := rs.GetReplica(promoted)
	if !ok {
		t.Fatalf("promoted node %s not found in replica set", promoted)
	}
	if r.GetStatus() != ReplicaActive {
		t.Errorf("promoted node status = %v, want ReplicaActive", r.GetStatus())
	}
}

func TestLagTracker_NewLagTracker(t *testing.T) {
	lt := NewLagTracker()
	if lt.AverageLag("nope") != 0 {
		t.Error("AverageLag on empty should return 0")
	}
	if lt.MaxLag("nope") != 0 {
		t.Error("MaxLag on empty should return 0")
	}
	if lt.RecentLags("nope", 5) != nil {
		t.Error("RecentLags on empty should return nil")
	}
}

func TestLagTracker_RecordAndAverage(t *testing.T) {
	lt := NewLagTracker()
	lt.Record("n1", 10)
	lt.Record("n1", 20)
	lt.Record("n1", 30)

	avg := lt.AverageLag("n1")
	if avg != 20 {
		t.Errorf("AverageLag = %d, want 20", avg)
	}
}

func TestLagTracker_MaxLag(t *testing.T) {
	lt := NewLagTracker()
	lt.Record("n1", 5)
	lt.Record("n1", 100)
	lt.Record("n1", 30)

	if lt.MaxLag("n1") != 100 {
		t.Errorf("MaxLag = %d, want 100", lt.MaxLag("n1"))
	}
}

func TestLagTracker_MaxLag_SingleSample(t *testing.T) {
	lt := NewLagTracker()
	lt.Record("n1", 42)
	if lt.MaxLag("n1") != 42 {
		t.Errorf("MaxLag = %d, want 42", lt.MaxLag("n1"))
	}
}

func TestLagTracker_RecentLags(t *testing.T) {
	lt := NewLagTracker()
	for i := int64(1); i <= 10; i++ {
		lt.Record("n1", i)
	}

	recent := lt.RecentLags("n1", 3)
	if len(recent) != 3 {
		t.Errorf("RecentLags count = %d, want 3", len(recent))
	}
	if recent[0].Lag != 8 || recent[1].Lag != 9 || recent[2].Lag != 10 {
		t.Errorf("RecentLags = %v, want [8, 9, 10]", recent)
	}
}

func TestLagTracker_RecentLags_MoreThanAvailable(t *testing.T) {
	lt := NewLagTracker()
	lt.Record("n1", 1)
	lt.Record("n1", 2)

	recent := lt.RecentLags("n1", 100)
	if len(recent) != 2 {
		t.Errorf("RecentLags count = %d, want 2 (all available)", len(recent))
	}
}

func TestLagTracker_MaxSamples(t *testing.T) {
	lt := NewLagTracker()
	for i := int64(0); i < 1500; i++ {
		lt.Record("n1", i)
	}

	samples := lt.RecentLags("n1", 1500)
	if len(samples) > 1000 {
		t.Errorf("samples len = %d, should not exceed maxSamples=1000", len(samples))
	}
	if len(samples) != 1000 {
		t.Errorf("samples len = %d, want 1000", len(samples))
	}
}

func TestLagTracker_MultipleNodes(t *testing.T) {
	lt := NewLagTracker()
	lt.Record("n1", 10)
	lt.Record("n2", 20)

	if lt.AverageLag("n1") != 10 {
		t.Errorf("n1 AverageLag = %d", lt.AverageLag("n1"))
	}
	if lt.AverageLag("n2") != 20 {
		t.Errorf("n2 AverageLag = %d", lt.AverageLag("n2"))
	}
}

func TestLagTracker_ConcurrentRecord(t *testing.T) {
	lt := NewLagTracker()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			lt.Record("n1", int64(id))
		}(i)
	}
	wg.Wait()

	if lt.AverageLag("n1") == 0 {
		t.Error("AverageLag should be non-zero after concurrent writes")
	}
}

func TestLagTracker_ConcurrentReadAndWrite(t *testing.T) {
	lt := NewLagTracker()
	const goroutines = 30
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				lt.Record("n1", int64(j))
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = lt.AverageLag("n1")
				_ = lt.MaxLag("n1")
				_ = lt.RecentLags("n1", 10)
			}
		}()
	}
	wg.Wait()
}

func TestNewPromotion(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	p := NewPromotion(rs)
	if p == nil {
		t.Fatal("NewPromotion should not return nil")
	}
	if p.IsPromoted() {
		t.Error("should not be promoted initially")
	}
	if p.PromotedNode() != "" {
		t.Error("PromotedNode should be empty initially")
	}
}

func TestPromotion_PromoteBestReplica(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.SetStatus("r1", ReplicaActive) // post-sync
	rs.UpdateLag("r1", 5)
	rs.UpdateLag("r1", 10)

	p := NewPromotion(rs)
	node, err := p.PromoteBestReplica()
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if node != "r1" {
		t.Errorf("promoted = %q, want r1", node)
	}
	if !p.IsPromoted() {
		t.Error("should be promoted")
	}
	if p.PromotedNode() != "r1" {
		t.Errorf("PromotedNode = %q, want r1", p.PromotedNode())
	}
}

func TestPromotion_PromoteBestReplica_PicksLowestLag(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")
	rs.SetStatus("r1", ReplicaActive) // post-sync
	rs.SetStatus("r2", ReplicaActive) // post-sync
	rs.UpdateLag("r1", 50)
	rs.UpdateLag("r2", 5)

	p := NewPromotion(rs)
	node, _ := p.PromoteBestReplica()
	if node != "r2" {
		t.Errorf("should pick r2 (lower lag), got %q", node)
	}
}

func TestPromotion_PromoteBestReplica_NoReplicas(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	p := NewPromotion(rs)
	_, err := p.PromoteBestReplica()
	if err != ErrNoReplicaAvailable {
		t.Errorf("expected ErrNoReplicaAvailable, got %v", err)
	}
}

func TestPromotion_PromoteBestReplica_AllFailed(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	r1, _ := rs.GetReplica("r1")
	r1.SetStatus(ReplicaFailed)

	p := NewPromotion(rs)
	_, err := p.PromoteBestReplica()
	if err != ErrNoReplicaAvailable {
		t.Errorf("expected ErrNoReplicaAvailable, got %v", err)
	}
}

// TestPromotion_ExcludesSyncingReplicas verifies that replicas in
// ReplicaSyncing state are excluded from promotion candidacy. A replica
// that hasn't completed its initial sync should never be promoted to
// primary — it doesn't have the full dataset yet.
func TestPromotion_ExcludesSyncingReplicas(t *testing.T) {
	rs := NewReplicaSet("primary-1")

	// r1 is syncing (default after AddReplica), r2 is active.
	rs.AddReplica("r1", "addr1")
	rs.AddReplica("r2", "addr2")
	rs.SetStatus("r2", ReplicaActive)
	rs.UpdateLag("r2", 5)

	// r1 stays in ReplicaSyncing — should NOT be promoted.
	p := NewPromotion(rs)
	node, err := p.PromoteBestReplica()
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if node != "r2" {
		t.Errorf("should promote r2 (active), got %s", node)
	}

	// Verify with BestReplicaFrom too.
	p2 := NewPromotion(rs)
	node2, err := p2.PromoteBestReplicaFrom("r1")
	if err != ErrNoReplicaAvailable {
		t.Errorf("r1 is syncing — should not be promotable via From, err=%v", err)
	}
	if node2 != "" {
		t.Errorf("PromoteBestReplicaFrom should not return syncing replica, got %s", node2)
	}
}

// TestBestReplica_ExcludesSyncing verifies that BestReplica and
// BestReplicaFrom skip replicas in ReplicaSyncing state.
func TestBestReplica_ExcludesSyncing(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1") // ReplicaSyncing (default)
	rs.AddReplica("r2", "addr2")
	rs.SetStatus("r2", ReplicaActive)
	rs.UpdateLag("r2", 10)

	best := rs.BestReplica()
	if best == nil || best.NodeID != "r2" {
		t.Errorf("BestReplica should skip syncing replicas, got %v", best)
	}

	// r1 is syncing — BestReplicaFrom should return nil.
	from := rs.BestReplicaFrom("r1")
	if from != nil {
		t.Errorf("BestReplicaFrom should return nil for syncing replica, got %v", from)
	}
}

func TestPromotion_ConcurrentPromote(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	rs.SetStatus("r1", ReplicaActive) // post-sync
	rs.UpdateLag("r1", 5)

	p := NewPromotion(rs)
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			p.PromoteBestReplica()
		}()
	}
	wg.Wait()
	if !p.IsPromoted() {
		t.Error("should be promoted after concurrent attempts")
	}
}

func TestErrNoReplicaAvailable_Error(t *testing.T) {
	err := ErrNoReplicaAvailable
	if err.Error() != "no replica available for promotion" {
		t.Errorf("error message = %q", err.Error())
	}
}

func TestReplicaError_Error(t *testing.T) {
	e := &ReplicaError{msg: "test error"}
	if e.Error() != "test error" {
		t.Errorf("Error() = %q", e.Error())
	}
}

func TestNewReplicationStream(t *testing.T) {
	s := NewReplicationStream(100)
	if s.Size() != 0 {
		t.Errorf("Size = %d, want 0", s.Size())
	}
	if s.LatestSeq() != 0 {
		t.Errorf("LatestSeq = %d, want 0", s.LatestSeq())
	}
}

func TestReplicationStream_AppendAndGetSince(t *testing.T) {
	s := NewReplicationStream(100)
	seq1 := s.Append(Operation{Command: "SET", Args: []string{"k1", "v1"}})
	seq2 := s.Append(Operation{Command: "SET", Args: []string{"k2", "v2"}})

	if seq1 != 1 {
		t.Errorf("seq1 = %d, want 1", seq1)
	}
	if seq2 != 2 {
		t.Errorf("seq2 = %d, want 2", seq2)
	}
	if s.LatestSeq() != 2 {
		t.Errorf("LatestSeq = %d, want 2", s.LatestSeq())
	}
	if s.Size() != 2 {
		t.Errorf("Size = %d, want 2", s.Size())
	}

	ops := s.GetSince(0)
	if len(ops) != 2 {
		t.Errorf("GetSince(0) len = %d, want 2", len(ops))
	}
	if ops[0].Command != "SET" || ops[1].Command != "SET" {
		t.Errorf("unexpected commands: %v", ops)
	}
}

func TestReplicationStream_GetSince_CurrentSeq(t *testing.T) {
	s := NewReplicationStream(100)
	s.Append(Operation{Command: "SET"})

	ops := s.GetSince(1)
	if len(ops) != 0 {
		t.Errorf("GetSince(LatestSeq) should return empty, got %d", len(ops))
	}
}

func TestReplicationStream_GetSince_FutureSeq(t *testing.T) {
	s := NewReplicationStream(100)
	s.Append(Operation{Command: "SET"})

	ops := s.GetSince(100)
	if len(ops) != 0 {
		t.Errorf("GetSince(future) should return empty, got %d", len(ops))
	}
}

func TestReplicationStream_GetSince_PartialWindow(t *testing.T) {
	s := NewReplicationStream(100)
	s.Append(Operation{Command: "A"})
	s.Append(Operation{Command: "B"})
	s.Append(Operation{Command: "C"})

	ops := s.GetSince(1)
	if len(ops) != 2 {
		t.Errorf("GetSince(1) len = %d, want 2", len(ops))
	}
	if ops[0].Command != "B" || ops[1].Command != "C" {
		t.Errorf("unexpected ops: %v", ops)
	}
}

func TestReplicationStream_GetSince_SeqBeforeBufferStart(t *testing.T) {
	s := NewReplicationStream(3)
	s.Append(Operation{Command: "A"})
	s.Append(Operation{Command: "B"})
	s.Append(Operation{Command: "C"})
	s.Append(Operation{Command: "D"})

	if s.Size() != 3 {
		t.Errorf("Size = %d, want 3 (capacity 3)", s.Size())
	}

	ops := s.GetSince(0)
	if len(ops) != 3 {
		t.Errorf("GetSince(0) len = %d, want 3 (buffer starts at seq 2)", len(ops))
	}
}

func TestReplicationStream_CapacityEviction(t *testing.T) {
	s := NewReplicationStream(5)
	for i := 0; i < 10; i++ {
		s.Append(Operation{Command: "SET", Args: []string{string(rune('a' + i))}})
	}

	if s.Size() != 5 {
		t.Errorf("Size = %d, want 5", s.Size())
	}
	if s.LatestSeq() != 10 {
		t.Errorf("LatestSeq = %d, want 10", s.LatestSeq())
	}

	ops := s.GetSince(5)
	if len(ops) != 5 {
		t.Errorf("GetSince(5) len = %d, want 5", len(ops))
	}
	if ops[0].Args[0] != "f" {
		t.Errorf("first retained op args = %v, want f", ops[0].Args)
	}
}

func TestReplicationStream_Clear(t *testing.T) {
	s := NewReplicationStream(100)
	s.Append(Operation{Command: "A"})
	s.Append(Operation{Command: "B"})

	s.Clear()

	if s.Size() != 0 {
		t.Errorf("Size after Clear = %d, want 0", s.Size())
	}
	if s.LatestSeq() != 2 {
		t.Errorf("LatestSeq should still be 2 after Clear, got %d", s.LatestSeq())
	}
	ops := s.GetSince(0)
	if len(ops) != 0 {
		t.Errorf("GetSince(0) after Clear len = %d, want 0", len(ops))
	}
}

func TestReplicationStream_ClearThenAppend(t *testing.T) {
	s := NewReplicationStream(10)
	s.Append(Operation{Command: "A"})
	s.Clear()
	seq := s.Append(Operation{Command: "B"})

	if seq != 2 {
		t.Errorf("seq after clear+append = %d, want 2", seq)
	}
	if s.Size() != 1 {
		t.Errorf("Size = %d, want 1", s.Size())
	}
}

func TestReplicationStream_ConcurrentAppend(t *testing.T) {
	s := NewReplicationStream(100)
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s.Append(Operation{Command: "SET"})
		}()
	}
	wg.Wait()

	if s.Size() != 100 {
		t.Errorf("Size = %d, want 100", s.Size())
	}
	if s.LatestSeq() != 100 {
		t.Errorf("LatestSeq = %d, want 100", s.LatestSeq())
	}
}

func TestReplicationStream_ConcurrentAppendAndGetSince(t *testing.T) {
	s := NewReplicationStream(100)
	const goroutines = 30
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				s.Append(Operation{Command: "SET"})
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = s.GetSince(0)
				_ = s.Size()
				_ = s.LatestSeq()
			}
		}()
	}
	wg.Wait()
}

func TestReplicationStream_SeqMonotonicallyIncreases(t *testing.T) {
	s := NewReplicationStream(10)
	var lastSeq int64
	for i := 0; i < 50; i++ {
		seq := s.Append(Operation{Command: "SET"})
		if seq <= lastSeq {
			t.Errorf("seq %d <= previous %d", seq, lastSeq)
		}
		lastSeq = seq
	}
}

func TestShortID(t *testing.T) {
	if got := shortID("abcdefghij"); got != "abcdefgh" {
		t.Errorf("shortID long = %q, want %q", got, "abcdefgh")
	}
	if got := shortID("abc"); got != "abc" {
		t.Errorf("shortID short = %q, want %q", got, "abc")
	}
}

func TestReplicationStream_EmptyBufferGetSince(t *testing.T) {
	s := NewReplicationStream(10)
	ops := s.GetSince(0)
	if ops != nil {
		t.Errorf("GetSince on empty should return nil, got %v", ops)
	}
}

// TestFailover_MismatchedLagVsRingPosition verifies that when the
// lowest-lag replica is NOT the ring's structural successor for the dead
// primary, PromoteBestReplicaFrom constrains promotion to the ring-successor.
// This prevents the split-brain where replication bookkeeping says node Y is
// primary while the ring routes all traffic to node Z.
func TestFailover_MismatchedLagVsRingPosition(t *testing.T) {
	// Setup: 3-node ring with dead-primary, replica-low-lag, replica-ring-succ.
	ring := hashring.New(150)
	ring.AddNode("dead-primary")
	ring.AddNode("replica-low-lag")
	ring.AddNode("replica-ring-succ")

	// Find the ring's actual successor for dead-primary.
	ringSuccessor := ring.SuccessorAfterRemoval("dead-primary")
	if ringSuccessor == "" {
		t.Fatal("SuccessorAfterRemoval should return a node")
	}

	// Determine which replica is the ring-successor and which is not.
	var lowLagNode string
	if ringSuccessor == "replica-low-lag" {
		// Ring successor happens to be the low-lag node — swap roles
		// so we can test the mismatched case.
		lowLagNode = "replica-ring-succ"
	} else {
		lowLagNode = "replica-low-lag"
	}

	// Create ReplicaSet: lowLagNode has lag=1, the other has lag=100.
	rs := NewReplicaSet("dead-primary")
	rs.AddReplica("replica-low-lag", "10.0.0.1:7000")
	rs.AddReplica("replica-ring-succ", "10.0.0.2:7000")
	rs.SetStatus("replica-low-lag", ReplicaActive)   // post-sync
	rs.SetStatus("replica-ring-succ", ReplicaActive) // post-sync

	if lowLagNode == "replica-low-lag" {
		rs.UpdateLag("replica-low-lag", 1)
		rs.UpdateLag("replica-ring-succ", 100)
	} else {
		rs.UpdateLag("replica-low-lag", 100)
		rs.UpdateLag("replica-ring-succ", 1)
	}

	// Verify setup: the lowest-lag node is NOT the ring-successor.
	bestOverall := rs.BestReplica()
	if bestOverall == nil {
		t.Fatal("BestReplica should not be nil")
	}
	if bestOverall.NodeID == ringSuccessor {
		// Both metrics agree — this test's setup didn't create a mismatch.
		// Force a mismatch by adjusting lags.
		if lowLagNode == "replica-low-lag" {
			rs.UpdateLag("replica-low-lag", 100)
			rs.UpdateLag("replica-ring-succ", 1)
		} else {
			rs.UpdateLag("replica-low-lag", 1)
			rs.UpdateLag("replica-ring-succ", 100)
		}
		bestOverall = rs.BestReplica()
	}

	// Now bestOverall.NodeID != ringSuccessor (the mismatch is set up).
	if bestOverall.NodeID == ringSuccessor {
		t.Fatalf("test setup failed: best overall (%s) == ring successor (%s); "+
			"cannot create mismatched scenario", bestOverall.NodeID, ringSuccessor)
	}

	t.Logf("mismatch created: best-lag=%s, ring-successor=%s",
		bestOverall.NodeID, ringSuccessor)

	// --- Failover using PromoteBestReplica (unconstrained) ---
	pUnconstrained := NewPromotion(rs)
	nodeUnconstrained, err := pUnconstrained.PromoteBestReplica()
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if nodeUnconstrained != bestOverall.NodeID {
		t.Errorf("unconstrained promoted %s, want %s (lowest lag)",
			nodeUnconstrained, bestOverall.NodeID)
	}

	// Reset status for the constrained test.
	// Reset both to Active (not Syncing) for the constrained test —
	// this test validates ring-successor constraint, not sync behavior.
	rs.SetStatus("replica-low-lag", ReplicaActive)
	rs.SetStatus("replica-ring-succ", ReplicaActive)

	// --- Failover using PromoteBestReplicaFrom (ring-constrained) ---
	pConstrained := NewPromotion(rs)
	nodeConstrained, err := pConstrained.PromoteBestReplicaFrom(ringSuccessor)
	if err != nil {
		t.Fatalf("PromoteBestReplicaFrom: %v", err)
	}

	// The constrained promotion MUST pick the ring-successor.
	if nodeConstrained != ringSuccessor {
		t.Errorf("constrained promoted %s, want %s (ring-successor)",
			nodeConstrained, ringSuccessor)
	}

	// --- Verify ring routing after ReplaceNode ---
	ring.ReplaceNode("dead-primary", nodeConstrained)

	// All keys that were on dead-primary must now route to the promoted node.
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("failover-key:%d", i)
		owner := ring.GetNode(key)
		if owner == "dead-primary" {
			t.Errorf("key %s still routes to dead-primary after failover", key)
			break
		}
	}

	// The promoted node should own at least some keys (the ones dead-primary had).
	promotedKeys := 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("failover-key:%d", i)
		if ring.GetNode(key) == nodeConstrained {
			promotedKeys++
		}
	}
	if promotedKeys == 0 {
		t.Errorf("promoted node %s owns no keys after failover", nodeConstrained)
	}

	t.Logf("failover complete: promoted %s (ring-successor=%v), owns %d/1000 keys",
		nodeConstrained, nodeConstrained == ringSuccessor, promotedKeys)
}

// TestFailover_PromotedNodePreservesOwnKeys verifies that after failover,
// the promoted replica retains its OWN pre-existing key range (as an
// independent primary for its own hash range) AND gains the dead primary's
// former range. This catches the class of bug where ReplaceNode wipes the
// promoted node's prior ring presence.
func TestFailover_PromotedNodePreservesOwnKeys(t *testing.T) {
	ring := hashring.New(150)
	ring.AddNode("dead-primary")
	ring.AddNode("promoted-replica")
	ring.AddNode("other-replica")

	// Capture key ownership BEFORE failover.
	keysBefore := map[string]int{
		"dead-primary":     0,
		"promoted-replica": 0,
		"other-replica":    0,
	}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("ownkey:%d", i)
		owner := ring.GetNode(key)
		keysBefore[owner]++
	}

	if keysBefore["dead-primary"] == 0 {
		t.Fatal("dead-primary should own keys before failover")
	}
	if keysBefore["promoted-replica"] == 0 {
		t.Fatal("promoted-replica should own its own keys before failover")
	}
	if keysBefore["other-replica"] == 0 {
		t.Fatal("other-replica should own keys before failover")
	}

	t.Logf("BEFORE failover: dead=%d, promoted=%d, other=%d",
		keysBefore["dead-primary"], keysBefore["promoted-replica"], keysBefore["other-replica"])

	// Determine which node the ring would promote (its structural successor).
	succ := ring.SuccessorAfterRemoval("dead-primary")
	if succ == "" {
		t.Fatal("no successor found")
	}
	t.Logf("ring-successor for dead-primary: %s", succ)

	// Create ReplicaSet — both replicas have some lag, both are candidates.
	rs := NewReplicaSet("dead-primary")
	rs.AddReplica("promoted-replica", "10.0.0.1:7000")
	rs.AddReplica("other-replica", "10.0.0.2:7000")
	rs.SetStatus("promoted-replica", ReplicaActive) // post-sync
	rs.SetStatus("other-replica", ReplicaActive)    // post-sync
	rs.UpdateLag("promoted-replica", 1)
	rs.UpdateLag("other-replica", 10)

	// Promote constrained to ring-successor.
	p := NewPromotion(rs)
	promoted, err := p.PromoteBestReplicaFrom(succ)
	if err != nil {
		t.Fatalf("PromoteBestReplicaFrom: %v", err)
	}
	t.Logf("promoted node: %s (ring-successor=%v)", promoted, promoted == succ)

	// The promoted node's OWN pre-existing keys before failover.
	ownKeysBefore := keysBefore[promoted]

	// Replace dead-primary in ring with the promoted node.
	ring.ReplaceNode("dead-primary", promoted)

	// Capture key ownership AFTER failover.
	keysAfter := map[string]int{
		"dead-primary":     0,
		"promoted-replica": 0,
		"other-replica":    0,
	}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("ownkey:%d", i)
		owner := ring.GetNode(key)
		keysAfter[owner]++
	}

	t.Logf("AFTER failover: dead=%d, promoted=%d, other=%d",
		keysAfter["dead-primary"], keysAfter["promoted-replica"], keysAfter["other-replica"])

	// dead-primary must own ZERO keys.
	if keysAfter["dead-primary"] != 0 {
		t.Errorf("dead-primary owns %d keys after failover, want 0", keysAfter["dead-primary"])
	}

	// The promoted node must STILL own its original keys.
	promotedAfter := keysAfter[promoted]
	if promotedAfter < ownKeysBefore {
		t.Errorf("promoted node %s lost its own keys: has %d after, had %d before",
			promoted, promotedAfter, ownKeysBefore)
	}

	// The promoted node must ALSO own dead-primary's former keys.
	if promotedAfter < keysBefore["dead-primary"] {
		t.Errorf("promoted node %s doesn't own dead-primary's former keys: has %d total, dead had %d",
			promoted, promotedAfter, keysBefore["dead-primary"])
	}

	// The OTHER replica (not promoted) must be UNCHANGED.
	otherNode := "promoted-replica"
	if promoted == "promoted-replica" {
		otherNode = "other-replica"
	}
	if keysAfter[otherNode] != keysBefore[otherNode] {
		t.Errorf("non-promoted node %s keys changed: had %d, now %d",
			otherNode, keysBefore[otherNode], keysAfter[otherNode])
	}

	// Total keys must be conserved.
	totalBefore := keysBefore["dead-primary"] + keysBefore["promoted-replica"] + keysBefore["other-replica"]
	totalAfter := keysAfter["promoted-replica"] + keysAfter["other-replica"]
	if totalAfter != totalBefore {
		t.Errorf("total keys changed: %d before, %d after (%d lost)",
			totalBefore, totalAfter, totalBefore-totalAfter)
	}
}

// TestFailover_RingSuccessorIsPromoted verifies the full failover sequence:
// dead primary → promote ring-successor → ReplaceNode → ring routes to promoted.
func TestFailover_RingSuccessorIsPromoted(t *testing.T) {
	ring := hashring.New(150)
	ring.AddNode("primary-X")
	ring.AddNode("replica-A")
	ring.AddNode("replica-B")

	// Capture routing before failover.
	routesBefore := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		routesBefore[ring.GetNode(key)]++
	}

	// Find ring successor.
	succ := ring.SuccessorAfterRemoval("primary-X")
	if succ == "" {
		t.Fatal("no successor found")
	}

	// Create ReplicaSet with A having lower lag than the successor.
	rs := NewReplicaSet("primary-X")
	rs.AddReplica("replica-A", "addr-A")
	rs.AddReplica("replica-B", "addr-B")
	rs.SetStatus("replica-A", ReplicaActive) // post-sync
	rs.SetStatus("replica-B", ReplicaActive) // post-sync
	if succ == "replica-A" {
		rs.UpdateLag("replica-A", 1)
		rs.UpdateLag("replica-B", 50)
	} else {
		rs.UpdateLag("replica-A", 50)
		rs.UpdateLag("replica-B", 1)
	}

	p := NewPromotion(rs)
	promoted, err := p.PromoteBestReplicaFrom(succ)
	if err != nil {
		t.Fatalf("PromoteBestReplicaFrom: %v", err)
	}
	if promoted != succ {
		t.Errorf("promoted %s, want ring-successor %s", promoted, succ)
	}

	// Replace in ring.
	ring.ReplaceNode("primary-X", promoted)

	// Verify primary-X owns zero keys.
	for i := 0; i < 1000; i++ {
		if ring.GetNode(fmt.Sprintf("key:%d", i)) == "primary-X" {
			t.Error("primary-X should own no keys after failover")
			break
		}
	}

	// Verify the promoted node owns at least the keys primary-X had.
	routesAfter := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key:%d", i)
		routesAfter[ring.GetNode(key)]++
	}

	// promoted node should own at least as many keys as primary-X had.
	if routesAfter[promoted] < routesBefore["primary-X"] {
		t.Errorf("promoted node owns %d keys, primary-X had %d",
			routesAfter[promoted], routesBefore["primary-X"])
	}
}
