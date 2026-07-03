package replication

import (
	"sync"
	"testing"
	"time"
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
	if r.Status != ReplicaSyncing {
		t.Errorf("Status = %v, want ReplicaSyncing", r.Status)
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
	r1.Status = ReplicaActive
	r2, _ := rs.GetReplica("r2")
	r2.Status = ReplicaLagging
	// r3 stays ReplicaSyncing

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

	rs.UpdateLag("r1", 5)

	r2, _ := rs.GetReplica("r2")
	r2.Status = ReplicaFailed

	best := rs.BestReplica()
	if best == nil || best.NodeID != "r1" {
		t.Errorf("BestReplica should skip failed, got %v", best)
	}
}

func TestReplicaSet_BestReplica_AllFailed(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
	r1, _ := rs.GetReplica("r1")
	r1.Status = ReplicaFailed

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

	rs.UpdateLag("r1", 10)
	r, _ := rs.GetReplica("r1")
	if r.Status != ReplicaSyncing {
		t.Errorf("after small lag, status = %v, want ReplicaSyncing", r.Status)
	}

	rs.UpdateLag("r1", 200)
	r, _ = rs.GetReplica("r1")
	if r.Status != ReplicaLagging {
		t.Errorf("after large lag, status = %v, want ReplicaLagging", r.Status)
	}

	rs.UpdateLag("r1", 50)
	r, _ = rs.GetReplica("r1")
	if r.Status != ReplicaActive {
		t.Errorf("after lag recovery, status = %v, want ReplicaActive", r.Status)
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
	if r.LastSync.Before(before) || r.LastSync.After(after) {
		t.Errorf("LastSync = %v not between %v and %v", r.LastSync, before, after)
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
	r1.Status = ReplicaFailed

	p := NewPromotion(rs)
	_, err := p.PromoteBestReplica()
	if err != ErrNoReplicaAvailable {
		t.Errorf("expected ErrNoReplicaAvailable, got %v", err)
	}
}

func TestPromotion_ConcurrentPromote(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	rs.AddReplica("r1", "addr1")
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
