package replication

import (
	"sync"
	"testing"
)

func TestReplicaSet_CheckAndAdvanceEpoch(t *testing.T) {
	rs := NewReplicaSet("primary-1")

	if !rs.CheckAndAdvanceEpoch(0) {
		t.Fatal("expected epoch 0 to be accepted on a fresh shard")
	}
	if !rs.CheckAndAdvanceEpoch(5) {
		t.Fatal("expected epoch 5 to be accepted (ahead of watermark)")
	}
	if rs.CheckAndAdvanceEpoch(3) {
		t.Fatal("expected epoch 3 to be rejected as stale (behind watermark 5)")
	}
	if !rs.CheckAndAdvanceEpoch(5) {
		t.Fatal("expected epoch equal to the watermark to be accepted")
	}
}

func TestReplicaSet_CheckAndAdvanceEpoch_ConcurrentAdvancesMonotonically(t *testing.T) {
	rs := NewReplicaSet("primary-1")
	const n = 200

	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		i := uint64(i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs.CheckAndAdvanceEpoch(i)
		}()
	}
	wg.Wait()

	if rs.CheckAndAdvanceEpoch(n) != true {
		t.Fatalf("expected watermark to have reached %d", n)
	}
	if rs.CheckAndAdvanceEpoch(n - 1) {
		t.Fatal("expected watermark to never go backwards under concurrent advances")
	}
}
