package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewCollector_InitializesZeroValues(t *testing.T) {
	c := NewCollector()
	snap := c.Snapshot()

	if snap["requests_total"] != int64(0) {
		t.Errorf("requests_total = %v, want 0", snap["requests_total"])
	}
	if snap["hits_total"] != int64(0) {
		t.Errorf("hits_total = %v, want 0", snap["hits_total"])
	}
	if snap["misses_total"] != int64(0) {
		t.Errorf("misses_total = %v, want 0", snap["misses_total"])
	}
	if snap["evictions_total"] != int64(0) {
		t.Errorf("evictions_total = %v, want 0", snap["evictions_total"])
	}
	if snap["keys_total"] != int64(0) {
		t.Errorf("keys_total = %v, want 0", snap["keys_total"])
	}
	if snap["memory_bytes"] != int64(0) {
		t.Errorf("memory_bytes = %v, want 0", snap["memory_bytes"])
	}
	if snap["connected_conns"] != int64(0) {
		t.Errorf("connected_conns = %v, want 0", snap["connected_conns"])
	}
	if snap["nodes_alive"] != int64(0) {
		t.Errorf("nodes_alive = %v, want 0", snap["nodes_alive"])
	}
	if snap["nodes_total"] != int64(0) {
		t.Errorf("nodes_total = %v, want 0", snap["nodes_total"])
	}
}

func TestNewCollector_StartTimeNonZero(t *testing.T) {
	c := NewCollector()
	before := time.Now()
	time.Sleep(time.Millisecond)
	c2 := NewCollector()
	after := time.Now()

	snap1 := c.Snapshot()
	snap2 := c2.Snapshot()
	uptime1 := snap1["uptime_seconds"].(float64)
	uptime2 := snap2["uptime_seconds"].(float64)

	if uptime1 < 0 {
		t.Errorf("uptime should be non-negative, got %v", uptime1)
	}
	if uptime1 <= uptime2 {
		t.Errorf("first collector uptime should be higher since created earlier, got %v <= %v", uptime1, uptime2)
	}
	_ = before
	_ = after
}

func TestIncrRequests(t *testing.T) {
	c := NewCollector()
	c.IncrRequests()
	c.IncrRequests()
	c.IncrRequests()
	snap := c.Snapshot()
	if snap["requests_total"] != int64(3) {
		t.Errorf("requests_total = %v, want 3", snap["requests_total"])
	}
}

func TestIncrHits(t *testing.T) {
	c := NewCollector()
	c.IncrHits()
	c.IncrHits()
	snap := c.Snapshot()
	if snap["hits_total"] != int64(2) {
		t.Errorf("hits_total = %v, want 2", snap["hits_total"])
	}
}

func TestIncrMisses(t *testing.T) {
	c := NewCollector()
	c.IncrMisses()
	snap := c.Snapshot()
	if snap["misses_total"] != int64(1) {
		t.Errorf("misses_total = %v, want 1", snap["misses_total"])
	}
}

func TestIncrEvictions(t *testing.T) {
	c := NewCollector()
	c.IncrEvictions()
	c.IncrEvictions()
	c.IncrEvictions()
	c.IncrEvictions()
	c.IncrEvictions()
	snap := c.Snapshot()
	if snap["evictions_total"] != int64(5) {
		t.Errorf("evictions_total = %v, want 5", snap["evictions_total"])
	}
}

func TestSetKeys(t *testing.T) {
	c := NewCollector()
	c.SetKeys(42)
	if c.Snapshot()["keys_total"] != int64(42) {
		t.Errorf("keys_total = %v, want 42", c.Snapshot()["keys_total"])
	}
	c.SetKeys(0)
	if c.Snapshot()["keys_total"] != int64(0) {
		t.Errorf("keys_total after reset = %v, want 0", c.Snapshot()["keys_total"])
	}
	c.SetKeys(-1)
	if c.Snapshot()["keys_total"] != int64(-1) {
		t.Errorf("keys_total negative = %v, want -1", c.Snapshot()["keys_total"])
	}
}

func TestSetMemory(t *testing.T) {
	c := NewCollector()
	c.SetMemory(1024 * 1024)
	if c.Snapshot()["memory_bytes"] != int64(1024*1024) {
		t.Errorf("memory_bytes = %v, want %d", c.Snapshot()["memory_bytes"], 1024*1024)
	}
}

func TestSetConns(t *testing.T) {
	c := NewCollector()
	c.SetConns(500)
	if c.Snapshot()["connected_conns"] != int64(500) {
		t.Errorf("connected_conns = %v, want 500", c.Snapshot()["connected_conns"])
	}
}

func TestSetAliveNodes(t *testing.T) {
	c := NewCollector()
	c.SetAliveNodes(3)
	c.SetTotalNodes(5)
	snap := c.Snapshot()
	if snap["nodes_alive"] != int64(3) {
		t.Errorf("nodes_alive = %v, want 3", snap["nodes_alive"])
	}
	if snap["nodes_total"] != int64(5) {
		t.Errorf("nodes_total = %v, want 5", snap["nodes_total"])
	}
}

func TestRecordLatency_SingleMethod(t *testing.T) {
	c := NewCollector()
	c.RecordLatency("SET", 10*time.Millisecond)
	c.RecordLatency("SET", 20*time.Millisecond)
	c.RecordLatency("SET", 5*time.Millisecond)

	val, ok := c.latencyBuckets.Load("SET")
	if !ok {
		t.Fatal("latency bucket for SET not found")
	}
	bucket := val.(*latencyBucket)
	if bucket.count.Load() != 3 {
		t.Errorf("count = %d, want 3", bucket.count.Load())
	}
	if bucket.total.Load() != int64(35*time.Millisecond) {
		t.Errorf("total = %d, want %d", bucket.total.Load(), int64(35*time.Millisecond))
	}
	if bucket.max.Load() != int64(20*time.Millisecond) {
		t.Errorf("max = %d, want %d", bucket.max.Load(), int64(20*time.Millisecond))
	}
}

func TestRecordLatency_MultipleMethods(t *testing.T) {
	c := NewCollector()
	c.RecordLatency("GET", 1*time.Millisecond)
	c.RecordLatency("SET", 5*time.Millisecond)

	if _, ok := c.latencyBuckets.Load("GET"); !ok {
		t.Error("GET bucket not found")
	}
	if _, ok := c.latencyBuckets.Load("SET"); !ok {
		t.Error("SET bucket not found")
	}
}

func TestRecordLatency_MaxTracking(t *testing.T) {
	c := NewCollector()
	c.RecordLatency("OP", 100*time.Millisecond)
	c.RecordLatency("OP", 50*time.Millisecond)
	c.RecordLatency("OP", 200*time.Millisecond)
	c.RecordLatency("OP", 150*time.Millisecond)

	val, _ := c.latencyBuckets.Load("OP")
	bucket := val.(*latencyBucket)
	if bucket.max.Load() != int64(200*time.Millisecond) {
		t.Errorf("max = %d, want %d", bucket.max.Load(), int64(200*time.Millisecond))
	}
}

func TestSetReplicationLag(t *testing.T) {
	c := NewCollector()
	c.SetReplicationLag("node-1", 100)
	c.SetReplicationLag("node-2", 200)

	val, ok := c.replicationLags.Load("node-1")
	if !ok || val.(int64) != 100 {
		t.Errorf("node-1 lag = %v, want 100", val)
	}
	val, ok = c.replicationLags.Load("node-2")
	if !ok || val.(int64) != 200 {
		t.Errorf("node-2 lag = %v, want 200", val)
	}
}

func TestSnapshot_ReturnsDistinctMap(t *testing.T) {
	c := NewCollector()
	c.SetKeys(10)
	snap1 := c.Snapshot()
	snap2 := c.Snapshot()
	snap1["keys_total"] = 999
	if snap2["keys_total"] != int64(10) {
		t.Error("modifying one snapshot should not affect another")
	}
}

func TestSnapshot_UptimeIncreases(t *testing.T) {
	c := NewCollector()
	time.Sleep(5 * time.Millisecond)
	snap := c.Snapshot()
	uptime := snap["uptime_seconds"].(float64)
	if uptime < 0.001 {
		t.Errorf("uptime too low: %v", uptime)
	}
}

func TestConcurrentIncrRequests(t *testing.T) {
	c := NewCollector()
	const goroutines = 100
	const perGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				c.IncrRequests()
			}
		}()
	}
	wg.Wait()

	snap := c.Snapshot()
	expected := int64(goroutines * perGoroutine)
	if snap["requests_total"] != expected {
		t.Errorf("requests_total = %v, want %d (race?)", snap["requests_total"], expected)
	}
}

func TestConcurrentMixedCounters(t *testing.T) {
	c := NewCollector()
	const goroutines = 50
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(5 * goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c.IncrRequests()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c.IncrHits()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c.IncrMisses()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c.IncrEvictions()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c.SetKeys(int64(j))
			}
		}()
	}
	wg.Wait()

	snap := c.Snapshot()
	expected := int64(goroutines * iterations)
	if snap["requests_total"] != expected {
		t.Errorf("requests_total = %v, want %d", snap["requests_total"], expected)
	}
	if snap["hits_total"] != expected {
		t.Errorf("hits_total = %v, want %d", snap["hits_total"], expected)
	}
	if snap["misses_total"] != expected {
		t.Errorf("misses_total = %v, want %d", snap["misses_total"], expected)
	}
	if snap["evictions_total"] != expected {
		t.Errorf("evictions_total = %v, want %d", snap["evictions_total"], expected)
	}
}

func TestConcurrentRecordLatency(t *testing.T) {
	c := NewCollector()
	const goroutines = 50
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			method := "M1"
			if id%2 == 0 {
				method = "M2"
			}
			for j := 0; j < iterations; j++ {
				c.RecordLatency(method, time.Duration(j)*time.Microsecond)
			}
		}(i)
	}
	wg.Wait()

	for _, method := range []string{"M1", "M2"} {
		val, ok := c.latencyBuckets.Load(method)
		if !ok {
			t.Errorf("bucket for %s not found", method)
			continue
		}
		bucket := val.(*latencyBucket)
		total := bucket.count.Load()
		if total != int64(goroutines/2*iterations) {
			t.Errorf("%s count = %d, want %d", method, total, goroutines/2*iterations)
		}
	}
}

func TestConcurrentSetReplicationLag(t *testing.T) {
	c := NewCollector()
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			c.SetReplicationLag("node", int64(id))
		}(i)
	}
	wg.Wait()

	val, ok := c.replicationLags.Load("node")
	if !ok {
		t.Fatal("node lag not found")
	}
	v := val.(int64)
	if v < 0 || v >= int64(goroutines) {
		t.Errorf("node lag = %v, out of expected range [0, %d)", v, goroutines)
	}
}

func TestHandler_ServesJSON(t *testing.T) {
	c := NewCollector()
	c.SetKeys(42)
	c.IncrHits()

	handler := c.Handler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var snap map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if snap["keys_total"] != float64(42) {
		t.Errorf("keys_total = %v, want 42", snap["keys_total"])
	}
	if snap["hits_total"] != float64(1) {
		t.Errorf("hits_total = %v, want 1", snap["hits_total"])
	}
}

func TestHandler_ReturnsValidJSON_EveryTime(t *testing.T) {
	c := NewCollector()
	handler := c.Handler()

	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		var snap map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
			t.Fatalf("iteration %d: invalid JSON: %v", i, err)
		}
	}
}

func TestPrometheusHandler_OutputFormat(t *testing.T) {
	c := NewCollector()
	c.SetKeys(100)
	c.SetAliveNodes(3)
	c.SetTotalNodes(5)
	c.IncrRequests()
	c.IncrHits()
	c.IncrMisses()
	c.IncrEvictions()

	handler := c.PrometheusHandler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/plain") {
		t.Errorf("Content-Type = %q, should contain text/plain", ct)
	}

	body := rec.Body.String()
	for _, expected := range []string{
		"# HELP hydracache_requests_total",
		"# TYPE hydracache_requests_total counter",
		"hydracache_requests_total 1",
		"# HELP hydracache_hits_total",
		"hydracache_hits_total 1",
		"# HELP hydracache_misses_total",
		"hydracache_misses_total 1",
		"# HELP hydracache_keys_total",
		"hydracache_keys_total 100",
		"# HELP hydracache_memory_bytes",
		"hydracache_nodes_alive",
		"hydracache_nodes_alive 3",
		"hydracache_nodes_total 5",
		"# HELP hydracache_evictions_total",
		"hydracache_evictions_total 1",
	} {
		if !strings.Contains(body, expected) {
			t.Errorf("prometheus output missing %q", expected)
		}
	}
}

func TestPrometheusHandler_ReplicationLags(t *testing.T) {
	c := NewCollector()
	c.SetReplicationLag("node-a", 50)
	c.SetReplicationLag("node-b", 200)

	handler := c.PrometheusHandler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, `hydracache_replication_lag{node="node-a"} 50`) {
		t.Error("missing node-a lag in prometheus output")
	}
	if !strings.Contains(body, `hydracache_replication_lag{node="node-b"} 200`) {
		t.Error("missing node-b lag in prometheus output")
	}
}

func TestPrometheusHandler_EmptyMetrics(t *testing.T) {
	c := NewCollector()
	handler := c.PrometheusHandler()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "hydracache_requests_total 0") {
		t.Error("should report zero for empty metrics")
	}
}

func TestLatencyBucket_CASMaxUnderContention(t *testing.T) {
	bucket := &latencyBucket{}
	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			d := time.Duration(id) * time.Millisecond
			bucket.record(d)
		}(i)
	}
	wg.Wait()

	if bucket.count.Load() != goroutines {
		t.Errorf("count = %d, want %d", bucket.count.Load(), goroutines)
	}

	max := bucket.max.Load()
	expectedMax := int64((goroutines - 1) * time.Millisecond)
	if max != expectedMax {
		t.Errorf("max = %d, want %d", max, expectedMax)
	}
}

func TestLatencyBucket_TotalSum(t *testing.T) {
	bucket := &latencyBucket{}
	bucket.record(10 * time.Millisecond)
	bucket.record(20 * time.Millisecond)
	bucket.record(30 * time.Millisecond)

	expectedTotal := int64(60 * time.Millisecond)
	if bucket.total.Load() != expectedTotal {
		t.Errorf("total = %d, want %d", bucket.total.Load(), expectedTotal)
	}
}

func TestLatencyBucket_ZeroDuration(t *testing.T) {
	bucket := &latencyBucket{}
	bucket.record(0)
	bucket.record(5 * time.Millisecond)

	if bucket.max.Load() != int64(5*time.Millisecond) {
		t.Errorf("max = %d, want %d", bucket.max.Load(), int64(5*time.Millisecond))
	}
	if bucket.count.Load() != 2 {
		t.Errorf("count = %d, want 2", bucket.count.Load())
	}
}

func TestConcurrentSnapshotReads(t *testing.T) {
	c := NewCollector()
	c.SetKeys(100)
	c.IncrRequests()

	var wg sync.WaitGroup
	const readers = 50
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			snap := c.Snapshot()
			_ = snap["requests_total"]
			_ = snap["keys_total"]
			_ = snap["uptime_seconds"]
		}()
	}
	wg.Wait()
}

func TestConcurrentSnapshotWithWrites(t *testing.T) {
	c := NewCollector()
	var wg sync.WaitGroup

	const writers = 10
	const readers = 20
	const iterations = 100

	wg.Add(writers + readers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c.IncrRequests()
				c.SetKeys(int64(j))
			}
		}()
	}
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				snap := c.Snapshot()
				_ = snap["requests_total"]
				_ = snap["uptime_seconds"]
			}
		}()
	}
	wg.Wait()
}

func TestAtomicInt64_Independence(t *testing.T) {
	c := NewCollector()
	c.SetKeys(10)
	c.IncrHits()

	snap := c.Snapshot()
	if snap["hits_total"] != int64(1) {
		t.Error("hits should be independent from keys")
	}
	if snap["keys_total"] != int64(10) {
		t.Error("keys should be independent from hits")
	}
}

func TestSetToZero(t *testing.T) {
	c := NewCollector()
	c.SetKeys(100)
	c.SetConns(50)
	c.SetAliveNodes(3)

	c.SetKeys(0)
	c.SetConns(0)
	c.SetAliveNodes(0)

	snap := c.Snapshot()
	if snap["keys_total"] != int64(0) {
		t.Errorf("keys_total = %v, want 0", snap["keys_total"])
	}
	if snap["connected_conns"] != int64(0) {
		t.Errorf("connected_conns = %v, want 0", snap["connected_conns"])
	}
	if snap["nodes_alive"] != int64(0) {
		t.Errorf("nodes_alive = %v, want 0", snap["nodes_alive"])
	}
}

func TestSnapshot_LatencyNotInSnapshot(t *testing.T) {
	c := NewCollector()
	c.RecordLatency("GET", 5*time.Millisecond)
	c.RecordLatency("SET", 10*time.Millisecond)

	snap := c.Snapshot()
	// latency buckets are stored separately, Snapshot doesn't include them
	// this test verifies Snapshot only returns the atomic counter fields
	if len(snap) != 10 {
		t.Errorf("Snapshot has %d keys, expected 10", len(snap))
	}

	_ = snap
}
