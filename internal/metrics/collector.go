package metrics

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Collector struct {
	requestsTotal   atomic.Int64
	hitsTotal       atomic.Int64
	missesTotal     atomic.Int64
	evictionsTotal  atomic.Int64
	keysTotal       atomic.Int64
	memoryBytes     atomic.Int64
	connectedConns  atomic.Int64
	nodesAlive      atomic.Int64
	nodesTotal      atomic.Int64
	replicationLags sync.Map
	latencyBuckets  sync.Map
	startTime       time.Time
}

func NewCollector() *Collector {
	return &Collector{
		startTime: time.Now(),
	}
}

func (c *Collector) IncrRequests()  { c.requestsTotal.Add(1) }
func (c *Collector) IncrHits()      { c.hitsTotal.Add(1) }
func (c *Collector) IncrMisses()    { c.missesTotal.Add(1) }
func (c *Collector) IncrEvictions() { c.evictionsTotal.Add(1) }

// SetHits, SetMisses, and SetEvictions overwrite the running total
// directly rather than incrementing it — used when a source that already
// counts these accurately elsewhere (cache.LocalCache, which needs its
// own hit/miss/eviction counts for HitRate() and eviction bookkeeping
// regardless of metrics) is periodically pushed in, instead of
// duplicating that counting here via a per-Get/per-eviction callback.
func (c *Collector) SetHits(n int64)      { c.hitsTotal.Store(n) }
func (c *Collector) SetMisses(n int64)    { c.missesTotal.Store(n) }
func (c *Collector) SetEvictions(n int64) { c.evictionsTotal.Store(n) }

func (c *Collector) SetKeys(n int64)       { c.keysTotal.Store(n) }
func (c *Collector) SetMemory(n int64)     { c.memoryBytes.Store(n) }
func (c *Collector) SetConns(n int64)      { c.connectedConns.Store(n) }
func (c *Collector) SetAliveNodes(n int64) { c.nodesAlive.Store(n) }
func (c *Collector) SetTotalNodes(n int64) { c.nodesTotal.Store(n) }

func (c *Collector) RecordLatency(method string, duration time.Duration) {
	val, _ := c.latencyBuckets.LoadOrStore(method, &latencyBucket{})
	bucket := val.(*latencyBucket)
	bucket.record(duration)
}

// numLatencyBuckets is len(LatencyHistogramBounds)+1 (the trailing "+1" is
// the overflow bucket for anything >= the last bound), fixed as a real
// constant so latencyBucket can size its counters as an array rather than
// a slice that would need explicit non-zero-value initialization on every
// LoadOrStore.
const numLatencyBuckets = 8

// LatencyHistogramBounds are the upper bound (exclusive) of each latency
// bucket in nanoseconds, in order; a duration >= the last bound falls into
// the final overflow bucket. LatencyHistogram's returned slice always has
// numLatencyBuckets (len(LatencyHistogramBounds)+1) entries, one count per
// bound plus the overflow bucket.
var LatencyHistogramBounds = []int64{
	int64(1 * time.Millisecond),
	int64(5 * time.Millisecond),
	int64(10 * time.Millisecond),
	int64(25 * time.Millisecond),
	int64(50 * time.Millisecond),
	int64(100 * time.Millisecond),
	int64(250 * time.Millisecond),
}

// LatencyHistogram aggregates every command's recorded latencies into one
// global histogram (summing the per-bucket counts of every method tracked
// by RecordLatency) using the fixed boundaries in LatencyHistogramBounds.
// A single global histogram, rather than one per command, is what the
// dashboard's latency chart actually needs; per-method detail is still
// available via the Prometheus exporter.
func (c *Collector) LatencyHistogram() []int64 {
	counts := make([]int64, numLatencyBuckets)
	c.latencyBuckets.Range(func(_, value interface{}) bool {
		bucket := value.(*latencyBucket)
		for i := range counts {
			counts[i] += bucket.buckets[i].Load()
		}
		return true
	})
	return counts
}

func (c *Collector) SetReplicationLag(nodeID string, lag int64) {
	c.replicationLags.Store(nodeID, lag)
}

// latencyBucket tracks one command's latency distribution: total+count
// (for an average), max, and a real histogram with LatencyHistogramBounds
// boundaries — not just enough for an average, since an average alone
// can't show the dashboard's latency distribution honestly.
type latencyBucket struct {
	total   atomic.Int64
	count   atomic.Int64
	max     atomic.Int64
	buckets [numLatencyBuckets]atomic.Int64
}

func (b *latencyBucket) record(d time.Duration) {
	ns := d.Nanoseconds()
	b.total.Add(ns)
	b.count.Add(1)
	for {
		old := b.max.Load()
		if ns <= old || b.max.CompareAndSwap(old, ns) {
			break
		}
	}

	idx := len(LatencyHistogramBounds) // overflow bucket by default
	for i, bound := range LatencyHistogramBounds {
		if ns < bound {
			idx = i
			break
		}
	}
	b.buckets[idx].Add(1)
}

func (c *Collector) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"requests_total":    c.requestsTotal.Load(),
		"hits_total":        c.hitsTotal.Load(),
		"misses_total":      c.missesTotal.Load(),
		"evictions_total":   c.evictionsTotal.Load(),
		"keys_total":        c.keysTotal.Load(),
		"memory_bytes":      c.memoryBytes.Load(),
		"connected_conns":   c.connectedConns.Load(),
		"nodes_alive":       c.nodesAlive.Load(),
		"nodes_total":       c.nodesTotal.Load(),
		"uptime_seconds":    time.Since(c.startTime).Seconds(),
		"latency_histogram": c.LatencyHistogram(),
	}
}

func (c *Collector) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		snap := c.Snapshot()
		data, _ := json.Marshal(snap)
		_, _ = w.Write(data)
	})
}
