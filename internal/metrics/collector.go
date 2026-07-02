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

func (c *Collector) IncrRequests()         { c.requestsTotal.Add(1) }
func (c *Collector) IncrHits()             { c.hitsTotal.Add(1) }
func (c *Collector) IncrMisses()           { c.missesTotal.Add(1) }
func (c *Collector) IncrEvictions()        { c.evictionsTotal.Add(1) }
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

func (c *Collector) SetReplicationLag(nodeID string, lag int64) {
	c.replicationLags.Store(nodeID, lag)
}

type latencyBucket struct {
	total atomic.Int64
	count atomic.Int64
	max   atomic.Int64
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
}

func (c *Collector) Snapshot() map[string]interface{} {
	return map[string]interface{}{
		"requests_total":  c.requestsTotal.Load(),
		"hits_total":      c.hitsTotal.Load(),
		"misses_total":    c.missesTotal.Load(),
		"evictions_total": c.evictionsTotal.Load(),
		"keys_total":      c.keysTotal.Load(),
		"memory_bytes":    c.memoryBytes.Load(),
		"connected_conns": c.connectedConns.Load(),
		"nodes_alive":     c.nodesAlive.Load(),
		"nodes_total":     c.nodesTotal.Load(),
		"uptime_seconds":  time.Since(c.startTime).Seconds(),
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
