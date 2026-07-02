package heartbeat

import (
	"log"
	"math"
	"sync"
	"time"
)

type HeartbeatMessage struct {
	NodeID    string
	Epoch     uint64
	Seq       int64
	Timestamp time.Time
	Load      float64
	MemoryMB  int64
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

type HeartbeatEntry struct {
	NodeID    string
	Seq       int64
	Timestamp time.Time
	intervals []time.Duration
}

func (h *HeartbeatEntry) RecordInterval() {
	if len(h.intervals) > 0 {
		interval := time.Since(h.Timestamp)
		h.intervals = append(h.intervals, interval)
		if len(h.intervals) > 1000 {
			h.intervals = h.intervals[1:]
		}
	} else {
		h.intervals = append(h.intervals, time.Since(h.Timestamp))
	}
	h.Timestamp = time.Now()
	h.Seq++
}

func (h *HeartbeatEntry) Phi(now time.Time) float64 {
	if len(h.intervals) < 2 {
		return 0
	}

	mean, variance := h.stats()
	stddev := math.Sqrt(variance)
	if stddev == 0 {
		stddev = 1
	}

	elapsed := now.Sub(h.Timestamp).Seconds()
	meanSec := float64(mean) / float64(time.Second)

	erfcArg := (elapsed - meanSec) / (stddev * math.Sqrt2)
	phi := -math.Log10(0.5 * math.Erfc(erfcArg))
	return phi
}

func (h *HeartbeatEntry) stats() (time.Duration, float64) {
	if len(h.intervals) == 0 {
		return 0, 0
	}
	var sum float64
	for _, d := range h.intervals {
		sum += float64(d)
	}
	mean := sum / float64(len(h.intervals))

	var variance float64
	for _, d := range h.intervals {
		diff := float64(d) - mean
		variance += diff * diff
	}
	variance /= float64(len(h.intervals))
	return time.Duration(mean), variance
}

type Detector struct {
	mu             sync.RWMutex
	selfID         string
	entries        map[string]*HeartbeatEntry
	phiThreshold   float64
	suspectTimeout time.Duration
	onNodeSuspect  func(nodeID string)
	onNodeDead     func(nodeID string)
	stopCh         chan struct{}
}

func NewDetector(selfID string) *Detector {
	return &Detector{
		selfID:         selfID,
		entries:        make(map[string]*HeartbeatEntry),
		phiThreshold:   8.0,
		suspectTimeout: 5 * time.Second,
		stopCh:         make(chan struct{}),
	}
}

func (d *Detector) OnNodeSuspect(fn func(nodeID string)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onNodeSuspect = fn
}

func (d *Detector) OnNodeDead(fn func(nodeID string)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onNodeDead = fn
}

func (d *Detector) RecordHeartbeat(msg HeartbeatMessage) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entry, ok := d.entries[msg.NodeID]
	if !ok {
		entry = &HeartbeatEntry{
			NodeID: msg.NodeID,
		}
		d.entries[msg.NodeID] = entry
	}

	entry.RecordInterval()
	entry.Seq = msg.Seq
}

func (d *Detector) CheckFailures() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	var dead []string

	for nodeID, entry := range d.entries {
		if nodeID == d.selfID {
			continue
		}

		phi := entry.Phi(now)
		if phi > d.phiThreshold {
			elapsed := now.Sub(entry.Timestamp)
			if elapsed > d.suspectTimeout {
				dead = append(dead, nodeID)
				delete(d.entries, nodeID)
				log.Printf("[heartbeat] node %s declared dead (phi=%.2f, elapsed=%v)", shortID(nodeID), phi, elapsed)
				if d.onNodeDead != nil {
					go d.onNodeDead(nodeID)
				}
			} else {
				log.Printf("[heartbeat] node %s suspect (phi=%.2f)", shortID(nodeID), phi)
				if d.onNodeSuspect != nil {
					go d.onNodeSuspect(nodeID)
				}
			}
		}
	}
	return dead
}

func (d *Detector) StartChecking(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-d.stopCh:
				return
			case <-ticker.C:
				d.CheckFailures()
			}
		}
	}()
}

func (d *Detector) Stop() {
	close(d.stopCh)
}

func (d *Detector) NodeCount() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.entries)
}

func (d *Detector) NodeStatus(nodeID string) (time.Duration, float64) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	entry, ok := d.entries[nodeID]
	if !ok {
		return 0, 0
	}
	elapsed := time.Since(entry.Timestamp)
	phi := entry.Phi(time.Now())
	return elapsed, phi
}
