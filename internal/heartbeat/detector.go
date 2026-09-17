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

// Clock abstracts time.Now() so tests can inject controlled, deterministic
// time instead of sleeping on the wall clock. Production code always uses
// realClock. Every elapsed-time computation in this package subtracts two
// Time values obtained from the same Clock rather than reconstructing
// times from Unix timestamps — for realClock that means every comparison
// uses Go's monotonic clock reading (time.Time carries both a wall and a
// monotonic reading from time.Now(), and Sub/Since use the monotonic one
// when both operands have it), so an NTP step or operator clock change
// mid-flight cannot make a node look falsely dead or falsely alive.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type intervalSample struct {
	d  time.Duration
	at time.Time
}

type HeartbeatEntry struct {
	NodeID       string
	Seq          int64
	Timestamp    time.Time
	samples      []intervalSample
	windowSize   int
	maxSampleAge time.Duration
	// suspectSince is zero while the node is not currently suspected;
	// set the moment phi first crosses the threshold, cleared on
	// recovery. Used to fire onNodeSuspect once per episode instead of
	// once per check tick (suspicion "decay": a node parked just over
	// the threshold doesn't re-alert every tick, only on state change).
	suspectSince time.Time
}

// RecordInterval records a new heartbeat arrival at "now" (from the
// detector's Clock). A negative computed interval — which should never
// happen with a real monotonic clock, but could with a misbehaving
// injected one — is clamped to zero rather than allowed to corrupt the
// running mean/variance.
func (h *HeartbeatEntry) RecordInterval(now time.Time) {
	if !h.Timestamp.IsZero() {
		interval := now.Sub(h.Timestamp)
		if interval < 0 {
			interval = 0
		}
		h.samples = append(h.samples, intervalSample{d: interval, at: now})
		h.pruneLocked(now)
	}
	// First heartbeat (Timestamp.IsZero): store the timestamp without
	// computing an interval — there is no meaningful prior reading.
	// The first real interval is computed on the next call, where
	// h.Timestamp holds the actual time of the previous heartbeat.
	h.Timestamp = now
	h.Seq++
}

// pruneLocked drops samples older than maxSampleAge (decay: stale
// evidence stops influencing the model) and caps the remainder to
// windowSize, oldest first. Caller holds the detector's lock.
func (h *HeartbeatEntry) pruneLocked(now time.Time) {
	if h.maxSampleAge > 0 {
		cutoff := now.Add(-h.maxSampleAge)
		i := 0
		for i < len(h.samples) && h.samples[i].at.Before(cutoff) {
			i++
		}
		if i > 0 {
			h.samples = h.samples[i:]
		}
	}
	if h.windowSize > 0 && len(h.samples) > h.windowSize {
		h.samples = h.samples[len(h.samples)-h.windowSize:]
	}
}

func (h *HeartbeatEntry) Phi(now time.Time) float64 {
	if len(h.samples) < 2 {
		return 0
	}

	mean, variance := h.stats()
	stddev := math.Sqrt(variance)

	elapsed := now.Sub(h.Timestamp).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	meanSec := float64(mean) / float64(time.Second)
	stddevSec := stddev / float64(time.Second)

	// Floor stddev at 10% of mean to prevent hypersensitivity when
	// intervals are extremely tight (e.g., OS sleep precision). Without
	// this, a stddev of 1ms with a mean of 1s causes phi to jump to
	// +Inf with only 100ms of additional delay past the mean.
	if meanSec > 0 && stddevSec < meanSec*0.1 {
		stddevSec = meanSec * 0.1
	}
	if stddevSec == 0 {
		stddevSec = 0.01
	}

	erfcArg := (elapsed - meanSec) / (stddevSec * math.Sqrt2)
	phi := -math.Log10(0.5 * math.Erfc(erfcArg))
	return phi
}

func (h *HeartbeatEntry) stats() (time.Duration, float64) {
	if len(h.samples) == 0 {
		return 0, 0
	}
	var sum float64
	for _, s := range h.samples {
		sum += float64(s.d)
	}
	mean := sum / float64(len(h.samples))

	var variance float64
	for _, s := range h.samples {
		diff := float64(s.d) - mean
		variance += diff * diff
	}
	variance /= float64(len(h.samples))
	return time.Duration(mean), variance
}

// Config tunes a Detector. Use DefaultConfig and override only what you
// need; zero-valued fields other than Clock fall back to their defaults
// in NewDetectorWithConfig.
type Config struct {
	// PhiThreshold is the phi value above which a node is "suspect."
	PhiThreshold float64
	// SuspectTimeout is how long a node must remain continuously suspect
	// before it's declared dead.
	SuspectTimeout time.Duration
	// WindowSize caps how many recent inter-arrival samples are kept per
	// node (a hard count-based bound).
	WindowSize int
	// MaxSampleAge additionally drops samples older than this regardless
	// of count — decay by time, not just by count, so a node whose
	// jitter profile changes (e.g. moved to a slower network path) isn't
	// still being judged against arrival statistics from an hour ago.
	MaxSampleAge time.Duration
	// Clock is injectable for deterministic tests; production code
	// should leave this nil (defaults to the real wall/monotonic clock).
	Clock Clock
}

func DefaultConfig() Config {
	return Config{
		PhiThreshold:   8.0,
		SuspectTimeout: 5 * time.Second,
		WindowSize:     1000,
		MaxSampleAge:   10 * time.Minute,
	}
}

func (c *Config) setDefaults() {
	d := DefaultConfig()
	if c.PhiThreshold <= 0 {
		c.PhiThreshold = d.PhiThreshold
	}
	if c.SuspectTimeout <= 0 {
		c.SuspectTimeout = d.SuspectTimeout
	}
	if c.WindowSize <= 0 {
		c.WindowSize = d.WindowSize
	}
	if c.MaxSampleAge <= 0 {
		c.MaxSampleAge = d.MaxSampleAge
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
}

type Detector struct {
	mu             sync.RWMutex
	selfID         string
	entries        map[string]*HeartbeatEntry
	phiThreshold   float64
	suspectTimeout time.Duration
	windowSize     int
	maxSampleAge   time.Duration
	clock          Clock
	onNodeSuspect  func(nodeID string)
	onNodeDead     func(nodeID string)
	stopCh         chan struct{}
}

// NewDetector creates a Detector with DefaultConfig. Use
// NewDetectorWithConfig to tune thresholds, window, decay, or inject a
// Clock for testing.
func NewDetector(selfID string) *Detector {
	return NewDetectorWithConfig(selfID, DefaultConfig())
}

func NewDetectorWithConfig(selfID string, cfg Config) *Detector {
	cfg.setDefaults()
	return &Detector{
		selfID:         selfID,
		entries:        make(map[string]*HeartbeatEntry),
		phiThreshold:   cfg.PhiThreshold,
		suspectTimeout: cfg.SuspectTimeout,
		windowSize:     cfg.WindowSize,
		maxSampleAge:   cfg.MaxSampleAge,
		clock:          cfg.Clock,
		stopCh:         make(chan struct{}),
	}
}

// SetThresholds updates the phi threshold and suspect timeout at runtime
// (e.g. once real cluster config is loaded, rather than only at
// construction). Safe to call while the detector is running.
func (d *Detector) SetThresholds(phiThreshold float64, suspectTimeout time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if phiThreshold > 0 {
		d.phiThreshold = phiThreshold
	}
	if suspectTimeout > 0 {
		d.suspectTimeout = suspectTimeout
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
			NodeID:       msg.NodeID,
			windowSize:   d.windowSize,
			maxSampleAge: d.maxSampleAge,
		}
		d.entries[msg.NodeID] = entry
	}

	now := d.clock.Now()
	entry.RecordInterval(now)
	entry.Seq = msg.Seq
	// A heartbeat arriving is recovery: clear suspicion so the next
	// episode of sustained trouble re-fires onNodeSuspect rather than
	// staying silent because it "already alerted once."
	entry.suspectSince = time.Time{}
}

func (d *Detector) CheckFailures() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := d.clock.Now()
	var dead []string

	for nodeID, entry := range d.entries {
		if nodeID == d.selfID {
			continue
		}

		phi := entry.Phi(now)
		if phi <= d.phiThreshold {
			entry.suspectSince = time.Time{}
			continue
		}

		justBecameSuspect := entry.suspectSince.IsZero()
		if justBecameSuspect {
			entry.suspectSince = now
		}
		elapsed := now.Sub(entry.Timestamp)

		if elapsed > d.suspectTimeout {
			dead = append(dead, nodeID)
			delete(d.entries, nodeID)
			log.Printf("[heartbeat] node %s declared dead (phi=%.2f, elapsed=%v)", shortID(nodeID), phi, elapsed)
			if d.onNodeDead != nil {
				go d.onNodeDead(nodeID)
			}
			continue
		}

		// Fire onNodeSuspect once per suspect episode, not once per
		// check tick — a node parked just over the threshold for
		// several ticks in a row shouldn't spam the callback/log.
		if justBecameSuspect {
			log.Printf("[heartbeat] node %s suspect (phi=%.2f)", shortID(nodeID), phi)
			if d.onNodeSuspect != nil {
				go d.onNodeSuspect(nodeID)
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
	now := d.clock.Now()
	elapsed := now.Sub(entry.Timestamp)
	if elapsed < 0 {
		elapsed = 0
	}
	phi := entry.Phi(now)
	return elapsed, phi
}
