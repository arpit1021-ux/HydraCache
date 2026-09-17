package heartbeat

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock gives tests full, deterministic control over elapsed time
// instead of relying on real sleeps — no flakiness from scheduler jitter,
// and it lets a test simulate things (a GC-pause-scale gap, a clock
// rollback) that would be impractical to reproduce with a real clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set jumps the clock to an arbitrary time, including backward —
// simulating a wall-clock step (NTP correction, operator adjustment).
func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func TestDetector_ConfigurableThreshold(t *testing.T) {
	clock := newFakeClock(time.Now())
	// A very low threshold makes the node "suspect" almost immediately
	// once any jitter appears, proving PhiThreshold is actually wired
	// in rather than the hardcoded 8.0 from before.
	d := NewDetectorWithConfig("self", Config{
		PhiThreshold:   0.001,
		SuspectTimeout: time.Hour, // never actually reach "dead" in this test
		Clock:          clock,
	})

	for i := 0; i < 5; i++ {
		d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: int64(i + 1)})
		clock.Advance(50 * time.Millisecond)
	}
	clock.Advance(60 * time.Millisecond) // slightly late relative to the 50ms mean

	_, phi := d.NodeStatus("node-1")
	if phi <= 0.001 {
		t.Fatalf("expected phi to exceed the configured low threshold, got %f", phi)
	}
}

func TestDetector_SampleAgeDecay(t *testing.T) {
	clock := newFakeClock(time.Now())
	d := NewDetectorWithConfig("self", Config{
		WindowSize:   1000,
		MaxSampleAge: time.Minute,
		Clock:        clock,
	})

	for i := 0; i < 5; i++ {
		d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: int64(i + 1)})
		clock.Advance(time.Second)
	}

	d.mu.RLock()
	before := len(d.entries["node-1"].samples)
	d.mu.RUnlock()
	if before == 0 {
		t.Fatal("expected some samples recorded before decay")
	}

	// Jump far past MaxSampleAge with no further heartbeats, then record
	// one more — every old sample should have aged out.
	clock.Advance(10 * time.Minute)
	d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: 999})

	d.mu.RLock()
	after := len(d.entries["node-1"].samples)
	d.mu.RUnlock()
	if after >= before {
		t.Errorf("expected old samples to have decayed out (before=%d after=%d)", before, after)
	}
}

func TestDetector_SuspectFiresOncePerEpisode(t *testing.T) {
	clock := newFakeClock(time.Now())
	d := NewDetectorWithConfig("self", Config{
		PhiThreshold:   1.0,
		SuspectTimeout: time.Hour, // stay suspect, never reach dead
		Clock:          clock,
	})

	var suspectCount int
	var mu sync.Mutex
	d.OnNodeSuspect(func(nodeID string) {
		mu.Lock()
		suspectCount++
		mu.Unlock()
	})

	for i := 0; i < 5; i++ {
		d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: int64(i + 1)})
		clock.Advance(50 * time.Millisecond)
	}

	// Go well past the mean without another heartbeat, then check
	// failures repeatedly — phi stays elevated across all of these
	// ticks, so onNodeSuspect must fire once, not once per tick.
	clock.Advance(500 * time.Millisecond)
	for i := 0; i < 10; i++ {
		d.CheckFailures()
		clock.Advance(10 * time.Millisecond)
	}

	// Give the async callback goroutines a moment to run.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		c := suspectCount
		mu.Unlock()
		if c > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if suspectCount != 1 {
		t.Errorf("expected onNodeSuspect to fire exactly once per episode, fired %d times", suspectCount)
	}
}

func TestDetector_SuspectRefiresAfterRecovery(t *testing.T) {
	clock := newFakeClock(time.Now())
	d := NewDetectorWithConfig("self", Config{
		PhiThreshold:   1.0,
		SuspectTimeout: time.Hour,
		Clock:          clock,
	})

	var suspectCount atomic.Int64
	d.OnNodeSuspect(func(nodeID string) { suspectCount.Add(1) })

	for i := 0; i < 5; i++ {
		d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: int64(i + 1)})
		clock.Advance(50 * time.Millisecond)
	}

	// First suspect episode.
	clock.Advance(500 * time.Millisecond)
	d.CheckFailures()

	// Recovery: a fresh heartbeat clears suspicion.
	clock.Advance(10 * time.Millisecond)
	d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: 100})

	// Second suspect episode.
	clock.Advance(500 * time.Millisecond)
	d.CheckFailures()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && suspectCount.Load() < 2 {
		time.Sleep(time.Millisecond)
	}
	if got := suspectCount.Load(); got != 2 {
		t.Errorf("expected onNodeSuspect to fire again after recovery (2 episodes), got %d", got)
	}
}

// TestDetector_TeleratesGCPauseScaleJitter proves a single large gap
// (comparable to a stop-the-world GC pause) in an otherwise-regular
// heartbeat stream does not, by itself, get the node declared dead —
// the audit's specific concern that phi-accrual's tolerance for jitter
// was asserted but never actually tested.
func TestDetector_ToleratesGCPauseScaleJitter(t *testing.T) {
	clock := newFakeClock(time.Now())
	d := NewDetectorWithConfig("self", DefaultConfig())
	d.clock = clock

	// Establish a steady ~50ms baseline.
	for i := 0; i < 20; i++ {
		d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: int64(i + 1)})
		clock.Advance(50 * time.Millisecond)
	}

	// One large gap: 400ms, 8x the normal interval — representative of a
	// GC stop-the-world pause or a scheduler hiccup, not a real outage.
	clock.Advance(400 * time.Millisecond)
	if dead := d.CheckFailures(); len(dead) != 0 {
		t.Fatalf("a single GC-pause-scale gap must not declare the node dead outright, got dead=%v", dead)
	}

	// Heartbeats resume normally.
	d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: 21})
	for i := 0; i < 10; i++ {
		clock.Advance(50 * time.Millisecond)
		d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: int64(22 + i)})
		if dead := d.CheckFailures(); len(dead) != 0 {
			t.Fatalf("node must not be declared dead while heartbeats have resumed normally, got dead=%v", dead)
		}
	}
}

// TestDetector_ClockRollbackDoesNotProduceNegativeStats proves that even
// if the injected Clock reports time moving backward (which a real
// monotonic-carrying time.Time from time.Now() never does, but a
// misbehaving Clock implementation could), the detector clamps the
// resulting interval/elapsed to zero instead of corrupting its running
// statistics with a negative duration.
func TestDetector_ClockRollbackDoesNotProduceNegativeStats(t *testing.T) {
	clock := newFakeClock(time.Now())
	d := NewDetectorWithConfig("self", Config{Clock: clock})

	base := clock.Now()
	d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: 1})

	// Roll the clock backward by an hour, then record another heartbeat.
	clock.Set(base.Add(-time.Hour))
	d.RecordHeartbeat(HeartbeatMessage{NodeID: "node-1", Seq: 2})

	d.mu.RLock()
	entry := d.entries["node-1"]
	d.mu.RUnlock()
	if len(entry.samples) != 1 {
		t.Fatalf("expected exactly 1 recorded interval, got %d", len(entry.samples))
	}
	if entry.samples[0].d < 0 {
		t.Fatalf("expected a negative interval from clock rollback to be clamped to >= 0, got %v", entry.samples[0].d)
	}

	// NodeStatus's elapsed must also never go negative.
	elapsed, phi := d.NodeStatus("node-1")
	if elapsed < 0 {
		t.Errorf("expected elapsed to be clamped to >= 0, got %v", elapsed)
	}
	if math.IsNaN(phi) {
		t.Errorf("expected phi to remain a real number, got NaN")
	}
}
