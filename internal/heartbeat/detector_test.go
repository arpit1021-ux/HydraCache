package heartbeat

import (
	"testing"
	"time"
)

func TestMembershipAddMember(t *testing.T) {
	m := NewMembership()
	m.AddMember("node-1", "localhost:7379")

	if m.Count() != 1 {
		t.Errorf("expected 1 member, got %d", m.Count())
	}
	if m.AliveCount() != 1 {
		t.Errorf("expected 1 alive, got %d", m.AliveCount())
	}
}

func TestMembershipSetState(t *testing.T) {
	m := NewMembership()
	m.AddMember("node-1", "localhost:7379")

	m.SetState("node-1", MemberDead)

	alive := m.AliveMembers()
	if len(alive) != 0 {
		t.Errorf("expected 0 alive, got %d", len(alive))
	}

	dead := m.DeadMembers()
	if len(dead) != 1 {
		t.Errorf("expected 1 dead, got %d", len(dead))
	}
}

func TestDetectorRecordHeartbeat(t *testing.T) {
	d := NewDetector("self")

	d.RecordHeartbeat(HeartbeatMessage{
		NodeID:    "node-1",
		Seq:       1,
		Timestamp: time.Now(),
	})

	count := d.NodeCount()
	if count != 1 {
		t.Errorf("expected 1 node, got %d", count)
	}
}

func TestDetectorNodeStatus(t *testing.T) {
	d := NewDetector("self")

	d.RecordHeartbeat(HeartbeatMessage{
		NodeID:    "node-1",
		Seq:       1,
		Timestamp: time.Now(),
	})

	elapsed, phi := d.NodeStatus("node-1")
	if elapsed > time.Second {
		t.Errorf("expected recent heartbeat, got %v", elapsed)
	}
	if phi != 0 {
		t.Errorf("expected phi=0 for fresh heartbeat, got %f", phi)
	}
}

// TestFirstHeartbeatDoesNotCorruptPhi verifies that a single
// RecordHeartbeat for a brand-new peer does NOT inject a spurious
// multi-year interval into the statistics. Before the fix, the
// zero-value Timestamp caused time.Since(zero) ≈ 5.4 years to be
// recorded as the first interval, permanently inflating the mean
// and variance so that phi could never reach the death threshold.
func TestFirstHeartbeatDoesNotCorruptPhi(t *testing.T) {
	d := NewDetector("self")

	// First heartbeat — should only store timestamp, no interval.
	d.RecordHeartbeat(HeartbeatMessage{
		NodeID: "node-1",
		Seq:    1,
	})

	elapsed, phi := d.NodeStatus("node-1")
	if elapsed > time.Second {
		t.Fatalf("expected recent heartbeat after first, got %v elapsed", elapsed)
	}
	if phi != 0 {
		t.Fatalf("expected phi=0 after first heartbeat (no intervals yet), got %f", phi)
	}

	// Verify no intervals were recorded from the first heartbeat.
	entry := d.entries["node-1"]
	if len(entry.intervals) != 0 {
		t.Fatalf("expected 0 intervals after first heartbeat, got %d (spurious interval injected)", len(entry.intervals))
	}

	// Second heartbeat after a real gap.
	time.Sleep(100 * time.Millisecond)
	d.RecordHeartbeat(HeartbeatMessage{
		NodeID: "node-1",
		Seq:    2,
	})

	// Now there should be exactly 1 real interval (~100ms), not years.
	if len(entry.intervals) != 1 {
		t.Fatalf("expected 1 interval after second heartbeat, got %d", len(entry.intervals))
	}
	interval := entry.intervals[0]
	if interval < 50*time.Millisecond || interval > 500*time.Millisecond {
		t.Fatalf("expected interval ~100ms, got %v — first heartbeat likely corrupted timing", interval)
	}

	// Phi still returns 0 because we need ≥2 intervals for meaningful stats.
	phi = entry.Phi(time.Now())
	if phi != 0 {
		t.Fatalf("expected phi=0 with only 1 interval, got %f", phi)
	}

	// Third heartbeat to get 2 real intervals.
	time.Sleep(100 * time.Millisecond)
	d.RecordHeartbeat(HeartbeatMessage{
		NodeID: "node-1",
		Seq:    3,
	})

	if len(entry.intervals) != 2 {
		t.Fatalf("expected 2 intervals after third heartbeat, got %d", len(entry.intervals))
	}

	// Now phi should be computable and small (heartbeat just arrived).
	phi = entry.Phi(time.Now())
	if phi > 1.0 {
		t.Fatalf("expected low phi after fresh heartbeat, got %f (mean/variance still corrupted?)", phi)
	}
}

// TestDeadNodeDetectedWithinBoundedTime verifies the full failure
// detection lifecycle: several real heartbeats establish the baseline,
// then heartbeats stop, and CheckFailures returns the dead node within
// a bounded and reasonable time. This is the test that would have
// caught the original zero-Timestamp bug.
func TestDeadNodeDetectedWithinBoundedTime(t *testing.T) {
	d := NewDetector("self")
	d.suspectTimeout = 500 * time.Millisecond // speed up test

	// Simulate 10 real heartbeats at 50ms intervals to build statistics.
	for i := 0; i < 10; i++ {
		d.RecordHeartbeat(HeartbeatMessage{
			NodeID: "node-1",
			Seq:    int64(i + 1),
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Verify phi is below the death threshold right after a heartbeat.
	_, phi := d.NodeStatus("node-1")
	if phi > 8.0 {
		t.Fatalf("expected phi below death threshold after regular heartbeats, got %f", phi)
	}

	// Stop heartbeats and poll CheckFailures until node-1 is declared dead.
	// With mean ~50ms and suspectTimeout=500ms, detection should complete
	// within ~1 second of the last heartbeat.
	started := time.Now()
	for time.Since(started) < 10*time.Second {
		elapsed, phi := d.NodeStatus("node-1")
		dead := d.CheckFailures()
		if len(dead) > 0 {
			detectedElapsed := time.Since(started)
			t.Logf("node-1 declared dead after %v (phi=%.2f, heartbeatElapsed=%v)", detectedElapsed, phi, elapsed)
			if detectedElapsed > 3*time.Second {
				t.Fatalf("detection took too long: %v (expected < 3s with suspectTimeout=500ms)", detectedElapsed)
			}
			return
		}
		if time.Since(started) > 1*time.Second {
			t.Logf("still waiting: phi=%.2f, heartbeatElapsed=%v, intervals=%d", phi, elapsed, len(d.entries["node-1"].intervals))
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node-1 was not declared dead within 10s")
}
