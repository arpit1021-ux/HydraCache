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
