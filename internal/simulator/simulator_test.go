package simulator

import (
	"sync"
	"testing"
	"time"
)

func TestShortID(t *testing.T) {
	if got := shortID("abcdefghij"); got != "abcdefgh" {
		t.Errorf("shortID long = %q, want %q", got, "abcdefgh")
	}
	if got := shortID("abc"); got != "abc" {
		t.Errorf("shortID short = %q, want %q", got, "abc")
	}
}

func TestNew(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New should not return nil")
	}
	if len(s.nodes) != 0 {
		t.Errorf("initial nodes = %d, want 0", len(s.nodes))
	}
	if s.IsActive() {
		t.Error("should not be active initially")
	}
}

func TestRegisterNode(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "10.0.0.1:7000")
	s.RegisterNode("n2", "10.0.0.2:7000")

	status := s.NodeStatus()
	if len(status) != 2 {
		t.Errorf("NodeStatus len = %d, want 2", len(status))
	}
	if !status["n1"] {
		t.Error("n1 should be running")
	}
	if !status["n2"] {
		t.Error("n2 should be running")
	}
}

func TestRegisterNode_OverwritesExisting(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr1")
	s.RegisterNode("n1", "addr2")

	status := s.NodeStatus()
	if len(status) != 1 {
		t.Errorf("should still have 1 node, got %d", len(status))
	}
}

func TestKillNode(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	if err := s.KillNode("n1"); err != nil {
		t.Fatalf("KillNode: %v", err)
	}

	status := s.NodeStatus()
	if status["n1"] {
		t.Error("n1 should not be running after kill")
	}
}

func TestKillNode_NotFound(t *testing.T) {
	s := New()
	err := s.KillNode("ghost")
	if err == nil {
		t.Error("expected error killing nonexistent node")
	}
}

func TestRestartNode(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.KillNode("n1")

	if err := s.RestartNode("n1"); err != nil {
		t.Fatalf("RestartNode: %v", err)
	}

	status := s.NodeStatus()
	if !status["n1"] {
		t.Error("n1 should be running after restart")
	}
}

func TestRestartNode_ClearsDelayAndDropRate(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.AddDelay("n1", 5*time.Second)
	s.AddPacketDrop("n1", 0.5)
	s.RestartNode("n1")

	node := s.nodes["n1"]
	if node.Delay != 0 {
		t.Errorf("Delay = %v, want 0 after restart", node.Delay)
	}
	if node.DropRate != 0 {
		t.Errorf("DropRate = %f, want 0 after restart", node.DropRate)
	}
}

func TestRestartNode_NotFound(t *testing.T) {
	s := New()
	err := s.RestartNode("ghost")
	if err == nil {
		t.Error("expected error restarting nonexistent node")
	}
}

func TestAddDelay(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	if err := s.AddDelay("n1", 100*time.Millisecond); err != nil {
		t.Fatalf("AddDelay: %v", err)
	}

	node := s.nodes["n1"]
	if node.Delay != 100*time.Millisecond {
		t.Errorf("Delay = %v, want 100ms", node.Delay)
	}
}

func TestAddDelay_NotFound(t *testing.T) {
	s := New()
	err := s.AddDelay("ghost", time.Second)
	if err == nil {
		t.Error("expected error")
	}
}

func TestAddPacketDrop(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	if err := s.AddPacketDrop("n1", 0.3); err != nil {
		t.Fatalf("AddPacketDrop: %v", err)
	}

	node := s.nodes["n1"]
	if node.DropRate != 0.3 {
		t.Errorf("DropRate = %f, want 0.3", node.DropRate)
	}
}

func TestAddPacketDrop_NotFound(t *testing.T) {
	s := New()
	err := s.AddPacketDrop("ghost", 0.5)
	if err == nil {
		t.Error("expected error")
	}
}

func TestRandomFailure_NoNodes(t *testing.T) {
	s := New()
	err := s.RandomFailure()
	if err == nil {
		t.Error("expected error with no nodes")
	}
}

func TestRandomFailure_AllKilled(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.KillNode("n1")
	err := s.RandomFailure()
	if err == nil {
		t.Error("expected error when all nodes are killed")
	}
}

func TestRandomFailure_SomeNodesRunning(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.RegisterNode("n2", "addr")
	s.KillNode("n1")

	err := s.RandomFailure()
	if err != nil {
		t.Errorf("RandomFailure should succeed with running nodes: %v", err)
	}
}

func TestRandomFailure_ActuallyFailsNode(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	for i := 0; i < 50; i++ {
		s.RestartNode("n1")
		s.RandomFailure()
		status := s.NodeStatus()
		if !status["n1"] {
			return
		}
	}
	t.Error("RandomFailure never killed the only node in 50 attempts")
}

func TestNodeStatus_AllRunning(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr1")
	s.RegisterNode("n2", "addr2")
	s.RegisterNode("n3", "addr3")

	status := s.NodeStatus()
	for id, running := range status {
		if !running {
			t.Errorf("node %s should be running", id)
		}
	}
}

func TestNodeStatus_Mixed(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr1")
	s.RegisterNode("n2", "addr2")
	s.KillNode("n1")

	status := s.NodeStatus()
	if status["n1"] {
		t.Error("n1 should not be running")
	}
	if !status["n2"] {
		t.Error("n2 should be running")
	}
}

func TestNodeStatus_ReturnsCopy(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	status := s.NodeStatus()
	status["n1"] = false

	if !s.nodes["n1"].Running {
		t.Error("modifying NodeStatus result should not affect internal state")
	}
}

func TestScenarios(t *testing.T) {
	s := New()
	scenarios := s.Scenarios()
	if len(scenarios) != 3 {
		t.Errorf("Scenarios len = %d, want 3", len(scenarios))
	}

	names := make(map[string]bool)
	for _, sc := range scenarios {
		names[sc.Name] = true
		if sc.Description == "" {
			t.Errorf("scenario %q has empty description", sc.Name)
		}
	}
	for _, want := range []string{"single_node_failure", "network_partition", "cascading_failure"} {
		if !names[want] {
			t.Errorf("missing scenario %q", want)
		}
	}
}

func TestScenario_SingleNodeFailure(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.RegisterNode("n2", "addr")

	scenarios := s.Scenarios()
	for _, sc := range scenarios {
		if sc.Name == "single_node_failure" {
			if err := sc.Action(s); err != nil {
				t.Fatalf("single_node_failure: %v", err)
			}
			n1 := s.nodes["n1"]
			n2 := s.nodes["n2"]
			changed := !n1.Running || n1.Delay != 0 || n1.DropRate != 0 ||
				!n2.Running || n2.Delay != 0 || n2.DropRate != 0
			if !changed {
				t.Error("single_node_failure should have affected at least one node")
			}
			return
		}
	}
	t.Error("single_node_failure scenario not found")
}

func TestScenario_NetworkPartition_TwoNodes(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.RegisterNode("n2", "addr")

	scenarios := s.Scenarios()
	for _, sc := range scenarios {
		if sc.Name == "network_partition" {
			if err := sc.Action(s); err != nil {
				t.Fatalf("network_partition: %v", err)
			}
			n1 := s.nodes["n1"]
			n2 := s.nodes["n2"]
			// The scenario iterates a map (non-deterministic order) and applies
			// delay to ids[0] and packet drop to ids[1]. Either node could be
			// first, so verify that exactly one has delay and the other has drop.
			hasDelay := map[bool]int{true: 0, false: 0}
			hasDrop := map[bool]int{true: 0, false: 0}
			hasDelay[n1.Delay > 0]++
			hasDelay[n2.Delay > 0]++
			hasDrop[n1.DropRate > 0]++
			hasDrop[n2.DropRate > 0]++
			if hasDelay[true] != 1 || hasDrop[true] != 1 {
				t.Errorf("partition should set delay on exactly one node and drop on the other, got delays=%d drops=%d",
					hasDelay[true], hasDrop[true])
			}
			return
		}
	}
}

func TestScenario_NetworkPartition_OneNode(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	scenarios := s.Scenarios()
	for _, sc := range scenarios {
		if sc.Name == "network_partition" {
			if err := sc.Action(s); err != nil {
				t.Fatalf("network_partition: %v", err)
			}
			n1 := s.nodes["n1"]
			if n1.Delay != 0 || n1.DropRate != 0 {
				t.Error("partition should not apply with < 2 nodes")
			}
			return
		}
	}
}

func TestScenario_CascadingFailure(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.RegisterNode("n2", "addr")
	s.RegisterNode("n3", "addr")

	scenarios := s.Scenarios()
	for _, sc := range scenarios {
		if sc.Name == "cascading_failure" {
			if err := sc.Action(s); err != nil {
				t.Fatalf("cascading_failure: %v", err)
			}
			time.Sleep(2 * time.Second)
			status := s.NodeStatus()
			dead := 0
			for _, running := range status {
				if !running {
					dead++
				}
			}
			if dead == 0 {
				t.Error("cascading failure should have killed at least 1 node")
			}
			return
		}
	}
}

func TestIsActive_InitiallyFalse(t *testing.T) {
	s := New()
	if s.IsActive() {
		t.Error("IsActive should be false initially")
	}
}

func TestIsActive_ConcurrentRead(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	var wg sync.WaitGroup
	const goroutines = 50
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = s.IsActive()
		}()
	}
	wg.Wait()
}

func TestChaos_ActiveFlag_DataRace(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	const iterations = 20
	for i := 0; i < iterations; i++ {
		s.Chaos(10*time.Millisecond, 5*time.Millisecond)

		var wg sync.WaitGroup
		wg.Add(20)
		for j := 0; j < 20; j++ {
			go func() {
				defer wg.Done()
				_ = s.IsActive()
			}()
		}
		wg.Wait()
		time.Sleep(15 * time.Millisecond)
	}
}

func TestConcurrentRegisterAndKill(t *testing.T) {
	s := New()
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			s.RegisterNode("n", "addr")
		}(i)
		go func(id int) {
			defer wg.Done()
			s.KillNode("n")
		}(i)
		go func() {
			defer wg.Done()
			_ = s.NodeStatus()
		}()
	}
	wg.Wait()
}

func TestConcurrentRandomFailure(t *testing.T) {
	s := New()
	for i := 0; i < 10; i++ {
		s.RegisterNode("n", "addr")
	}

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s.RandomFailure()
		}()
	}
	wg.Wait()
}

func TestSimNode_Fields(t *testing.T) {
	node := &SimNode{
		ID:       "n1",
		Address:  "10.0.0.1:7000",
		Running:  true,
		Delay:    100 * time.Millisecond,
		DropRate: 0.25,
	}
	if node.ID != "n1" {
		t.Errorf("ID = %q", node.ID)
	}
	if node.Address != "10.0.0.1:7000" {
		t.Errorf("Address = %q", node.Address)
	}
	if !node.Running {
		t.Error("Running should be true")
	}
	if node.Delay != 100*time.Millisecond {
		t.Errorf("Delay = %v", node.Delay)
	}
	if node.DropRate != 0.25 {
		t.Errorf("DropRate = %f", node.DropRate)
	}
}

func TestFailureType_Constants(t *testing.T) {
	if FailureKill != 0 {
		t.Errorf("FailureKill = %d, want 0", FailureKill)
	}
	if FailureDelay != 1 {
		t.Errorf("FailureDelay = %d, want 1", FailureDelay)
	}
	if FailureDrop != 2 {
		t.Errorf("FailureDrop = %d, want 2", FailureDrop)
	}
	if FailureRestart != 3 {
		t.Errorf("FailureRestart = %d, want 3", FailureRestart)
	}
}

func TestScenario_Action_CanKillNodes(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.RegisterNode("n2", "addr")
	s.RegisterNode("n3", "addr")

	err := s.KillNode("n1")
	if err != nil {
		t.Fatalf("KillNode: %v", err)
	}

	err = s.KillNode("n2")
	if err != nil {
		t.Fatalf("KillNode: %v", err)
	}

	status := s.NodeStatus()
	if status["n1"] || status["n2"] {
		t.Error("n1 and n2 should be dead")
	}
	if !status["n3"] {
		t.Error("n3 should still be running")
	}
}

func TestAddDelay_MultipleNodes(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.RegisterNode("n2", "addr")

	s.AddDelay("n1", 100*time.Millisecond)
	s.AddDelay("n2", 200*time.Millisecond)

	if s.nodes["n1"].Delay != 100*time.Millisecond {
		t.Errorf("n1 Delay = %v", s.nodes["n1"].Delay)
	}
	if s.nodes["n2"].Delay != 200*time.Millisecond {
		t.Errorf("n2 Delay = %v", s.nodes["n2"].Delay)
	}
}

func TestAddPacketDrop_Zero(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")
	s.AddPacketDrop("n1", 0.5)
	s.AddPacketDrop("n1", 0.0)

	if s.nodes["n1"].DropRate != 0.0 {
		t.Errorf("DropRate = %f, want 0 after reset", s.nodes["n1"].DropRate)
	}
}

func TestKillAndRestartCycle(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	for i := 0; i < 10; i++ {
		s.KillNode("n1")
		if s.NodeStatus()["n1"] {
			t.Fatalf("iteration %d: n1 should be dead", i)
		}
		s.RestartNode("n1")
		if !s.NodeStatus()["n1"] {
			t.Fatalf("iteration %d: n1 should be running", i)
		}
	}
}

func TestConcurrentNodeStatus(t *testing.T) {
	s := New()
	for i := 0; i < 20; i++ {
		s.RegisterNode("n", "addr")
	}

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			status := s.NodeStatus()
			_ = status["n"]
		}()
	}
	wg.Wait()
}

func TestConcurrentDelayAndKill(t *testing.T) {
	s := New()
	s.RegisterNode("n1", "addr")

	const goroutines = 30
	var wg sync.WaitGroup
	wg.Add(goroutines * 3)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s.AddDelay("n1", time.Millisecond)
		}()
		go func() {
			defer wg.Done()
			s.KillNode("n1")
		}()
		go func() {
			defer wg.Done()
			s.RestartNode("n1")
		}()
	}
	wg.Wait()
}

func TestNodeStatus_ReturnsCorrectCount(t *testing.T) {
	s := New()
	for i := 0; i < 100; i++ {
		s.RegisterNode("n", "addr")
	}

	status := s.NodeStatus()
	if len(status) != 1 {
		t.Errorf("NodeStatus should show 1 node (overwritten), got %d", len(status))
	}
}
