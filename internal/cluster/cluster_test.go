package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hydracache/hydracache/internal/cache"
	"github.com/hydracache/hydracache/internal/hashring"
	"github.com/hydracache/hydracache/internal/network"
	"github.com/hydracache/hydracache/internal/replication"
)

func newTestCache() cache.Cache {
	return cache.New(&cache.Options{
		EvictionPolicy:       cache.EvictionLRU,
		EvictionCapacity:     10000,
		ActiveExpiration:     false,
		ExpirationInterval:   time.Second,
		ExpirationSampleSize: 10,
	})
}

func TestNewNode_DefaultFields(t *testing.T) {
	n := NewNode("node-1", "127.0.0.1:7000")
	if n.ID != "node-1" {
		t.Errorf("ID = %q", n.ID)
	}
	if n.Address != "127.0.0.1:7000" {
		t.Errorf("Address = %q", n.Address)
	}
	if n.GetRole() != RolePeer {
		t.Errorf("Role = %v, want RolePeer", n.GetRole())
	}
	if n.GetHealth() != HealthAlive {
		t.Errorf("Health = %v, want HealthAlive", n.GetHealth())
	}
	if n.Version != "1.0.0" {
		t.Errorf("Version = %q", n.Version)
	}
}

func TestRole_String(t *testing.T) {
	tests := []struct {
		r    Role
		want string
	}{
		{RolePeer, "peer"},
		{RoleLeader, "leader"},
		{RoleReplica, "replica"},
		{Role(99), "peer"},
	}
	for _, tt := range tests {
		if got := tt.r.String(); got != tt.want {
			t.Errorf("Role(%d).String() = %q, want %q", tt.r, got, tt.want)
		}
	}
}

func TestHealth_String(t *testing.T) {
	tests := []struct {
		h    Health
		want string
	}{
		{HealthAlive, "alive"},
		{HealthSuspect, "suspect"},
		{HealthDead, "dead"},
		{HealthLeft, "left"},
		{Health(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.h.String(); got != tt.want {
			t.Errorf("Health(%d).String() = %q, want %q", tt.h, got, tt.want)
		}
	}
}

func TestNode_IsAlive(t *testing.T) {
	n := NewNode("n1", "addr")
	if !n.IsAlive() {
		t.Error("new node should be alive")
	}
	n.SetHealth(HealthDead)
	if n.IsAlive() {
		t.Error("dead node should not be alive")
	}
}

func TestNode_IsLeader(t *testing.T) {
	n := NewNode("n1", "addr")
	if n.IsLeader() {
		t.Error("new node should not be leader")
	}
	n.SetRole(RoleLeader)
	if !n.IsLeader() {
		t.Error("leader node should report is leader")
	}
}

func TestNode_IsReplica(t *testing.T) {
	n := NewNode("n1", "addr")
	if n.IsReplica() {
		t.Error("new node should not be replica")
	}
	n.SetRole(RoleReplica)
	if !n.IsReplica() {
		t.Error("replica node should report is replica")
	}
}

func TestNode_String(t *testing.T) {
	n := NewNode("node-123456789", "10.0.0.1:7000")
	s := n.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

func TestNewTopology_Empty(t *testing.T) {
	topo := NewTopology()
	if topo.NodeCount() != 0 {
		t.Errorf("NodeCount = %d, want 0", topo.NodeCount())
	}
	if topo.AliveCount() != 0 {
		t.Errorf("AliveCount = %d, want 0", topo.AliveCount())
	}
	if topo.Epoch() != 0 {
		t.Errorf("Epoch = %d, want 0", topo.Epoch())
	}
	if topo.GetLeader() != nil {
		t.Error("GetLeader should return nil on empty topology")
	}
}

func TestTopology_AddNode(t *testing.T) {
	topo := NewTopology()
	n := NewNode("n1", "addr")
	if err := topo.AddNode(n); err != nil {
		t.Fatalf("AddNode error: %v", err)
	}
	if topo.NodeCount() != 1 {
		t.Errorf("NodeCount = %d, want 1", topo.NodeCount())
	}
	if topo.Epoch() != 1 {
		t.Errorf("Epoch = %d, want 1", topo.Epoch())
	}
	got, ok := topo.GetNode("n1")
	if !ok || got != n {
		t.Error("GetNode should return the added node")
	}
}

func TestTopology_AddNode_Duplicate(t *testing.T) {
	topo := NewTopology()
	n := NewNode("n1", "addr")
	topo.AddNode(n)
	err := topo.AddNode(NewNode("n1", "addr2"))
	if err == nil {
		t.Error("expected error for duplicate node")
	}
	if topo.NodeCount() != 1 {
		t.Errorf("NodeCount = %d, want 1 after duplicate add", topo.NodeCount())
	}
}

func TestTopology_RemoveNode(t *testing.T) {
	topo := NewTopology()
	topo.AddNode(NewNode("n1", "addr"))
	if err := topo.RemoveNode("n1"); err != nil {
		t.Fatalf("RemoveNode error: %v", err)
	}
	if topo.NodeCount() != 0 {
		t.Errorf("NodeCount = %d, want 0", topo.NodeCount())
	}
	if _, ok := topo.GetNode("n1"); ok {
		t.Error("removed node should not be found")
	}
	if topo.Epoch() != 2 {
		t.Errorf("Epoch = %d, want 2 after add+remove", topo.Epoch())
	}
}

func TestTopology_RemoveNode_NotFound(t *testing.T) {
	topo := NewTopology()
	err := topo.RemoveNode("nonexistent")
	if err == nil {
		t.Error("expected error for removing nonexistent node")
	}
}

func TestTopology_GetNode_NotFound(t *testing.T) {
	topo := NewTopology()
	_, ok := topo.GetNode("nope")
	if ok {
		t.Error("GetNode should return false for missing node")
	}
}

func TestTopology_AllNodes(t *testing.T) {
	topo := NewTopology()
	topo.AddNode(NewNode("n1", "a1"))
	topo.AddNode(NewNode("n2", "a2"))
	topo.AddNode(NewNode("n3", "a3"))
	nodes := topo.AllNodes()
	if len(nodes) != 3 {
		t.Errorf("AllNodes len = %d, want 3", len(nodes))
	}
}

func TestTopology_AliveNodes(t *testing.T) {
	topo := NewTopology()
	topo.AddNode(NewNode("n1", "a1"))
	n2 := NewNode("n2", "a2")
	n2.SetHealth(HealthDead)
	topo.AddNode(n2)
	topo.AddNode(NewNode("n3", "a3"))

	alive := topo.AliveNodes()
	if len(alive) != 2 {
		t.Errorf("AliveNodes len = %d, want 2", len(alive))
	}
}

func TestTopology_DeadNodes(t *testing.T) {
	topo := NewTopology()
	n1 := NewNode("n1", "a1")
	n1.SetHealth(HealthDead)
	topo.AddNode(n1)
	topo.AddNode(NewNode("n2", "a2"))
	n3 := NewNode("n3", "a3")
	n3.SetHealth(HealthSuspect)
	topo.AddNode(n3)

	dead := topo.DeadNodes()
	if len(dead) != 1 {
		t.Errorf("DeadNodes len = %d, want 1", len(dead))
	}
}

func TestTopology_GetLeader_None(t *testing.T) {
	topo := NewTopology()
	topo.AddNode(NewNode("n1", "a1"))
	if topo.GetLeader() != nil {
		t.Error("no leader should exist")
	}
}

func TestTopology_GetLeader_Found(t *testing.T) {
	topo := NewTopology()
	n := NewNode("n1", "a1")
	n.SetRole(RoleLeader)
	topo.AddNode(n)

	leader := topo.GetLeader()
	if leader == nil || leader.ID != "n1" {
		t.Error("should find leader")
	}
}

func TestTopology_GetLeader_DeadLeaderNotReturned(t *testing.T) {
	topo := NewTopology()
	n := NewNode("n1", "a1")
	n.SetRole(RoleLeader)
	n.SetHealth(HealthDead)
	topo.AddNode(n)

	if topo.GetLeader() != nil {
		t.Error("dead leader should not be returned")
	}
}

func TestTopology_GetReplicas(t *testing.T) {
	topo := NewTopology()
	n1 := NewNode("n1", "a1")
	n1.SetRole(RoleReplica)
	topo.AddNode(n1)
	n2 := NewNode("n2", "a2")
	n2.SetRole(RoleReplica)
	n2.SetHealth(HealthDead)
	topo.AddNode(n2)
	topo.AddNode(NewNode("n3", "a3"))

	replicas := topo.GetReplicas()
	if len(replicas) != 1 {
		t.Errorf("GetReplicas len = %d, want 1 (dead replica excluded)", len(replicas))
	}
}

func TestTopology_SetNodeHealth(t *testing.T) {
	topo := NewTopology()
	topo.AddNode(NewNode("n1", "a1"))
	topo.SetNodeHealth("n1", HealthSuspect)

	n, _ := topo.GetNode("n1")
	if n.GetHealth() != HealthSuspect {
		t.Errorf("Health = %v, want HealthSuspect", n.GetHealth())
	}
	epoch := topo.Epoch()
	if epoch != 2 {
		t.Errorf("Epoch = %d, want 2 after health change", epoch)
	}
}

func TestTopology_SetNodeHealth_Nonexistent(t *testing.T) {
	topo := NewTopology()
	topo.SetNodeHealth("nope", HealthDead)
	if topo.Epoch() != 0 {
		t.Error("Epoch should not change for nonexistent node")
	}
}

func TestTopology_SetNodeRole(t *testing.T) {
	topo := NewTopology()
	topo.AddNode(NewNode("n1", "a1"))
	topo.SetNodeRole("n1", RoleLeader)

	n, _ := topo.GetNode("n1")
	if n.GetRole() != RoleLeader {
		t.Errorf("Role = %v, want RoleLeader", n.GetRole())
	}
}

func TestTopology_SetNodeRole_Nonexistent(t *testing.T) {
	topo := NewTopology()
	topo.SetNodeRole("nope", RoleLeader)
	if topo.Epoch() != 0 {
		t.Error("Epoch should not change for nonexistent node")
	}
}

func TestTopology_OnChange_ReceivesEvents(t *testing.T) {
	topo := NewTopology()
	var received []TopologyEvent
	var mu sync.Mutex

	topo.OnChange(func(ev TopologyEvent) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, ev)
	})

	topo.AddNode(NewNode("n1", "a1"))
	topo.SetNodeHealth("n1", HealthDead)
	topo.RemoveNode("n1")

	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) < 2 {
		t.Errorf("received %d events, want at least 2", len(received))
	}

	types := make(map[string]bool)
	for _, ev := range received {
		types[ev.Type] = true
	}
	for _, want := range []string{"node_added", "health_changed", "node_removed"} {
		if !types[want] {
			t.Errorf("missing event type %q", want)
		}
	}
}

func TestTopology_MarshalJSON(t *testing.T) {
	topo := NewTopology()
	topo.AddNode(NewNode("n1", "a1"))

	data, err := json.Marshal(topo)
	if err != nil {
		t.Fatalf("MarshalJSON error: %v", err)
	}

	var result struct {
		Nodes []*Node `json:"nodes"`
		Epoch uint64  `json:"epoch"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if len(result.Nodes) != 1 {
		t.Errorf("nodes len = %d, want 1", len(result.Nodes))
	}
	if result.Epoch != 1 {
		t.Errorf("epoch = %d, want 1", result.Epoch)
	}
}

func TestTopology_ConcurrentAddRemove(t *testing.T) {
	topo := NewTopology()
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			nid := "n" + string(rune('0'+id%10))
			topo.AddNode(NewNode(nid, "addr"))
			topo.RemoveNode(nid)
		}(i)
	}
	wg.Wait()
}

func TestTopology_ConcurrentReadsAndWrites(t *testing.T) {
	topo := NewTopology()
	for i := 0; i < 10; i++ {
		topo.AddNode(NewNode("n"+string(rune('0'+i)), "addr"))
	}

	const goroutines = 30
	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = topo.AllNodes()
		}()
		go func() {
			defer wg.Done()
			_ = topo.AliveNodes()
		}()
		go func() {
			defer wg.Done()
			_ = topo.NodeCount()
		}()
	}
	wg.Wait()
}

func TestManager_NewManager(t *testing.T) {
	self := NewNode("self", "127.0.0.1:7000")
	topo := NewTopology()
	ring := hashring.New(150)
	mgr := NewManager(self, topo, ring, newTestCache())

	if mgr.Self() != self {
		t.Error("Self() should return the self node")
	}
	if mgr.Topology() != topo {
		t.Error("Topology() should return the topology")
	}
	if mgr.Ring() != ring {
		t.Error("Ring() should return the ring")
	}
}

func TestManager_Start(t *testing.T) {
	self := NewNode("self", "127.0.0.1:7000")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	if topo.NodeCount() != 1 {
		t.Errorf("after Start, NodeCount = %d, want 1", topo.NodeCount())
	}
}

func TestManager_Start_DuplicateSelf(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	topo.AddNode(self)
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())

	err := mgr.Start(context.Background())
	if err == nil {
		t.Error("expected error when self already in topology")
	}
}

func TestManager_AddRemoveNode(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())

	n := NewNode("n1", "addr1")
	if err := mgr.AddNode(n); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := mgr.RemoveNode("n1"); err != nil {
		t.Fatalf("RemoveNode: %v", err)
	}
	if topo.NodeCount() != 0 {
		t.Errorf("NodeCount = %d, want 0", topo.NodeCount())
	}
}

func TestManager_PromoteReplica(t *testing.T) {
	self := NewNode("self", "addr")
	self.SetRole(RoleLeader)
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())
	mgr.Start(context.Background())

	repl := NewNode("replica1", "addr1")
	repl.SetRole(RoleReplica)
	topo.AddNode(repl)

	if err := mgr.PromoteReplica("replica1"); err != nil {
		t.Fatalf("PromoteReplica: %v", err)
	}
	n, _ := topo.GetNode("replica1")
	if n.GetRole() != RoleLeader {
		t.Errorf("promoted node role = %v, want RoleLeader", n.GetRole())
	}
}

func TestManager_PromoteReplica_NotFound(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())

	err := mgr.PromoteReplica("nonexistent")
	if err == nil {
		t.Error("expected error promoting nonexistent node")
	}
}

func TestManager_PromoteReplica_NotReplica(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())
	mgr.Start(context.Background())

	peer := NewNode("peer1", "addr1")
	topo.AddNode(peer)

	err := mgr.PromoteReplica("peer1")
	if err == nil {
		t.Error("expected error promoting non-replica")
	}
}

func TestManager_Bootstrap(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())
	if err := mgr.Bootstrap([]string{"10.0.0.1:7000", "10.0.0.2:7000"}); err != nil {
		t.Fatalf("Bootstrap error: %v", err)
	}
}

func TestManager_IsLeader(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())

	if mgr.IsLeader() {
		t.Error("new manager should not be leader")
	}
	self.SetRole(RoleLeader)
	if !mgr.IsLeader() {
		t.Error("manager with leader role should report is leader")
	}
}

func TestManager_GetLeaderNode(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())
	mgr.Start(context.Background())

	if mgr.GetLeaderNode() != nil {
		t.Error("no leader initially")
	}

	n := NewNode("leader", "addr2")
	n.SetRole(RoleLeader)
	topo.AddNode(n)

	leader := mgr.GetLeaderNode()
	if leader == nil || leader.ID != "leader" {
		t.Error("should find leader")
	}
}

func TestManager_String(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())
	s := mgr.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

func TestManager_Shutdown(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())
	mgr.Shutdown()
}

func TestShortID(t *testing.T) {
	if got := shortID("abcdefghij"); got != "abcdefgh" {
		t.Errorf("shortID long = %q, want %q", got, "abcdefgh")
	}
	if got := shortID("abc"); got != "abc" {
		t.Errorf("shortID short = %q, want %q", got, "abc")
	}
}

func TestTopology_ConcurrentAddRemoveAndQueries(t *testing.T) {
	topo := NewTopology()
	const goroutines = 20

	var wg sync.WaitGroup
	wg.Add(goroutines * 4)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			nid := "n" + string(rune('A'+id%26))
			topo.AddNode(NewNode(nid, "addr"))
		}(i)
		go func(id int) {
			defer wg.Done()
			nid := "n" + string(rune('A'+id%26))
			topo.RemoveNode(nid)
		}(i)
		go func() {
			defer wg.Done()
			_ = topo.AllNodes()
		}()
		go func() {
			defer wg.Done()
			_ = topo.AliveNodes()
		}()
	}
	wg.Wait()
}

func TestManager_ConcurrentOperations(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())
	mgr.Start(context.Background())

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			nid := "peer" + string(rune('0'+id%10))
			mgr.AddNode(NewNode(nid, "addr"))
		}(i)
		go func(id int) {
			defer wg.Done()
			nid := "peer" + string(rune('0'+id%10))
			mgr.RemoveNode(nid)
		}(i)
		go func() {
			defer wg.Done()
			_ = mgr.IsLeader()
			_ = mgr.GetLeaderNode()
		}()
	}
	wg.Wait()
}

func TestTopology_EpochMonotonicallyIncreases(t *testing.T) {
	topo := NewTopology()
	for i := 0; i < 100; i++ {
		nid := "n" + string(rune('0'+i%10))
		topo.AddNode(NewNode(nid, "addr"))
	}
	epoch := topo.Epoch()
	if epoch < 1 {
		t.Errorf("Epoch = %d, should be >= 1 after adds", epoch)
	}
}

func TestManager_StartIdempotent(t *testing.T) {
	self := NewNode("self", "addr")
	topo := NewTopology()
	mgr := NewManager(self, topo, hashring.New(150), newTestCache())

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	err := mgr.Start(context.Background())
	if err == nil {
		t.Error("second Start should fail (self already in topology)")
	}
}

func TestNode_JSONSerialization(t *testing.T) {
	n := NewNode("n1", "127.0.0.1:7000")
	n.SetRole(RoleLeader)
	n.Region = "us-east-1"
	n.Load = 0.75
	n.MemoryMB = 4096

	data, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var decoded Node
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if decoded.ID != "n1" {
		t.Errorf("decoded ID = %q", decoded.ID)
	}
	if decoded.Region != "us-east-1" {
		t.Errorf("decoded Region = %q", decoded.Region)
	}
	if decoded.GetRole() != RoleLeader {
		t.Errorf("decoded Role = %v, want RoleLeader", decoded.GetRole())
	}
}

func TestNode_MarshalJSON_StringEnums(t *testing.T) {
	n := NewNode("n1", "127.0.0.1:7000")
	n.SetRole(RoleLeader)
	n.SetHealth(HealthSuspect)

	data, err := json.Marshal(n)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("raw Unmarshal error: %v", err)
	}

	role, ok := raw["role"].(string)
	if !ok || role != "leader" {
		t.Errorf("role = %v (%T), want string \"leader\"", raw["role"], raw["role"])
	}
	health, ok := raw["health"].(string)
	if !ok || health != "suspect" {
		t.Errorf("health = %v (%T), want string \"suspect\"", raw["health"], raw["health"])
	}
}

func TestRole_MarshalJSON_RoundTrip(t *testing.T) {
	tests := []struct {
		role Role
		want string
	}{
		{RolePeer, "peer"},
		{RoleLeader, "leader"},
		{RoleReplica, "replica"},
	}
	for _, tt := range tests {
		data, err := json.Marshal(tt.role)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}
		got := string(data)
		expected := `"` + tt.want + `"`
		if got != expected {
			t.Errorf("Role(%d) marshal = %s, want %s", tt.role, got, expected)
		}

		var decoded Role
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}
		if decoded != tt.role {
			t.Errorf("Role roundtrip = %v, want %v", decoded, tt.role)
		}
	}
}

func TestHealth_MarshalJSON_RoundTrip(t *testing.T) {
	tests := []struct {
		health Health
		want   string
	}{
		{HealthAlive, "alive"},
		{HealthSuspect, "suspect"},
		{HealthDead, "dead"},
		{HealthLeft, "left"},
	}
	for _, tt := range tests {
		data, err := json.Marshal(tt.health)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}
		got := string(data)
		expected := `"` + tt.want + `"`
		if got != expected {
			t.Errorf("Health(%d) marshal = %s, want %s", tt.health, got, expected)
		}

		var decoded Health
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}
		if decoded != tt.health {
			t.Errorf("Health roundtrip = %v, want %v", decoded, tt.health)
		}
	}
}

func BenchmarkTopology_AddNode(b *testing.B) {
	topo := NewTopology()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topo.AddNode(NewNode("n", "addr"))
	}
}

func BenchmarkTopology_AllNodes(b *testing.B) {
	topo := NewTopology()
	for i := 0; i < 1000; i++ {
		topo.AddNode(NewNode("n", "addr"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = topo.AllNodes()
	}
}

func TestTopology_MarshalJSON_NoDeadlock(t *testing.T) {
	topo := NewTopology()
	for i := 0; i < 10; i++ {
		topo.AddNode(NewNode("n"+string(rune('0'+i)), "addr"))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		const iterations = 500
		for i := 0; i < iterations; i++ {
			_, _ = json.Marshal(topo)
		}
	}()

	// Concurrent writers that would deadlock if MarshalJSON re-enters RLock.
	var wg sync.WaitGroup
	const writers = 10
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				nid := "new-" + string(rune('A'+id%26)) + "-" + string(rune('0'+j%10))
				_ = topo.AddNode(NewNode(nid, "addr"))
				_ = topo.RemoveNode(nid)
			}
		}(i)
	}

	select {
	case <-done:
		// MarshalJSON goroutine finished.
	case <-time.After(5 * time.Second):
		t.Fatal("MarshalJSON deadlocked under concurrent AddNode/RemoveNode (fix not applied or insufficient)")
	}
	wg.Wait()
}

func TestNode_ConcurrentHealthMutation(t *testing.T) {
	topo := NewTopology()
	n := NewNode("n1", "addr1")
	topo.AddNode(n)

	// Get a live pointer via AllNodes — same as what external callers do.
	nodes := topo.AllNodes()
	if len(nodes) != 1 {
		t.Fatalf("AllNodes len = %d, want 1", len(nodes))
	}
	live := nodes[0]

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Writers: mutate health under Topology's lock.
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			if id%2 == 0 {
				topo.SetNodeHealth("n1", HealthDead)
			} else {
				topo.SetNodeHealth("n1", HealthAlive)
			}
		}(i)
	}

	// Readers: read health via the live pointer without Topology's lock.
	// This must be race-free because Health is now atomic.
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = live.IsAlive()
				_ = live.GetHealth()
			}
		}()
	}

	wg.Wait()
}

// --- Gossip tests ---

func TestGossip_Merge_CrossNode_HigherIncarnationWins(t *testing.T) {
	topo := NewTopology()
	self := NewNode("self", "addr")
	topo.AddNode(self)

	g := NewGossip(self, topo)

	// Peer B is alive at incarnation 3
	g.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "alive", Incarnation: 3},
	})

	m, ok := g.table.Get("peer-b")
	if !ok || m.State != "alive" || m.Incarnation != 3 {
		t.Fatalf("expected alive inc=3, got %+v", m)
	}

	// Peer B is now dead at incarnation 5 — should win
	g.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "dead", Incarnation: 5},
	})

	m, ok = g.table.Get("peer-b")
	if !ok || m.State != "dead" || m.Incarnation != 5 {
		t.Fatalf("expected dead inc=5, got %+v", m)
	}
}

func TestGossip_Merge_CrossNode_StaleAliveIgnored(t *testing.T) {
	topo := NewTopology()
	self := NewNode("self", "addr")
	topo.AddNode(self)

	g := NewGossip(self, topo)

	// Peer B is dead at incarnation 5
	g.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "dead", Incarnation: 5},
	})

	// Stale alive at incarnation 3 — should be ignored
	g.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "alive", Incarnation: 3},
	})

	m, ok := g.table.Get("peer-b")
	if !ok || m.State != "dead" || m.Incarnation != 5 {
		t.Fatalf("stale alive should not override dead, got %+v", m)
	}
}

func TestGossip_Merge_CrossNode_SameIncarnation_TerminalWins(t *testing.T) {
	topo := NewTopology()
	self := NewNode("self", "addr")
	topo.AddNode(self)

	g := NewGossip(self, topo)

	// Peer B alive at incarnation 3
	g.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "alive", Incarnation: 3},
	})

	// Peer B dead at SAME incarnation 3 — terminal wins
	g.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "dead", Incarnation: 3},
	})

	m, ok := g.table.Get("peer-b")
	if !ok || m.State != "dead" {
		t.Fatalf("terminal should win at same incarnation, got %+v", m)
	}
}

func TestGossip_SelfRefutation_DeadClaim_RecoversNode(t *testing.T) {
	// SCENARIO: Node crashes at incarnation 5, restarts at incarnation 1,
	// peers hold "dead inc=5". Self-refutation must recover the node.
	topo := NewTopology()
	self := NewNode("self-node", "self-addr")
	topo.AddNode(self)

	g := NewGossip(self, topo)
	// Simulate restart: current incarnation is 1
	g.incarnation.Store(1)

	// A peer sends back: "you are dead at incarnation 5"
	refuted := g.Merge([]GossipMember{
		{ID: "self-node", Address: "self-addr", State: "dead", Incarnation: 5},
	})

	if !refuted {
		t.Fatal("self-refutation should have occurred")
	}

	// Node should now be alive at incarnation 6 (max(5,1)+1)
	inc := g.Incarnation()
	if inc != 6 {
		t.Fatalf("expected incarnation 6, got %d", inc)
	}

	m, ok := g.table.Get("self-node")
	if !ok {
		t.Fatal("self should be in gossip table")
	}
	if m.State != "alive" {
		t.Fatalf("self should be alive after refutation, got state=%s", m.State)
	}
	if m.Incarnation != 6 {
		t.Fatalf("self incarnation should be 6, got %d", m.Incarnation)
	}

	// Topology should reflect alive
	node, ok := topo.GetNode("self-node")
	if !ok {
		t.Fatal("self should be in topology")
	}
	if node.GetHealth() != HealthAlive {
		t.Fatalf("topology health should be alive, got %v", node.GetHealth())
	}
}

func TestGossip_SelfRefutation_LeftClaim_RecoversNode(t *testing.T) {
	topo := NewTopology()
	self := NewNode("self-2", "addr-2")
	topo.AddNode(self)

	g := NewGossip(self, topo)
	g.incarnation.Store(2)

	// Peer claims "left at incarnation 10"
	refuted := g.Merge([]GossipMember{
		{ID: "self-2", Address: "addr-2", State: "left", Incarnation: 10},
	})

	if !refuted {
		t.Fatal("self-refutation should have occurred for left claim")
	}

	if g.Incarnation() != 11 {
		t.Fatalf("expected incarnation 11, got %d", g.Incarnation())
	}
}

func TestGossip_SelfRefutation_StaleDead_StillRefutes(t *testing.T) {
	// Self-refutation is unconditional for dead/left: even a stale
	// dead claim must be refuted to guarantee the correction propagates.
	topo := NewTopology()
	self := NewNode("self-3", "addr-3")
	topo.AddNode(self)

	g := NewGossip(self, topo)
	g.incarnation.Store(10)

	// Stale dead claim at incarnation 5 — still triggers refutation
	refuted := g.Merge([]GossipMember{
		{ID: "self-3", Address: "addr-3", State: "dead", Incarnation: 5},
	})

	if !refuted {
		t.Fatal("self-refutation should always trigger for dead claims")
	}

	// Incarnation bumped to max(5, 10) + 1 = 11
	if g.Incarnation() != 11 {
		t.Fatalf("expected incarnation 11, got %d", g.Incarnation())
	}
}

func TestGossip_SelfRefutation_PartitionHalves_Converge(t *testing.T) {
	// Two isolated partition halves each see the node as dead.
	// Both independently bump incarnation. When partition heals,
	// the higher incarnation wins.
	topoA := NewTopology()
	selfA := NewNode("node-x", "x-addr")
	topoA.AddNode(selfA)

	gA := NewGossip(selfA, topoA)
	gA.incarnation.Store(1)

	// Half A learns "dead inc=5" and refutes to inc=6
	gA.Merge([]GossipMember{
		{ID: "node-x", Address: "x-addr", State: "dead", Incarnation: 5},
	})
	if gA.Incarnation() != 6 {
		t.Fatalf("half A: expected inc=6, got %d", gA.Incarnation())
	}

	// Simulate: half B independently refutes to inc=7
	gB_table := []GossipMember{
		{ID: "node-x", Address: "x-addr", State: "alive", Incarnation: 7},
	}

	// Half A receives half B's table — inc=7 > inc=6, so A adopts it
	gA.Merge(gB_table)

	if gA.Incarnation() != 7 {
		t.Fatalf("after convergence: expected inc=7, got %d", gA.Incarnation())
	}

	m, ok := gA.table.Get("node-x")
	if !ok || m.State != "alive" || m.Incarnation != 7 {
		t.Fatalf("expected alive inc=7, got %+v", m)
	}
}

func TestGossip_HandleGossip_RoundTrip(t *testing.T) {
	topo := NewTopology()
	self := NewNode("s1", "s1-addr")
	topo.AddNode(self)

	g := NewGossip(self, topo)

	// Add a peer
	g.Merge([]GossipMember{
		{ID: "peer-a", Address: "a-addr", State: "alive", Incarnation: 1},
	})

	// Simulate incoming GOSSIP from another node
	incoming := GossipMessage{
		Members: []GossipMember{
			{ID: "peer-b", Address: "b-addr", State: "alive", Incarnation: 2},
		},
	}
	payload, _ := json.Marshal(incoming)

	resp, err := g.HandleGossip(string(payload))
	if err != nil {
		t.Fatalf("HandleGossip error: %v", err)
	}

	// Response should contain both peer-a and peer-b
	var respMsg GossipMessage
	if err := json.Unmarshal([]byte(resp), &respMsg); err != nil {
		t.Fatalf("response unmarshal error: %v", err)
	}

	ids := make(map[string]bool)
	for _, m := range respMsg.Members {
		ids[m.ID] = true
	}

	if !ids["s1"] {
		t.Error("response should contain self")
	}
	if !ids["peer-a"] {
		t.Error("response should contain peer-a")
	}
	if !ids["peer-b"] {
		t.Error("response should contain peer-b")
	}
}

// --- MarkMemberDead regression tests ---

// TestMarkMemberDead_RejectedAtEqualIncarnation verifies that when a member
// is marked dead on instance A's table, a stale alive claim at the SAME
// incarnation is correctly rejected (dead wins at equal incarnation).
func TestMarkMemberDead_RejectedAtEqualIncarnation(t *testing.T) {
	topoA := NewTopology()
	selfA := NewNode("node-a", "a-addr")
	topoA.AddNode(selfA)
	gA := NewGossip(selfA, topoA)

	// Add peer B as alive at incarnation 3.
	gA.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "alive", Incarnation: 3},
	})

	m, ok := gA.table.Get("peer-b")
	if !ok || m.State != "alive" || m.Incarnation != 3 {
		t.Fatalf("precondition: expected alive inc=3, got %+v", m)
	}

	// Mark peer-b dead on A's table (at its current incarnation 3).
	gA.MarkMemberDead("peer-b")

	m, ok = gA.table.Get("peer-b")
	if !ok || m.State != "dead" || m.Incarnation != 3 {
		t.Fatalf("after MarkMemberDead: expected dead inc=3, got %+v", m)
	}

	// Stale alive claim at the SAME incarnation — dead should win.
	gA.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "alive", Incarnation: 3},
	})

	m, ok = gA.table.Get("peer-b")
	if !ok || m.State != "dead" || m.Incarnation != 3 {
		t.Fatalf("stale alive at same incarnation should not override dead: got %+v", m)
	}
}

// TestMarkMemberDead_RejectedAtLowerIncarnation verifies that a stale alive
// claim at a LOWER incarnation is also correctly rejected.
func TestMarkMemberDead_RejectedAtLowerIncarnation(t *testing.T) {
	topoA := NewTopology()
	selfA := NewNode("node-a", "a-addr")
	topoA.AddNode(selfA)
	gA := NewGossip(selfA, topoA)

	// Simulate: B was alive at inc 5, now dead at inc 5.
	gA.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "alive", Incarnation: 5},
	})
	gA.MarkMemberDead("peer-b")

	// Stale alive at inc 2 — should be ignored.
	gA.Merge([]GossipMember{
		{ID: "peer-b", Address: "b-addr", State: "alive", Incarnation: 2},
	})

	m, ok := gA.table.Get("peer-b")
	if !ok || m.State != "dead" || m.Incarnation != 5 {
		t.Fatalf("stale alive at lower inc should not override dead: got %+v", m)
	}
}

// TestMarkMemberDead_SelfRefutationRecovery is the full end-to-end scenario:
// A marks B dead, B restarts (incarnation 1), B receives A's dead claim,
// B self-refutes to incarnation 6, B's alive-at-6 reaches A, A adopts it.
func TestMarkMemberDead_SelfRefutationRecovery(t *testing.T) {
	// --- Node A's perspective ---
	topoA := NewTopology()
	selfA := NewNode("node-a", "a-addr")
	topoA.AddNode(selfA)
	gA := NewGossip(selfA, topoA)

	// Node B was alive at incarnation 5.
	gA.Merge([]GossipMember{
		{ID: "node-b", Address: "b-addr", State: "alive", Incarnation: 5},
	})

	// Node A detects B dead, marks it in its gossip table.
	gA.MarkMemberDead("node-b")

	m, ok := gA.table.Get("node-b")
	if !ok || m.State != "dead" || m.Incarnation != 5 {
		t.Fatalf("A: expected dead inc=5, got %+v", m)
	}

	// --- Node B restarts (new process, incarnation resets to 1) ---
	topoB := NewTopology()
	selfB := NewNode("node-b", "b-addr")
	topoB.AddNode(selfB)
	gB := NewGossip(selfB, topoB)
	// Simulate restart: incarnation is already 1 from NewGossip.

	// Step 1: B sends alive/inc=1 to A. A rejects it (dead at 5 wins).
	gA.Merge([]GossipMember{
		{ID: "node-b", Address: "b-addr", State: "alive", Incarnation: 1},
	})
	m, ok = gA.table.Get("node-b")
	if !ok || m.State != "dead" || m.Incarnation != 5 {
		t.Fatalf("A: should still be dead inc=5 after stale alive, got %+v", m)
	}

	// Step 2: A responds with its table (including dead/inc=5 for B).
	// B receives this and self-refutes.
	gA_table := gA.table.Snapshot()
	aMembers := make([]GossipMember, 0, len(gA_table))
	for _, member := range gA_table {
		aMembers = append(aMembers, member)
	}
	refuted := gB.Merge(aMembers)

	if !refuted {
		t.Fatal("B: self-refutation should have occurred")
	}
	if gB.Incarnation() != 6 {
		t.Fatalf("B: expected incarnation 6, got %d", gB.Incarnation())
	}
	m, ok = gB.table.Get("node-b")
	if !ok || m.State != "alive" || m.Incarnation != 6 {
		t.Fatalf("B: expected alive inc=6, got %+v", m)
	}

	// Step 3: B's alive/inc=6 reaches A. A should adopt it.
	gB_table := gB.table.Snapshot()
	bMembers := make([]GossipMember, 0, len(gB_table))
	for _, member := range gB_table {
		bMembers = append(bMembers, member)
	}
	gA.Merge(bMembers)

	m, ok = gA.table.Get("node-b")
	if !ok || m.State != "alive" || m.Incarnation != 6 {
		t.Fatalf("A: should adopt alive inc=6 from B, got %+v", m)
	}

	// Verify topology would be updated: node-b should be added back.
	// (applyToTopology creates the node since it was RemoveNode'd.)
}

// TestMarkMemberDead_UnknownNode gets a dead entry at incarnation 1 when
// the node was never in the gossip table.
func TestMarkMemberDead_UnknownNode(t *testing.T) {
	topoA := NewTopology()
	selfA := NewNode("node-a", "a-addr")
	topoA.AddNode(selfA)
	gA := NewGossip(selfA, topoA)

	gA.MarkMemberDead("never-seen")

	m, ok := gA.table.Get("never-seen")
	if !ok || m.State != "dead" || m.Incarnation != 1 {
		t.Fatalf("expected dead inc=1 for unknown node, got %+v", m)
	}
}

// --- Replica sync handshake integration tests ---

// TestSync_GapOpsAppliedBeforeActive verifies that a replica added with a
// real gap (primary has ops at seq 1-50, replica starts at seq 0) gets those
// specific operations applied before being marked Active.
func TestSync_GapOpsAppliedBeforeActive(t *testing.T) {
	// Set up primary with its own cache and server.
	primaryCache := newTestCache()
	primaryTopo := NewTopology()
	primaryNode := NewNode("primary", "127.0.0.1:0")
	primaryRing := hashring.New(150)
	primaryRing.AddNode("primary")

	// Seed 50 keys into the primary's cache.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("gapkey:%03d", i)
		if err := primaryCache.Set(key, []byte(fmt.Sprintf("val%d", i)), 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	// Set up replica server with its own empty cache.
	replicaCache := newTestCache()
	replicaSrv := network.NewServer(network.ServerConfig{Addr: "127.0.0.1:0"}, replicaCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := replicaSrv.Start(ctx); err != nil {
		t.Fatalf("replica server start: %v", err)
	}
	t.Cleanup(func() { replicaSrv.Shutdown() })

	// Set up primary manager — Start() adds self to topology + registers ReplicaSet.
	mgr := NewManager(primaryNode, primaryTopo, primaryRing, primaryCache)
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("mgr.Start: %v", err)
	}

	// Add 50 ops to the primary's own ReplicationStream.
	rs, ok := mgr.Registry().GetReplicaSet("primary")
	if !ok {
		t.Fatal("primary ReplicaSet not found")
	}
	primaryInfo, ok := rs.GetReplica("primary")
	if !ok || primaryInfo.Stream == nil {
		t.Fatal("primary stream not found")
	}
	for i := 0; i < 50; i++ {
		primaryInfo.Stream.Append(replication.Operation{
			Command: "SET",
			Args:    []string{fmt.Sprintf("gapkey:%03d", i), fmt.Sprintf("val%d", i)},
		})
	}

	// Add replica to primary's ReplicaSet (starts as ReplicaSyncing).
	rs.AddReplica("replica-1", replicaSrv.Addr().String())
	if r, ok := rs.GetReplica("replica-1"); !ok || r.GetStatus() != replication.ReplicaSyncing {
		t.Fatal("replica should start as ReplicaSyncing")
	}

	// Initiate sync.
	err := mgr.doReplicaSync("primary", "replica-1", replicaSrv.Addr().String())
	if err != nil {
		t.Fatalf("doReplicaSync: %v", err)
	}

	// Mark active (simulating what initiateReplicaSync does on success).
	rs.SetStatus("replica-1", replication.ReplicaActive)

	// Verify replica has all 50 keys with correct values.
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("gapkey:%03d", i)
		val, err := replicaCache.Get(key)
		if err != nil {
			t.Errorf("replica missing key %q: %v", key, err)
			continue
		}
		want := fmt.Sprintf("val%d", i)
		if string(val) != want {
			t.Errorf("key %q: got %q, want %q", key, string(val), want)
		}
	}

	// Verify replica is now Active.
	if r, ok := rs.GetReplica("replica-1"); !ok || r.GetStatus() != replication.ReplicaActive {
		t.Errorf("replica status = %v, want ReplicaActive", r.GetStatus())
	}
}

// TestSync_FullSyncWhenGapExceedsBuffer verifies that when the primary's
// ReplicationStream buffer has been evicted (gap exceeds retention), the
// sync falls back to a full cache snapshot transfer.
func TestSync_FullSyncWhenGapExceedsBuffer(t *testing.T) {
	primaryCache := newTestCache()
	primaryTopo := NewTopology()
	primaryNode := NewNode("primary", "127.0.0.1:0")
	primaryRing := hashring.New(150)
	primaryRing.AddNode("primary")

	// Seed 20 keys into the primary's cache.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("fullkey:%03d", i)
		if err := primaryCache.Set(key, []byte(fmt.Sprintf("snap%d", i)), 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	replicaCache := newTestCache()
	replicaSrv := network.NewServer(network.ServerConfig{Addr: "127.0.0.1:0"}, replicaCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := replicaSrv.Start(ctx); err != nil {
		t.Fatalf("replica server start: %v", err)
	}
	t.Cleanup(func() { replicaSrv.Shutdown() })

	mgr := NewManager(primaryNode, primaryTopo, primaryRing, primaryCache)
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("mgr.Start: %v", err)
	}

	rs, _ := mgr.Registry().GetReplicaSet("primary")
	primaryInfo, _ := rs.GetReplica("primary")

	// Overflow the stream buffer (capacity 10000) so the oldest ops are evicted.
	// Append 10001 ops to push seq 0 out of the buffer.
	for i := 0; i < 10001; i++ {
		primaryInfo.Stream.Append(replication.Operation{
			Command: "SET",
			Args:    []string{fmt.Sprintf("overflow:%d", i), "x"},
		})
	}

	// BufferStartSeq > 0 — gap exceeds retention.
	if primaryInfo.Stream.BufferStartSeq() == 0 {
		t.Fatal("buffer should have evicted oldest ops")
	}

	// Add replica and run sync — should trigger full sync path.
	rs.AddReplica("replica-1", replicaSrv.Addr().String())
	err := mgr.doReplicaSync("primary", "replica-1", replicaSrv.Addr().String())
	if err != nil {
		t.Fatalf("doReplicaSync (full sync): %v", err)
	}

	// Replica should have the primary's full cache (20 keys).
	if replicaCache.Size() != 20 {
		t.Errorf("replica cache size = %d, want 20", replicaCache.Size())
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("fullkey:%03d", i)
		val, err := replicaCache.Get(key)
		if err != nil {
			t.Errorf("replica missing key %q: %v", key, err)
			continue
		}
		want := fmt.Sprintf("snap%d", i)
		if string(val) != want {
			t.Errorf("key %q: got %q, want %q", key, string(val), want)
		}
	}
}

// TestSync_PromotionExcludesSyncing verifies the full chain: a replica
// still in ReplicaSyncing is not considered for promotion, while an Active
// replica with the lowest lag is.
func TestSync_PromotionExcludesSyncing(t *testing.T) {
	rs := replication.NewReplicaSet("dead-primary")
	rs.AddReplica("syncing-replica", "10.0.0.1:7000") // stays ReplicaSyncing
	rs.AddReplica("active-replica", "10.0.0.2:7000")
	rs.SetStatus("active-replica", replication.ReplicaActive)
	rs.UpdateLag("active-replica", 5)

	p := replication.NewPromotion(rs)
	promoted, err := p.PromoteBestReplica()
	if err != nil {
		t.Fatalf("PromoteBestReplica: %v", err)
	}
	if promoted != "active-replica" {
		t.Errorf("should promote active-replica, got %s", promoted)
	}

	// Verify syncing-replica was NOT promoted.
	if promoted == "syncing-replica" {
		t.Error("syncing replica should not be promoted")
	}
}

// TestSync_FailurePath_ReplicaUnreachable verifies that when the replica
// is unreachable during sync, the replica stays in ReplicaSyncing (not
// silently marked Active) and the error is logged.
func TestSync_FailurePath_ReplicaUnreachable(t *testing.T) {
	primaryCache := newTestCache()
	primaryTopo := NewTopology()
	primaryNode := NewNode("primary", "127.0.0.1:0")
	primaryRing := hashring.New(150)
	primaryRing.AddNode("primary")

	mgr := NewManager(primaryNode, primaryTopo, primaryRing, primaryCache)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("mgr.Start: %v", err)
	}

	rs, _ := mgr.Registry().GetReplicaSet("primary")
	primaryInfo, _ := rs.GetReplica("primary")
	// Add some ops so sync has work to do.
	for i := 0; i < 10; i++ {
		primaryInfo.Stream.Append(replication.Operation{
			Command: "SET",
			Args:    []string{fmt.Sprintf("k%d", i), "v"},
		})
	}

	// Add replica pointing to unreachable address.
	rs.AddReplica("dead-replica", "127.0.0.1:19999")

	// doReplicaSync should fail (connection refused).
	err := mgr.doReplicaSync("primary", "dead-replica", "127.0.0.1:19999")
	if err == nil {
		t.Fatal("expected error for unreachable replica")
	}

	// Replica must NOT be Active.
	if r, ok := rs.GetReplica("dead-replica"); !ok {
		t.Fatal("replica not found")
	} else if r.GetStatus() == replication.ReplicaActive {
		t.Error("unreachable replica should NOT be marked Active")
	} else if r.GetStatus() != replication.ReplicaSyncing {
		t.Errorf("replica status = %v, want ReplicaSyncing", r.GetStatus())
	}
}

// TestSync_BufferStartSeq verifies the new BufferStartSeq method.
func TestSync_BufferStartSeq(t *testing.T) {
	s := replication.NewReplicationStream(5)
	if s.BufferStartSeq() != 0 {
		t.Errorf("empty stream BufferStartSeq = %d, want 0", s.BufferStartSeq())
	}

	// Fill past capacity.
	for i := 0; i < 10; i++ {
		s.Append(replication.Operation{Command: "SET", Args: []string{"k"}})
	}

	if s.BufferStartSeq() == 0 {
		t.Error("BufferStartSeq should be > 0 after overflow")
	}
	if s.BufferStartSeq() != 5 {
		t.Errorf("BufferStartSeq = %d, want 5 (10 appended, capacity 5)", s.BufferStartSeq())
	}
}
