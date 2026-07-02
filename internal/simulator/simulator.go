package simulator

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

type Simulator struct {
	mu       sync.RWMutex
	nodes    map[string]*SimNode
	scenarios []Scenario
	active   bool
}

type SimNode struct {
	ID       string
	Address  string
	Running  bool
	Delay    time.Duration
	DropRate float64
}

type Scenario struct {
	Name        string
	Description string
	Action      func(s *Simulator) error
}

type FailureType int

const (
	FailureKill FailureType = iota
	FailureDelay
	FailureDrop
	FailureRestart
)

func New() *Simulator {
	return &Simulator{
		nodes:    make(map[string]*SimNode),
		scenarios: defaultScenarios(),
	}
}

func (s *Simulator) RegisterNode(id, address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[id] = &SimNode{
		ID:      id,
		Address: address,
		Running: true,
	}
}

func (s *Simulator) KillNode(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}
	node.Running = false
	log.Printf("[simulator] KILLED node %s", shortID(id))
	return nil
}

func (s *Simulator) RestartNode(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}
	node.Running = true
	node.Delay = 0
	node.DropRate = 0
	log.Printf("[simulator] RESTARTED node %s", shortID(id))
	return nil
}

func (s *Simulator) AddDelay(id string, delay time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}
	node.Delay = delay
	log.Printf("[simulator] Added %v delay to node %s", delay, shortID(id))
	return nil
}

func (s *Simulator) AddPacketDrop(id string, percent float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, ok := s.nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}
	node.DropRate = percent
	log.Printf("[simulator] Added %.0f%% packet drop to node %s", percent*100, shortID(id))
	return nil
}

func (s *Simulator) RandomFailure() error {
	s.mu.RLock()
	var running []string
	for id, node := range s.nodes {
		if node.Running {
			running = append(running, id)
		}
	}
	s.mu.RUnlock()

	if len(running) == 0 {
		return fmt.Errorf("no running nodes to fail")
	}

	target := running[rand.Intn(len(running))]
	failureType := FailureType(rand.Intn(3))

	switch failureType {
	case FailureKill:
		return s.KillNode(target)
	case FailureDelay:
		return s.AddDelay(target, time.Duration(500+rand.Intn(2000))*time.Millisecond)
	case FailureDrop:
		return s.AddPacketDrop(target, float64(rand.Intn(50))/100.0)
	}
	return nil
}

func (s *Simulator) Chaos(duration time.Duration, interval time.Duration) {
	go func() {
		s.active = true
		defer func() { s.active = false }()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		timeout := time.After(duration)

		for {
			select {
			case <-timeout:
				log.Printf("[simulator] chaos scenario complete")
				return
			case <-ticker.C:
				if err := s.RandomFailure(); err != nil {
					log.Printf("[simulator] chaos error: %v", err)
				}
			}
		}
	}()
}

func (s *Simulator) IsActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

func (s *Simulator) NodeStatus() map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := make(map[string]bool, len(s.nodes))
	for id, node := range s.nodes {
		status[id] = node.Running
	}
	return status
}

func (s *Simulator) Scenarios() []Scenario {
	return s.scenarios
}

func defaultScenarios() []Scenario {
	return []Scenario{
		{
			Name:        "single_node_failure",
			Description: "Kill a random node and verify recovery",
			Action: func(s *Simulator) error {
				return s.RandomFailure()
			},
		},
		{
			Name:        "network_partition",
			Description: "Simulate network partition between nodes",
			Action: func(s *Simulator) error {
				s.mu.RLock()
				var ids []string
				for id := range s.nodes {
					ids = append(ids, id)
				}
				s.mu.RUnlock()

				if len(ids) >= 2 {
					_ = s.AddDelay(ids[0], 5*time.Second)
					_ = s.AddPacketDrop(ids[1], 0.5)
				}
				return nil
			},
		},
		{
			Name:        "cascading_failure",
			Description: "Kill multiple nodes in sequence",
			Action: func(s *Simulator) error {
				go func() {
					s.mu.RLock()
					var ids []string
					for id, node := range s.nodes {
						if node.Running {
							ids = append(ids, id)
						}
					}
					s.mu.RUnlock()

					for i, id := range ids {
						if i >= len(ids)/2 {
							break
						}
						time.Sleep(500 * time.Millisecond)
						_ = s.KillNode(id)
					}
				}()
				return nil
			},
		},
	}
}
