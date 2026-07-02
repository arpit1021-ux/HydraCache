package persistence

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type SnapshotData struct {
	Entries   map[string]SnapshotEntry `json:"entries"`
	Seq       int64                     `json:"seq"`
	Timestamp time.Time                `json:"timestamp"`
	NodeID    string                    `json:"node_id"`
}

type SnapshotEntry struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	ExpiresAt int64  `json:"expires_at"`
	CreatedAt int64  `json:"created_at"`
}

type Snapshotter struct {
	mu       sync.RWMutex
	dir      string
	interval time.Duration
	nodeID   string
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewSnapshotter(dir string, interval time.Duration, nodeID string) (*Snapshotter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}
	return &Snapshotter{
		dir:      dir,
		interval: interval,
		nodeID:   nodeID,
		stopCh:   make(chan struct{}),
	}, nil
}

func (s *Snapshotter) Save(data SnapshotData) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data.Timestamp = time.Now()
	data.NodeID = s.nodeID

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	tmpPath := filepath.Join(s.dir, "snapshot.tmp")
	finalPath := filepath.Join(s.dir, "snapshot.json")

	if err := os.WriteFile(tmpPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("failed to finalize snapshot: %w", err)
	}

	log.Printf("[snapshot] saved snapshot: %d entries, %d bytes", len(data.Entries), len(jsonData))
	return nil
}

func (s *Snapshotter) Load() (*SnapshotData, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshotPath := filepath.Join(s.dir, "snapshot.json")
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot SnapshotData
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	log.Printf("[snapshot] loaded snapshot: %d entries from %v", len(snapshot.Entries), snapshot.Timestamp)
	return &snapshot, nil
}

func (s *Snapshotter) Start(saveFn func() SnapshotData) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.stopCh:
				return
			case <-ticker.C:
				if err := s.Save(saveFn()); err != nil {
					log.Printf("[snapshot] error: %v", err)
				}
			}
		}
	}()
}

func (s *Snapshotter) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *Snapshotter) LatestSnapshotTime() time.Time {
	info, err := os.Stat(filepath.Join(s.dir, "snapshot.json"))
	if err != nil {
		return time.Time{}
	}
	return info.ModTime()
}
