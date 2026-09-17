package election

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PersistedState is the durable term/vote record. It must be written to
// stable storage before a node grants a vote or starts a new election —
// otherwise a crash-and-restart can forget a vote already cast and grant
// a second, conflicting one in the same term.
type PersistedState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// TermStore durably persists the current term and vote.
type TermStore interface {
	Load() (PersistedState, error)
	Save(PersistedState) error
}

// FileTermStore persists term/vote as a single JSON file, written via
// write-temp-then-rename so a crash mid-write can never leave a torn
// record: the rename is atomic on the same filesystem, so a reader always
// sees either the previous complete file or the new complete file.
type FileTermStore struct {
	mu   sync.Mutex
	path string
}

func NewFileTermStore(dir string) (*FileTermStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("election: create state dir %s: %w", dir, err)
	}
	return &FileTermStore{path: filepath.Join(dir, "election-state.json")}, nil
}

func (s *FileTermStore) Load() (PersistedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return PersistedState{}, nil
		}
		return PersistedState{}, fmt.Errorf("election: read state file %s: %w", s.path, err)
	}
	var st PersistedState
	if err := json.Unmarshal(data, &st); err != nil {
		return PersistedState{}, fmt.Errorf("election: corrupt state file %s: %w", s.path, err)
	}
	return st, nil
}

func (s *FileTermStore) Save(st PersistedState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("election: marshal state: %w", err)
	}

	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("election: open temp state file %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("election: write temp state file %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("election: fsync temp state file %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("election: close temp state file %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("election: rename %s to %s: %w", tmp, s.path, err)
	}
	return nil
}

// MemoryTermStore is a non-durable TermStore for tests that don't exercise
// crash/restart behavior. It is NOT safe to use in production — a process
// restart silently loses the vote record, which is exactly the bug this
// package exists to prevent.
type MemoryTermStore struct {
	mu    sync.Mutex
	state PersistedState
}

func NewMemoryTermStore() *MemoryTermStore { return &MemoryTermStore{} }

func (s *MemoryTermStore) Load() (PersistedState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func (s *MemoryTermStore) Save(st PersistedState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
	return nil
}
