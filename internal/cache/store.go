package cache

import (
	"bytes"
	"sync"
	"sync/atomic"
)

type Store struct {
	mu       sync.RWMutex
	entries  map[string]*Entry
	memBytes atomic.Int64
}

func NewStore() *Store {
	return &Store{
		entries: make(map[string]*Entry),
	}
}

func (s *Store) Get(key string) (*Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.entries[key]
	return entry, ok
}

func (s *Store) Set(entry *Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, exists := s.entries[entry.Key]; exists {
		s.memBytes.Add(-old.Size)
	}
	s.entries[entry.Key] = entry
	s.memBytes.Add(entry.Size)
}

func (s *Store) SetNX(entry *Entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[entry.Key]; exists {
		return false
	}
	s.entries[entry.Key] = entry
	s.memBytes.Add(entry.Size)
	return true
}

func (s *Store) SetXX(entry *Entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.entries[entry.Key]
	if !exists {
		return false
	}
	s.entries[entry.Key] = entry
	s.memBytes.Add(entry.Size - old.Size)
	return true
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, existed := s.entries[key]
	if existed {
		s.memBytes.Add(-old.Size)
		delete(s.entries, key)
	}
	return existed
}

// CompareAndDelete atomically deletes key only if its current value
// equals expected, under a single lock acquisition — unlike a separate
// Get-then-Delete, no other goroutine can write a new value in between.
// Returns false (no-op) if the key is absent or its value differs.
func (s *Store) CompareAndDelete(key string, expected []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[key]
	if !ok || !bytes.Equal(entry.Value, expected) {
		return false
	}
	s.memBytes.Add(-entry.Size)
	delete(s.entries, key)
	return true
}

func (s *Store) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.entries[key]
	return ok
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.entries))
	for k := range s.entries {
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// MemoryBytes returns the running total of estimated bytes held by every
// entry currently in the store (see estimatedSize). Updated incrementally
// on every mutation rather than recomputed by scanning, so it stays cheap
// regardless of store size.
func (s *Store) MemoryBytes() int64 {
	return s.memBytes.Load()
}

func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]*Entry)
	s.memBytes.Store(0)
}

func (s *Store) Range(fn func(key string, entry *Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, v := range s.entries {
		if !fn(k, v) {
			break
		}
	}
}

// Snapshot returns a shallow copy of the entries map under a single RLock.
// The map itself is new, but *Entry pointers are shared with the store.
// This is safe because Entry's only post-creation mutable field
// (ExpiresAt) is an atomic.Int64 — reads and writes to it cannot tear.
func (s *Store) Snapshot() map[string]*Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := make(map[string]*Entry, len(s.entries))
	for k, v := range s.entries {
		snap[k] = v
	}
	return snap
}
