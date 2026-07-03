package cache

import (
	"sync"
)

type Store struct {
	mu      sync.RWMutex
	entries map[string]*Entry
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
	s.entries[entry.Key] = entry
}

func (s *Store) SetNX(entry *Entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[entry.Key]; exists {
		return false
	}
	s.entries[entry.Key] = entry
	return true
}

func (s *Store) SetXX(entry *Entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.entries[entry.Key]; !exists {
		return false
	}
	s.entries[entry.Key] = entry
	return true
}

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, existed := s.entries[key]
	delete(s.entries, key)
	return existed
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

func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[string]*Entry)
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
