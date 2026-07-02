package lock

import (
	"fmt"
	"sync"
	"time"
)

type DistributedLock struct {
	mu    sync.RWMutex
	locks map[string]*LockEntry
}

type LockEntry struct {
	Key       string
	Owner     string
	ExpiresAt time.Time
	Value     string
}

func NewDistributedLock() *DistributedLock {
	return &DistributedLock{
		locks: make(map[string]*LockEntry),
	}
}

func (dl *DistributedLock) Acquire(key, owner string, ttl time.Duration) (bool, error) {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	if entry, ok := dl.locks[key]; ok {
		if entry.ExpiresAt.After(time.Now()) {
			if entry.Owner == owner {
				entry.ExpiresAt = time.Now().Add(ttl)
				return true, nil
			}
			return false, nil
		}
		delete(dl.locks, key)
	}

	dl.locks[key] = &LockEntry{
		Key:       key,
		Owner:     owner,
		ExpiresAt: time.Now().Add(ttl),
	}
	return true, nil
}

func (dl *DistributedLock) Release(key, owner string) bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	entry, ok := dl.locks[key]
	if !ok {
		return false
	}

	if entry.Owner != owner {
		return false
	}

	delete(dl.locks, key)
	return true
}

func (dl *DistributedLock) IsLocked(key string) bool {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	entry, ok := dl.locks[key]
	if !ok {
		return false
	}
	return entry.ExpiresAt.After(time.Now())
}

func (dl *DistributedLock) GetOwner(key string) (string, error) {
	dl.mu.RLock()
	defer dl.mu.RUnlock()

	entry, ok := dl.locks[key]
	if !ok {
		return "", fmt.Errorf("lock not found")
	}
	if entry.ExpiresAt.Before(time.Now()) {
		return "", fmt.Errorf("lock expired")
	}
	return entry.Owner, nil
}

func (dl *DistributedLock) Cleanup() int {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	count := 0
	now := time.Now()
	for key, entry := range dl.locks {
		if entry.ExpiresAt.Before(now) {
			delete(dl.locks, key)
			count++
		}
	}
	return count
}

func (dl *DistributedLock) LockCount() int {
	dl.mu.RLock()
	defer dl.mu.RUnlock()
	count := 0
	for _, entry := range dl.locks {
		if entry.ExpiresAt.After(time.Now()) {
			count++
		}
	}
	return count
}
