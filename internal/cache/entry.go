package cache

import (
	"sync/atomic"
	"time"
)

type Entry struct {
	Key         string
	Value       []byte
	Flags       int64
	ExpiresAt   atomic.Int64
	CreatedAt   int64
	AccessCount int64
	Size        int64
}

// entryOverheadBytes estimates the fixed per-entry bookkeeping cost — the
// Entry struct itself, its slot in the store's map, and eviction-tracker
// bookkeeping — so that a memory bound accounts for more than just raw
// key/value bytes (otherwise "5 million 1-byte keys" would report as
// nearly free). This is a deliberate approximation, not an exact
// reflection of Go's runtime memory layout, which varies by GOARCH, map
// load factor, and allocator state — good enough to make a memory bound
// meaningful, not a promise of byte-exact RSS accounting.
const entryOverheadBytes = 64

func estimatedSize(key string, value []byte) int64 {
	return int64(len(key)) + int64(len(value)) + entryOverheadBytes
}

func NewEntry(key string, value []byte, ttl time.Duration) *Entry {
	now := time.Now().UnixNano()
	var expiresAt int64
	if ttl > 0 {
		expiresAt = now + int64(ttl)
	}
	e := &Entry{
		Key:       key,
		Value:     value,
		CreatedAt: now,
		Size:      estimatedSize(key, value),
	}
	e.ExpiresAt.Store(expiresAt)
	return e
}

// NewEntryWithTTL creates an entry with an explicit absolute ExpiresAt and
// CreatedAt. Used during WAL/snapshot recovery where timestamps are known
// from persisted state rather than the current wall clock.
func NewEntryWithTTL(key string, value []byte, expiresAt, createdAt int64) *Entry {
	e := &Entry{
		Key:       key,
		Value:     value,
		CreatedAt: createdAt,
		Size:      estimatedSize(key, value),
	}
	e.ExpiresAt.Store(expiresAt)
	return e
}

func (e *Entry) IsExpired() bool {
	expiresAt := e.ExpiresAt.Load()
	if expiresAt == 0 {
		return false
	}
	return time.Now().UnixNano() >= expiresAt
}

func (e *Entry) TTL() time.Duration {
	expiresAt := e.ExpiresAt.Load()
	if expiresAt == 0 {
		return -1
	}
	remaining := expiresAt - time.Now().UnixNano()
	if remaining <= 0 {
		return 0
	}
	return time.Duration(remaining)
}
