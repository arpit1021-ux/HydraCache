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
