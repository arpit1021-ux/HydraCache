package cache

import "time"

type Entry struct {
	Key         string
	Value       []byte
	Flags       int64
	ExpiresAt   int64
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
	return &Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
		CreatedAt: now,
	}
}

func (e *Entry) IsExpired() bool {
	if e.ExpiresAt == 0 {
		return false
	}
	return time.Now().UnixNano() >= e.ExpiresAt
}

func (e *Entry) TTL() time.Duration {
	if e.ExpiresAt == 0 {
		return -1
	}
	remaining := e.ExpiresAt - time.Now().UnixNano()
	if remaining <= 0 {
		return 0
	}
	return time.Duration(remaining)
}
