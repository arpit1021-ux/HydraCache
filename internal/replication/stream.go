package replication

import (
	"sync"
	"sync/atomic"
)

type Operation struct {
	Seq     int64
	Command string
	Args    []string
	NodeID  string
}

type ReplicationStream struct {
	mu       sync.RWMutex
	buffer   []Operation
	capacity int
	seq      int64
	start    int64
}

func NewReplicationStream(capacity int) *ReplicationStream {
	return &ReplicationStream{
		buffer:   make([]Operation, 0, capacity),
		capacity: capacity,
	}
}

func (rs *ReplicationStream) Append(op Operation) int64 {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	seq := atomic.AddInt64(&rs.seq, 1)
	op.Seq = seq

	if len(rs.buffer) >= rs.capacity {
		rs.buffer = rs.buffer[1:]
		rs.start++
	}

	rs.buffer = append(rs.buffer, op)
	return seq
}

func (rs *ReplicationStream) GetSince(seq int64) []Operation {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	if seq >= rs.seq {
		return nil
	}

	startIdx := int(seq - rs.start)
	if startIdx < 0 {
		startIdx = 0
	}

	result := make([]Operation, len(rs.buffer)-startIdx)
	copy(result, rs.buffer[startIdx:])
	return result
}

func (rs *ReplicationStream) LatestSeq() int64 {
	return atomic.LoadInt64(&rs.seq)
}

func (rs *ReplicationStream) Size() int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.buffer)
}

func (rs *ReplicationStream) Clear() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.buffer = rs.buffer[:0]
	rs.start = rs.seq
}

// BufferStartSeq returns the sequence number of the oldest operation
// still retained in the ring buffer. If the caller's lastKnownSeq is
// below this value, the gap exceeds the buffer and a full sync is needed.
func (rs *ReplicationStream) BufferStartSeq() int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.start
}
