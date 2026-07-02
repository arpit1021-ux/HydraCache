package replication

import (
	"sync"
	"time"
)

type LagSample struct {
	Timestamp time.Time
	Lag       int64
}

type LagTracker struct {
	mu       sync.RWMutex
	samples  map[string][]LagSample
	maxSamples int
}

func NewLagTracker() *LagTracker {
	return &LagTracker{
		samples:    make(map[string][]LagSample),
		maxSamples: 1000,
	}
}

func (lt *LagTracker) Record(nodeID string, lag int64) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	lt.samples[nodeID] = append(lt.samples[nodeID], LagSample{
		Timestamp: time.Now(),
		Lag:       lag,
	})

	if len(lt.samples[nodeID]) > lt.maxSamples {
		lt.samples[nodeID] = lt.samples[nodeID][1:]
	}
}

func (lt *LagTracker) AverageLag(nodeID string) int64 {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	samples := lt.samples[nodeID]
	if len(samples) == 0 {
		return 0
	}

	var total int64
	for _, s := range samples {
		total += s.Lag
	}
	return total / int64(len(samples))
}

func (lt *LagTracker) MaxLag(nodeID string) int64 {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	samples := lt.samples[nodeID]
	if len(samples) == 0 {
		return 0
	}

	var max int64
	for _, s := range samples {
		if s.Lag > max {
			max = s.Lag
		}
	}
	return max
}

func (lt *LagTracker) RecentLags(nodeID string, count int) []LagSample {
	lt.mu.RLock()
	defer lt.mu.RUnlock()

	samples := lt.samples[nodeID]
	if len(samples) == 0 {
		return nil
	}

	start := len(samples) - count
	if start < 0 {
		start = 0
	}
	result := make([]LagSample, len(samples)-start)
	copy(result, samples[start:])
	return result
}
