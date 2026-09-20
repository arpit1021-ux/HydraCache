package cache

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
)

type EvictionPolicy int

const (
	EvictionLRU EvictionPolicy = iota
	EvictionLFU
)

// EvictionPolicyFromString converts a config string to an EvictionPolicy.
// An unrecognized string is a hard error rather than a silent fallback —
// matching persistence.SyncModeFromString's precedent in this codebase:
// a config typo should fail startup loudly, not silently pick a
// different policy than the operator asked for.
func EvictionPolicyFromString(s string) (EvictionPolicy, error) {
	switch strings.ToLower(s) {
	case "lru":
		return EvictionLRU, nil
	case "lfu":
		return EvictionLFU, nil
	default:
		return EvictionLRU, fmt.Errorf("unknown cache eviction_policy %q: must be one of lru, lfu", s)
	}
}

type LRU struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List
}

type lruEntry struct {
	key   string
	value *Entry
}

func NewLRU(capacity int) *LRU {
	return &LRU{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
}

func (l *LRU) Get(key string) (*Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[key]; ok {
		l.order.MoveToFront(el)
		return el.Value.(*lruEntry).value, true
	}
	return nil, false
}

func (l *LRU) Put(entry *Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[entry.Key]; ok {
		l.order.MoveToFront(el)
		el.Value.(*lruEntry).value = entry
		return
	}
	if l.order.Len() >= l.capacity {
		l.evict()
	}
	el := l.order.PushFront(&lruEntry{key: entry.Key, value: entry})
	l.items[entry.Key] = el
}

func (l *LRU) Remove(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.items[key]; ok {
		l.order.Remove(el)
		delete(l.items, key)
		return true
	}
	return false
}

func (l *LRU) evict() {
	el := l.order.Back()
	if el == nil {
		return
	}
	l.order.Remove(el)
	delete(l.items, el.Value.(*lruEntry).key)
}

// RemoveOldest evicts and returns the least-recently-used key, or ("",
// false) if the tracker is empty. Used to drive eviction externally
// (e.g. by a real byte-budget check) rather than only Put's own
// capacity threshold.
func (l *LRU) RemoveOldest() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	el := l.order.Back()
	if el == nil {
		return "", false
	}
	key := el.Value.(*lruEntry).key
	l.order.Remove(el)
	delete(l.items, key)
	return key, true
}

// Reset clears all tracked entries (e.g. on FLUSHALL).
func (l *LRU) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items = make(map[string]*list.Element)
	l.order.Init()
}

// LFU evicts the least-frequently-used entry. Finding it is O(n) per
// eviction (a linear scan for the minimum frequency) rather than O(1) —
// a real LFU with O(1) eviction needs frequency-bucketed doubly-linked
// lists, which is meaningfully more code for a benefit that only matters
// at eviction rates far higher than a cache's write rate would ever
// produce; not implemented here.
type LFU struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*lfuEntry
	minFreq  int
	clock    int64 // monotonically increasing touch counter, for tie-breaking
}

type lfuEntry struct {
	key   string
	value *Entry
	freq  int
	// seq is the clock value as of this entry's last touch (Put or Get).
	// Frequency alone doesn't produce a deterministic victim when several
	// entries are tied — e.g. every newly-inserted key starts at freq=1,
	// same as any once-touched key that's never been touched again.
	// Without a tiebreaker, findMinLocked's map iteration picks whichever
	// tied entry happens to come first in Go's randomized map order,
	// meaning eviction could just as easily throw out the key that was
	// inserted a moment ago as the one that's sat cold the longest — a
	// real correctness gap (silently unpredictable evictions), not just
	// a testing inconvenience. Breaking ties by lowest seq (stalest
	// first) makes eviction deterministic and matches how a real cache
	// operator would expect LFU to behave under ties.
	seq int64
}

func NewLFU(capacity int) *LFU {
	return &LFU{
		capacity: capacity,
		items:    make(map[string]*lfuEntry),
	}
}

func (l *LFU) Get(key string) (*Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if entry, ok := l.items[key]; ok {
		l.clock++
		entry.freq++
		entry.seq = l.clock
		return entry.value, true
	}
	return nil, false
}

func (l *LFU) Put(entry *Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.clock++
	if existing, ok := l.items[entry.Key]; ok {
		existing.freq++
		existing.value = entry
		existing.seq = l.clock
		return
	}
	if len(l.items) >= l.capacity {
		l.evict()
	}
	l.items[entry.Key] = &lfuEntry{
		key:   entry.Key,
		value: entry,
		freq:  1,
		seq:   l.clock,
	}
	l.minFreq = 1
}

func (l *LFU) Remove(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.items[key]; ok {
		delete(l.items, key)
		return true
	}
	return false
}

func (l *LFU) evict() {
	minKey, minFreq, found := l.findMinLocked()
	if found {
		delete(l.items, minKey)
		l.minFreq = minFreq
	}
}

// findMinLocked scans for the lowest-frequency entry, breaking ties by
// lowest seq (the stalest of the tied entries). Caller holds l.mu. O(n)
// per eviction — see the type doc for why this isn't O(1).
func (l *LFU) findMinLocked() (key string, freq int, found bool) {
	var seq int64
	first := true
	for k, v := range l.items {
		if first || v.freq < freq || (v.freq == freq && v.seq < seq) {
			key, freq, seq, found = k, v.freq, v.seq, true
			first = false
		}
	}
	return key, freq, found
}

// RemoveOldest evicts and returns the least-frequently-used key, or ("",
// false) if the tracker is empty. Used to drive eviction externally
// (e.g. by a real byte-budget check) rather than only Put's own
// capacity threshold.
func (l *LFU) RemoveOldest() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	minKey, minFreq, found := l.findMinLocked()
	if !found {
		return "", false
	}
	delete(l.items, minKey)
	l.minFreq = minFreq
	return minKey, true
}

// Reset clears all tracked entries (e.g. on FLUSHALL).
func (l *LFU) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.items = make(map[string]*lfuEntry)
	l.minFreq = 0
	l.clock = 0
}
