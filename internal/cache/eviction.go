package cache

import (
	"container/list"
	"sync"
)

type EvictionPolicy int

const (
	EvictionLRU EvictionPolicy = iota
	EvictionLFU
)

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

type LFU struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*lfuEntry
	minFreq  int
}

type lfuEntry struct {
	key   string
	value *Entry
	freq  int
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
		entry.freq++
		return entry.value, true
	}
	return nil, false
}

func (l *LFU) Put(entry *Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.items[entry.Key]; ok {
		existing.freq++
		existing.value = entry
		return
	}
	if len(l.items) >= l.capacity {
		l.evict()
	}
	l.items[entry.Key] = &lfuEntry{
		key:   entry.Key,
		value: entry,
		freq:  1,
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
	var minKey string
	minFreq := l.minFreq + 1
	for k, v := range l.items {
		if v.freq < minFreq {
			minFreq = v.freq
			minKey = k
		}
	}
	if minKey != "" {
		delete(l.items, minKey)
	}
}
