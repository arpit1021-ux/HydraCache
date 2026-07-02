package cache

import (
	"math"
	"sync"
)

type BloomFilter struct {
	mu        sync.RWMutex
	bits      []bool
	size      uint
	hashFuncs int
	count     uint
}

func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	size := optimalSize(expectedItems, falsePositiveRate)
	hashFuncs := optimalHashFuncs(size, expectedItems)
	return &BloomFilter{
		bits:      make([]bool, size),
		size:      size,
		hashFuncs: hashFuncs,
	}
}

func optimalSize(n int, p float64) uint {
	return uint(math.Ceil(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2))))
}

func optimalHashFuncs(m uint, n int) int {
	return int(math.Ceil(float64(m) / float64(n) * math.Log(2)))
}

func (bf *BloomFilter) hash(data string, seed uint) uint {
	var h uint = seed
	for i := 0; i < len(data); i++ {
		h = h*31 + uint(data[i])
	}
	return h % bf.size
}

func (bf *BloomFilter) Add(key string) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := 0; i < bf.hashFuncs; i++ {
		idx := bf.hash(key, uint(i+1))
		bf.bits[idx] = true
	}
	bf.count++
}

func (bf *BloomFilter) Contains(key string) bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	for i := 0; i < bf.hashFuncs; i++ {
		idx := bf.hash(key, uint(i+1))
		if !bf.bits[idx] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Reset() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	bf.bits = make([]bool, bf.size)
	bf.count = 0
}

func (bf *BloomFilter) Count() uint {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.count
}
