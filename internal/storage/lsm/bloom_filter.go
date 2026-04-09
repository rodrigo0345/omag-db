package lsm

import (
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure that is used to
// test whether an element is a member of a set.
type BloomFilter struct {
	m      uint   // Number of bits in the filter
	k      uint   // Number of hash functions
	bitset []byte // The bit array
}

// NewBloomFilter creates a new BloomFilter given the expected number of items
// and the desired false positive rate.
func NewBloomFilter(expectedItems uint, falsePositiveRate float64) *BloomFilter {
	if expectedItems == 0 {
		expectedItems = 1 // Prevent division by zero
	}

	// Calculate optimal size of bit array (m) and optimal number of hash functions (k)
	m := uint(math.Ceil(float64(expectedItems) * math.Log(falsePositiveRate) / math.Log(1.0/math.Pow(2, math.Log(2)))))
	k := uint(math.Round((float64(m) / float64(expectedItems)) * math.Log(2)))

	// Ensure at least 1 hash function and at least 1 byte
	if k == 0 {
		k = 1
	}
	if m == 0 {
		m = 8
	}

	return &BloomFilter{
		m:      m,
		k:      k,
		bitset: make([]byte, (m+7)/8),
	}
}

// Add inserts an element into the Bloom filter.
func (bf *BloomFilter) Add(data []byte) {
	h1, h2 := hash(data)
	for i := uint(0); i < bf.k; i++ {
		bitIndex := (uint(h1) + i*uint(h2)) % bf.m
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8
		bf.bitset[byteIndex] |= (1 << bitOffset)
	}
}

// Test checks if an element is likely in the Bloom filter.
func (bf *BloomFilter) Test(data []byte) bool {
	h1, h2 := hash(data)
	for i := uint(0); i < bf.k; i++ {
		bitIndex := (uint(h1) + i*uint(h2)) % bf.m
		byteIndex := bitIndex / 8
		bitOffset := bitIndex % 8
		if bf.bitset[byteIndex]&(1<<bitOffset) == 0 {
			return false // Definitely not in the set
		}
	}
	return true // Probably in the set
}

// hash uses FNV-1a to generate two 32-bit hash values from the data.
// This allows us to simulate k hash functions using the formula: h_i(x) = h1(x) + i * h2(x).
func hash(data []byte) (uint32, uint32) {
	h := fnv.New64a()
	h.Write(data)
	val := h.Sum64()
	return uint32(val), uint32(val >> 32)
}
