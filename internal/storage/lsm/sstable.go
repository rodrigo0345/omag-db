package lsm

import "fmt"

type SSTable struct {
	id          uint64
	data        map[string][]byte // Simulating on-disk storage
	bloomFilter *BloomFilter
	// More metadata like minKey, maxKey, offsets etc would go here in reality
}

func NewSSTable(id uint64, data map[string][]byte, bf *BloomFilter) *SSTable {
	return &SSTable{
		id:          id,
		data:        data,
		bloomFilter: bf,
	}
}

// Get checks the Bloom filter first, and if present, performs a disk look-up (mocked).
func (ss *SSTable) Get(key []byte) ([]byte, error) {
	// First check the bloom filter.
	// We save an expensive I/O operation if we know the key is certainly not here.
	if !ss.bloomFilter.Test(key) {
		return nil, fmt.Errorf("Key %q not found in SSTable %d (Bloom Filter rejected)", string(key), ss.id)
	}

	// Reached only if Bloom filter returned true.
	// We might have a false positive from the Bloom Filter so the key might not be there.
	val, ok := ss.data[string(key)]
	if !ok {
		return nil, fmt.Errorf("Key %q not found in SSTable %d (Bloom filter false positive)", string(key), ss.id)
	}
	return val, nil
}
