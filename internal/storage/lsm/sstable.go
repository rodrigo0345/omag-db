package lsm

import "fmt"

type Tombstone struct {
	Deleted bool
}

type SSTable struct {
	id          uint64
	data        map[string][]byte
	tombstones  map[string]bool
	bloomFilter *BloomFilter
}

func NewSSTable(id uint64, data map[string][]byte, bf *BloomFilter) *SSTable {
	return &SSTable{
		id:          id,
		data:        data,
		tombstones:  make(map[string]bool),
		bloomFilter: bf,
	}
}

func (ss *SSTable) Get(key []byte) ([]byte, error) {
	keyStr := string(key)
	if ss.tombstones[keyStr] {
		return nil, ErrKeyTombstoned
	}

	if !ss.bloomFilter.Test(key) {
		return nil, fmt.Errorf("Key %q not found in SSTable %d (Bloom Filter rejected)", keyStr, ss.id)
	}

	val, ok := ss.data[keyStr]
	if !ok {
		return nil, fmt.Errorf("Key %q not found in SSTable %d (Bloom filter false positive)", keyStr, ss.id)
	}
	return val, nil
}

func (ss *SSTable) IsTombstoned(key []byte) bool {
	return ss.tombstones[string(key)]
}
