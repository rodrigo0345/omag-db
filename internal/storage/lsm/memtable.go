package lsm

type MemTable struct {
	data map[string][]byte
}

func NewMemTable() *MemTable {
	return &MemTable{
		data: make(map[string][]byte),
	}
}

func (m *MemTable) Put(key []byte, value []byte) {
	m.data[string(key)] = value
}

func (m *MemTable) Get(key []byte) ([]byte, bool) {
	val, ok := m.data[string(key)]
	return val, ok
}

// FlushMemTable converts the MemTable into an immutable SSTable and initializes its Bloom filter.
func FlushMemTable(id uint64, m *MemTable, falsePositiveRate float64) *SSTable {
	bf := NewBloomFilter(uint(len(m.data)), falsePositiveRate)

	// Create SSTable mock data and populate the Bloom filter
	dataCopy := make(map[string][]byte)
	for k, v := range m.data {
		bf.Add([]byte(k))
		dataCopy[k] = v
	}

	return NewSSTable(id, dataCopy, bf)
}
