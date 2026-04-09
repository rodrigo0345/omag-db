package lsm

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type LSMTreeBackend struct {
	logManager    log.ILogManager
	bufferManager buffer.IBufferPoolManager

	memtable *MemTable
	sstables []*SSTable

	ssTableIdCounter uint64 // For mock assigning IDs
}

func NewLSMTreeBackend(log log.ILogManager, buf buffer.IBufferPoolManager) *LSMTreeBackend {
	return &LSMTreeBackend{
		logManager:       log,
		bufferManager:    buf,
		memtable:         NewMemTable(),
		sstables:         make([]*SSTable, 0),
		ssTableIdCounter: 1,
	}
}

func (l *LSMTreeBackend) Get(key []byte) ([]byte, error) {
	// 1. Check current MemTable first.
	if val, ok := l.memtable.Get(key); ok {
		return val, nil
	}

	// 2. Read through SSTables (from newest to oldest).
	// The Bloom filter avoids an expensive lookup on an SSTable that definitively doesn't have the key.
	for i := len(l.sstables) - 1; i >= 0; i-- {
		sstable := l.sstables[i]
		val, err := sstable.Get(key)
		if err == nil {
			return val, nil // Found
		}
		// If err != nil (either Bloom filter rejected or false positive), continue checking older tables
	}

	return nil, fmt.Errorf("Key %q not found in LSM tree", string(key))
}

func (l *LSMTreeBackend) Put(key []byte, value []byte) error {
	// Write to MemTable.
	// If it reached a certain limit, we'd flush it to an SSTable. Let's mock a flush threshold.
	l.memtable.Put(key, value)

	// Simplified mock flushing logic:
	// Let's say we flush if size > 100 on every Put.
	if len(l.memtable.data) >= 100 {
		l.flush()
	}
	return nil
}

func (l *LSMTreeBackend) flush() {
	if len(l.memtable.data) == 0 {
		return
	}
	// Create SSTable with a Bloom Filter (e.g., 1% false positive rate target)
	sstable := FlushMemTable(l.ssTableIdCounter, l.memtable, 0.01)

	// Prepend to array (or append). Newest tables should be queried first!
	l.sstables = append(l.sstables, sstable)
	l.ssTableIdCounter++

	// Reset the MemTable
	l.memtable = NewMemTable()
}

func (l *LSMTreeBackend) Delete(key []byte) error {
	return nil
}
