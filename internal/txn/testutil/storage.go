package testutil

import (
	"fmt"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage"
)

type InMemoryStorageEngine struct {
	data map[string][]byte
	mu   sync.RWMutex
}

func NewInMemoryStorageEngine() storage.IStorageEngine {
	return &InMemoryStorageEngine{
		data: make(map[string][]byte),
	}
}

func (s *InMemoryStorageEngine) Put(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dataCopy := make([]byte, len(value))
	copy(dataCopy, value)
	s.data[string(key)] = dataCopy
	return nil
}

func (s *InMemoryStorageEngine) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, exists := s.data[string(key)]; exists {
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		return valCopy, nil
	}
	return nil, fmt.Errorf("key not found")
}

func (s *InMemoryStorageEngine) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(key))
	return nil
}

func (s *InMemoryStorageEngine) Scan() ([]storage.ScanEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]storage.ScanEntry, 0, len(s.data))
	for key, value := range s.data {
		result = append(result, storage.ScanEntry{
			Key:   []byte(key),
			Value: value,
		})
	}
	return result, nil
}
