package schema

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
)

// SecondaryIndexEntry represents an entry in a secondary index
// Maps index key -> list of primary keys
type SecondaryIndexEntry struct {
	IndexKey    []byte   // The indexed column value(s)
	PrimaryKeys [][]byte // All primary keys that have this index key value
}

// SecondaryIndexManager manages secondary indexes for a table
// It uses the storage backend to persist index data
type SecondaryIndexManager struct {
	tableName      string
	tableSchema    *TableSchema
	storageBackend interface {
		Put(key []byte, value []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	}

	// In-memory cache of index entries (for performance)
	indexCache map[string]map[string][]byte // indexName -> (indexKey -> primaryKey)
	cacheSize  int
}

// NewSecondaryIndexManager creates a new secondary index manager for a table
func NewSecondaryIndexManager(
	tableName string,
	tableSchema *TableSchema,
	storageBackend interface {
		Put(key []byte, value []byte) error
		Get(key []byte) ([]byte, error)
		Delete(key []byte) error
	},
) *SecondaryIndexManager {
	return &SecondaryIndexManager{
		tableName:      tableName,
		tableSchema:    tableSchema,
		storageBackend: storageBackend,
		indexCache:     make(map[string]map[string][]byte),
	}
}

// IsSecondaryIndexKey checks if a key belongs to a secondary index
func (sim *SecondaryIndexManager) IsSecondaryIndexKey(key []byte) bool {
	prefix := sim.getIndexKeyPrefix("")
	return bytes.HasPrefix(key, []byte(prefix))
}

// AddToIndex adds or updates an entry in a secondary index
func (sim *SecondaryIndexManager) AddToIndex(indexName string, indexValue []byte, primaryKey []byte) error {
	// Verify index exists
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return err
	}

	key := sim.getIndexKey(indexName, indexValue)

	// Try to get existing entries
	existingData, _ := sim.storageBackend.Get(key)

	// Decode or create new entry
	var entry *SecondaryIndexEntry
	if existingData != nil {
		var err error
		entry, err = DecodeSecondaryIndexEntry(existingData)
		if err != nil {
			return fmt.Errorf("failed to decode index entry: %w", err)
		}
	} else {
		entry = &SecondaryIndexEntry{
			IndexKey:    indexValue,
			PrimaryKeys: make([][]byte, 0),
		}
	}

	// Check if primary key already exists
	for _, pk := range entry.PrimaryKeys {
		if bytes.Equal(pk, primaryKey) {
			return nil // Already exists
		}
	}

	// Add primary key
	entry.PrimaryKeys = append(entry.PrimaryKeys, primaryKey)

	// Persist
	encoded, err := EncodeSecondaryIndexEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to encode index entry: %w", err)
	}

	return sim.storageBackend.Put(key, encoded)
}

// RemoveFromIndex removes an entry from a secondary index
func (sim *SecondaryIndexManager) RemoveFromIndex(indexName string, indexValue []byte, primaryKey []byte) error {
	// Verify index exists
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return err
	}

	key := sim.getIndexKey(indexName, indexValue)

	// Get existing entries
	existingData, err := sim.storageBackend.Get(key)
	if err != nil {
		return nil // Entry doesn't exist
	}

	entry, err := DecodeSecondaryIndexEntry(existingData)
	if err != nil {
		return fmt.Errorf("failed to decode index entry: %w", err)
	}

	// Remove primary key
	newPrimaryKeys := make([][]byte, 0)
	for _, pk := range entry.PrimaryKeys {
		if !bytes.Equal(pk, primaryKey) {
			newPrimaryKeys = append(newPrimaryKeys, pk)
		}
	}

	if len(newPrimaryKeys) == 0 {
		// Delete the entire entry
		return sim.storageBackend.Delete(key)
	}

	// Update with remaining keys
	entry.PrimaryKeys = newPrimaryKeys
	encoded, err := EncodeSecondaryIndexEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to encode index entry: %w", err)
	}

	return sim.storageBackend.Put(key, encoded)
}

// GetPrimaryKeysForIndexValue retrieves all primary keys for a given index value
func (sim *SecondaryIndexManager) GetPrimaryKeysForIndexValue(indexName string, indexValue []byte) ([][]byte, error) {
	// Verify index exists
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	key := sim.getIndexKey(indexName, indexValue)

	// Try to get from storage
	existingData, err := sim.storageBackend.Get(key)
	if err != nil || existingData == nil {
		return [][]byte{}, nil // No entries found
	}

	entry, err := DecodeSecondaryIndexEntry(existingData)
	if err != nil {
		return nil, fmt.Errorf("failed to decode index entry: %w", err)
	}

	return entry.PrimaryKeys, nil
}

// RangeQueryIndex performs a range query on a secondary index
func (sim *SecondaryIndexManager) RangeQueryIndex(
	indexName string,
	startValue []byte,
	endValue []byte,
	inclusive bool,
) ([][]byte, error) {
	// Verify index exists
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	result := make([][]byte, 0)

	// This is a simplified implementation
	// A full implementation would need an iterator interface on the storage backend
	// For now, we require an exact key lookup

	if bytes.Equal(startValue, endValue) && inclusive {
		// Single value lookup
		return sim.GetPrimaryKeysForIndexValue(indexName, startValue)
	}

	return result, fmt.Errorf("range queries require storage engine iterator support")
}

// ClearIndex removes all entries from a secondary index
func (sim *SecondaryIndexManager) ClearIndex(indexName string) error {
	// Verify index exists
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return err
	}

	// This requires an iterator interface on the storage backend
	// For now, return an error
	return fmt.Errorf("clear index requires storage engine iterator support")
}

// getIndexKey returns the storage key for an index entry
func (sim *SecondaryIndexManager) getIndexKey(indexName string, indexValue []byte) []byte {
	prefix := sim.getIndexKeyPrefix(indexName)
	return append([]byte(prefix), indexValue...)
}

// getIndexKeyPrefix returns the prefix for all entries in an index
func (sim *SecondaryIndexManager) getIndexKeyPrefix(indexName string) string {
	return fmt.Sprintf("__index:%s:%s:", sim.tableName, indexName)
}

// EncodeSecondaryIndexEntry encodes an index entry to bytes
func EncodeSecondaryIndexEntry(entry *SecondaryIndexEntry) ([]byte, error) {
	var buf bytes.Buffer

	// Write number of primary keys
	numKeys := uint32(len(entry.PrimaryKeys))
	if err := binary.Write(&buf, binary.BigEndian, numKeys); err != nil {
		return nil, err
	}

	// Write each primary key
	for _, pk := range entry.PrimaryKeys {
		// Write length of primary key
		pkLen := uint32(len(pk))
		if err := binary.Write(&buf, binary.BigEndian, pkLen); err != nil {
			return nil, err
		}

		// Write primary key data
		if _, err := buf.Write(pk); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// DecodeSecondaryIndexEntry decodes bytes to an index entry
func DecodeSecondaryIndexEntry(data []byte) (*SecondaryIndexEntry, error) {
	buf := bytes.NewReader(data)

	// Read number of primary keys
	var numKeys uint32
	if err := binary.Read(buf, binary.BigEndian, &numKeys); err != nil {
		return nil, err
	}

	primaryKeys := make([][]byte, numKeys)

	// Read each primary key
	for i := uint32(0); i < numKeys; i++ {
		var pkLen uint32
		if err := binary.Read(buf, binary.BigEndian, &pkLen); err != nil {
			return nil, err
		}

		pk := make([]byte, pkLen)
		if _, err := buf.Read(pk); err != nil {
			return nil, err
		}

		primaryKeys[i] = pk
	}

	return &SecondaryIndexEntry{
		PrimaryKeys: primaryKeys,
	}, nil
}

// ValidateIndex checks that an index is properly maintained
func (sim *SecondaryIndexManager) ValidateIndex(indexName string) error {
	// This would require iterating through all data rows and checking index consistency
	// For now, return not implemented
	return fmt.Errorf("index validation requires storage engine iterator support")
}

// RebuildIndex rebuilds a secondary index from scratch
func (sim *SecondaryIndexManager) RebuildIndex(indexName string) error {
	// This would require:
	// 1. Get all data rows from the table
	// 2. For each row, extract the indexed columns
	// 3. Add entries to the index
	// Requires iterator support on the storage backend
	return fmt.Errorf("index rebuild requires storage engine iterator support")
}

// CompactIndex compacts index data to improve query performance
func (sim *SecondaryIndexManager) CompactIndex(indexName string) error {
	// This is an optimization that consolidates index entries
	// For now, not implemented
	return fmt.Errorf("index compaction requires storage engine iterator support")
}

// GetIndexStats returns statistics about an index
type IndexStats struct {
	IndexName  string
	TableName  string
	NumEntries int64
	AvgKeySize int64
	SizeBytes  int64
}

func (sim *SecondaryIndexManager) GetIndexStats(indexName string) (*IndexStats, error) {
	// Verify index exists
	_, err := sim.tableSchema.GetIndex(indexName)
	if err != nil {
		return nil, err
	}

	// This requires an iterator interface to scan all index entries
	return nil, fmt.Errorf("index statistics require storage engine iterator support")
}

// GetAllIndexNames returns all secondary indexes for the table
func (sim *SecondaryIndexManager) GetAllIndexNames() []string {
	var names []string
	for indexName, idx := range sim.tableSchema.Indexes {
		if idx.Type != IndexTypePrimary {
			names = append(names, indexName)
		}
	}
	sort.Strings(names)
	return names
}
