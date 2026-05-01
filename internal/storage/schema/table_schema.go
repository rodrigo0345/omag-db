package schema

import (
	"encoding/json"

	"github.com/rodrigo0345/omag/internal/storage"
)

type TableSchema struct {
	Name                  string
	PrimaryKey            []string
	Columns               map[string]string
	Indexes               map[string][]string
	IndexedStorageEngines map[string]storage.IStorageEngine
}

var _ ITableSchema = (*TableSchema)(nil)

func NewTableSchema(name string, primaryKey []string, columns map[string]string) *TableSchema {
	return &TableSchema{
		Name:                  name,
		PrimaryKey:            primaryKey,
		Columns:               columns,
		Indexes:               make(map[string][]string),
		IndexedStorageEngines: make(map[string]storage.IStorageEngine),
	}
}

func (ts *TableSchema) GetName() string {
	return ts.Name
}

func (ts *TableSchema) GetPrimaryKey() []string {
	return ts.PrimaryKey
}

func (ts *TableSchema) GetColumns() map[string]string {
	return ts.Columns
}

func (ts *TableSchema) GetIndexes() map[string][]string {
	return ts.Indexes
}

func (ts *TableSchema) GetIndexedStorage(columns []string) storage.IStorageEngine {
	// hash all columns to create a unique key for the index storage
	hash := hashColumns(columns)
	if engine, exists := ts.IndexedStorageEngines[hash]; exists {
		return engine
	}
	return nil
}

func (ts *TableSchema) SetIndexedStorage(columns []string, engine storage.IStorageEngine) {
	hash := hashColumns(columns)
	ts.IndexedStorageEngines[hash] = engine
}

// when a write occurs, all storages need to be updated
func (ts *TableSchema) GetAllStorages() []storage.IStorageEngine {
	storages := make([]storage.IStorageEngine, 0, len(ts.IndexedStorageEngines))
	for _, engine := range ts.IndexedStorageEngines {
		storages = append(storages, engine)
	}
	return storages
}

func (ts *TableSchema) AddIndex(indexName string, columns []string) {
	ts.Indexes[indexName] = columns
}

func (ts *TableSchema) ToJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":       ts.Name,
		"primaryKey": ts.PrimaryKey,
		"columns":    ts.Columns,
		"indexes":    ts.Indexes,
	})
}

func hashColumns(columns []string) string {
	// TODO: make this faster
	hash := ""
	for _, col := range columns {
		hash += col + "|"
	}
	return hash
}
