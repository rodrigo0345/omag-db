package schema

import (
	"fmt"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage"
)

type TableManager struct {
	schemas map[string]*TableSchema
	mu      sync.RWMutex
}

func NewTableManager() *TableManager {
	return &TableManager{
		schemas: make(map[string]*TableSchema),
	}
}

func (tm *TableManager) CreateTable(schema ITableSchema, errorOnExists bool) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	name := schema.GetName()
	if _, exists := tm.schemas[name]; exists {
		if errorOnExists {
			return fmt.Errorf("table %s already exists", name)
		}
		return nil
	}

	tableSchema, ok := schema.(*TableSchema)
	if !ok {
		// If it's not a TableSchema, create a new one from the interface
		tableSchema = NewTableSchema(
			schema.GetName(),
			schema.GetPrimaryKey(),
			schema.GetColumns(),
		)
		for indexName, columns := range schema.GetIndexes() {
			tableSchema.AddIndex(indexName, columns)
		}
	}

	tm.schemas[name] = tableSchema
	return nil
}

func (tm *TableManager) DropTable(tableName string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.schemas[tableName]; !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	delete(tm.schemas, tableName)
	return nil
}

func (tm *TableManager) CreateIndex(tableName string, index IndexDefinition, indexBackend storage.IStorageEngine) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if len(index.Columns) == 0 {
		return fmt.Errorf("index columns cannot be empty")
	}

	schema, exists := tm.schemas[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	if _, indexExists := schema.Indexes[index.Name]; indexExists {
		return fmt.Errorf("index %s already exists on table %s", index.Name, tableName)
	}

	schema.AddIndex(index.Name, index.Columns)
	schema.SetIndexedStorage(index.Columns, indexBackend)

	return nil
}

func (tm *TableManager) GetTableSchema(tableName string) (ITableSchema, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	schema, exists := tm.schemas[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	return schema, nil
}
