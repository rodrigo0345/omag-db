package storage

import "github.com/rodrigo0345/omag/internal/storage/schema"

// ITableStorageEngine extends IStorageEngine with table and index support
type ITableStorageEngine interface {
	IStorageEngine

	// Table operations
	CreateTable(tableSchema *schema.TableSchema) error
	DropTable(tableName string) error
	GetTableSchema(tableName string) (*schema.TableSchema, error)

	// Table-specific operations
	TablePut(tableName string, key []byte, value []byte) error
	TableGet(tableName string, key []byte) ([]byte, error)
	TableDelete(tableName string, key []byte) error

	// Index operations
	CreateIndex(tableName string, indexName string, indexType schema.IndexType, columns []string) error
	DropIndex(tableName string, indexName string) error

	// Index-based queries
	IndexGet(tableName string, indexName string, key []byte) ([][]byte, error) // Returns all matching row primary keys
	IndexScan(tableName string, indexName string, startKey []byte, endKey []byte) ([][]byte, error)

	// Secondary index maintenance (called by transaction manager)
	// When a row is inserted/updated/deleted, these are called to maintain indexes
	UpdateSecondaryIndexes(tableName string, operation string, oldRow map[string][]byte, newRow map[string][]byte) error
}

// TableStorageOptions provides configuration for table storage
type TableStorageOptions struct {
	// Enable automatic index maintenance
	AutoIndexUpdate bool

	// Maximum number of secondary indexes per table
	MaxIndexesPerTable int
}
