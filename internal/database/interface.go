package database

import (
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/schema"
)

type txnid = uint64
type Iterator interface {
	Next() bool
	Entry() storage.ScanEntry
	Error() error
	Close() error
}

// Database describes the small, opinionated entry point for the preferred
// MVCC + LSM-tree configuration.
type Database interface {
	Close() error

	// transaction management
	BeginTransaction(isolationLevel uint8) txnid
	Commit(txnID txnid) error
	Abort(txnID txnid) error

	Read(txnID txnid, tableName string, key []byte) ([]byte, error)
	Scan(txnID txnid, tableName string, options storage.ScanOptions) (Iterator, error)
	Write(txnID txnid, tableName string, key []byte, value []byte) error
	Delete(txnID txnid, tableName string, key []byte) error

	// tables
	CreateTable(tableSchema *schema.TableSchema) error
	DropTable(tableName string) error
	GetTableSchema(tableName string) (*schema.TableSchema, error)

	// indexes
	CreateIndex(tableName string, indexName string, indexType schema.IndexType, columns []string) error
	DropIndex(tableName string, indexName string) error

	// exclusively distributed operations
	Partition(tableName string, partitionKey string) error
	Replicate(tableName string, targetNodeID string) error
}
