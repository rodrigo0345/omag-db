package txn

import (
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/schema"
)

// the implementation is in internal/isolation, but it was defined here to avoid circular dependencies
type IIsolationManager interface {
	// Transaction Lifecycle
	BeginTransaction(isolationLevel uint8) int64
	Commit(txnID int64) error
	Abort(txnID int64) error

	// Data Operations (DML) - target is used instead of tableName
	// because I assume the user already contacted TableManager to get the storage engine for the table, given a certain query
	Read(txnID int64, target storage.IStorageEngine, key []byte) ([]byte, error)
	Write(txnID int64, target storage.IStorageEngine, key []byte, value []byte) error
	Delete(txnID int64, target storage.IStorageEngine, key []byte) error
	Scan(txnID int64, target storage.IStorageEngine, opts storage.ScanOptions) (storage.ICursor, error)

	// Schema Operations (DDL)
	CreateTable(txnID int64, schema schema.ITableSchema) error
	UpdateTable(txnID int64, schema schema.ITableSchema) error
	DropTable(txnID int64, tableName string) error
	CreateIndex(txnID int64, tableName string, index schema.IndexDefinition) error
	DropIndex(txnID int64, tableName string, indexName string) error

	Close() error
}
