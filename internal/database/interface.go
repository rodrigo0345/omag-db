package database

import (
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
)

// Database describes the small, opinionated entry point for the preferred
// MVCC + LSM-tree configuration.
type Database interface {
	Close() error
	StorageEngine() storage.IStorageEngine
	TableStorageEngine(tableName string) storage.IStorageEngine
	IsolationManager() txn.IIsolationManager
	SchemaManager() *schema.SchemaManager
	RollbackManager() *rollback.RollbackManager
	Scan(lower []byte, upper []byte) ([]storage.ScanEntry, error)

	BeginTransaction(isolationLevel uint8, tableName string, tableSchema *schema.TableSchema) int64
	Read(txnID int64, key []byte) ([]byte, error)
	Write(txnID int64, key []byte, value []byte) error
	Delete(txnID int64, key []byte) error
	Commit(txnID int64) error
	Abort(txnID int64) error

	CreateTable(tableSchema *schema.TableSchema) error
	DropTable(tableName string) error
	GetTableSchema(tableName string) (*schema.TableSchema, error)
	CreateIndex(tableName string, indexName string, indexType schema.IndexType, columns []string, isUnique bool) error
	DropIndex(tableName string, indexName string) error
	GetIndexManager(tableName string) (*schema.SecondaryIndexManager, bool)
}
