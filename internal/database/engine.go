package database

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/rodrigo0345/omag/internal/concurrency"
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/lsm"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/isolation"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

const (
	DefaultBufferPoolSize   = 50
	DefaultReplacerCapacity = 128
)

// Options configures the default MVCC + LSM engine.
type Options struct {
	DBPath           string
	LSMDataDir       string
	WALPath          string
	BufferPoolSize   int
	ReplacerCapacity int
}

// Engine provides a small, opinionated database entry point.
// It prefers MVCC transaction handling with an LSM-tree storage backend.
type Engine struct {
	storageEngine  storage.IStorageEngine
	isolationMgr   txn.IIsolationManager
	bufferPool     buffer.IBufferPoolManager
	diskMgr        *buffer.DiskManager
	walMgr         log.ILogManager
	schemaManager  *schema.SchemaManager
	indexManagers  map[string]*schema.SecondaryIndexManager
	rollbackMgr    *rollback.RollbackManager
	mu             sync.RWMutex
}

var _ Database = (*Engine)(nil)

// OpenMVCCLSM opens a database engine using MVCC and an LSM-tree backend.
func OpenMVCCLSM(opts Options) (*Engine, error) {
	if opts.DBPath == "" {
		opts.DBPath = filepath.Join(".", "test.db")
	}
	if opts.LSMDataDir == "" {
		opts.LSMDataDir = filepath.Join(".", "lsm_data")
	}
	if opts.WALPath == "" {
		opts.WALPath = filepath.Join(".", "test.wal")
	}
	if opts.BufferPoolSize <= 0 {
		opts.BufferPoolSize = DefaultBufferPoolSize
	}
	if opts.ReplacerCapacity <= 0 {
		opts.ReplacerCapacity = DefaultReplacerCapacity
	}

	diskMgr, err := buffer.NewDiskManager(opts.DBPath)
	if err != nil {
		return nil, fmt.Errorf("open disk manager: %w", err)
	}

	replacer := concurrency.NewClockReplacer(opts.ReplacerCapacity)
	bufferPool := buffer.NewBufferPoolManagerWithReplacer(opts.BufferPoolSize, diskMgr, replacer)

	walMgr, err := log.NewWALManager(opts.WALPath)
	if err != nil {
		return nil, fmt.Errorf("open WAL manager: %w", err)
	}

	storageEngine := lsm.NewLSMTreeBackendWithDataDir(walMgr, bufferPool, opts.LSMDataDir)
	rollbackMgr := rollback.NewRollbackManager(bufferPool)
	writeHandler := write_handler.NewDefaultWriteHandler(storageEngine, rollbackMgr, bufferPool, walMgr)

	indexManagers := make(map[string]*schema.SecondaryIndexManager)
	isolationMgr := isolation.NewMVCCManager(
		walMgr,
		bufferPool,
		writeHandler,
		rollbackMgr,
		storageEngine,
		indexManagers,
	)

	return &Engine{
		storageEngine: storageEngine,
		isolationMgr:  isolationMgr,
		bufferPool:    bufferPool,
		diskMgr:       diskMgr,
		walMgr:        walMgr,
		schemaManager: schema.NewSchemaManager(storageEngine),
		indexManagers: indexManagers,
		rollbackMgr:   rollbackMgr,
	}, nil
}

func (e *Engine) Close() error {
	if e == nil {
		return nil
	}

	var errs []error
	if closer, ok := e.storageEngine.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if e.walMgr != nil {
		if err := e.walMgr.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if closer, ok := e.bufferPool.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e *Engine) StorageEngine() storage.IStorageEngine {
	return e.storageEngine
}

func (e *Engine) BufferPoolManager() buffer.IBufferPoolManager {
	return e.bufferPool
}

func (e *Engine) DiskManager() *buffer.DiskManager {
	return e.diskMgr
}

func (e *Engine) WALManager() log.ILogManager {
	return e.walMgr
}

func (e *Engine) IsolationManager() txn.IIsolationManager {
	return e.isolationMgr
}

func (e *Engine) SchemaManager() *schema.SchemaManager {
	return e.schemaManager
}

func (e *Engine) RollbackManager() *rollback.RollbackManager {
	return e.rollbackMgr
}

func (e *Engine) BeginTransaction(isolationLevel uint8, tableName string, tableSchema *schema.TableSchema) int64 {
	return e.isolationMgr.BeginTransaction(isolationLevel, tableName, tableSchema)
}

func (e *Engine) Read(txnID int64, key []byte) ([]byte, error) {
	return e.isolationMgr.Read(txnID, key)
}

func (e *Engine) Write(txnID int64, key []byte, value []byte) error {
	return e.isolationMgr.Write(txnID, key, value)
}

func (e *Engine) Delete(txnID int64, key []byte) error {
	return e.isolationMgr.Delete(txnID, key)
}

func (e *Engine) Commit(txnID int64) error {
	return e.isolationMgr.Commit(txnID)
}

func (e *Engine) Abort(txnID int64) error {
	return e.isolationMgr.Abort(txnID)
}

func (e *Engine) CreateTable(tableSchema *schema.TableSchema) error {
	if tableSchema == nil {
		return fmt.Errorf("table schema is nil")
	}
	if err := e.schemaManager.CreateTable(tableSchema); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.indexManagers[tableSchema.Name] = schema.NewSecondaryIndexManager(tableSchema.Name, tableSchema, e.storageEngine)
	return nil
}

func (e *Engine) DropTable(tableName string) error {
	if err := e.schemaManager.DropTable(tableName); err != nil {
		return err
	}

	e.mu.Lock()
	delete(e.indexManagers, tableName)
	e.mu.Unlock()
	return nil
}

func (e *Engine) GetTableSchema(tableName string) (*schema.TableSchema, error) {
	return e.schemaManager.GetTable(tableName)
}

func (e *Engine) CreateIndex(tableName string, indexName string, indexType schema.IndexType, columns []string, isUnique bool) error {
	if err := e.schemaManager.AddIndex(tableName, indexName, indexType, columns, isUnique); err != nil {
		return err
	}

	tableSchema, err := e.schemaManager.GetTable(tableName)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.indexManagers[tableName] = schema.NewSecondaryIndexManager(tableName, tableSchema, e.storageEngine)
	e.mu.Unlock()
	return nil
}

func (e *Engine) DropIndex(tableName string, indexName string) error {
	if err := e.schemaManager.RemoveIndex(tableName, indexName); err != nil {
		return err
	}

	tableSchema, err := e.schemaManager.GetTable(tableName)
	if err != nil {
		return err
	}

	e.mu.Lock()
	e.indexManagers[tableName] = schema.NewSecondaryIndexManager(tableName, tableSchema, e.storageEngine)
	e.mu.Unlock()
	return nil
}

func (e *Engine) GetIndexManager(tableName string) (*schema.SecondaryIndexManager, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	mgr, ok := e.indexManagers[tableName]
	return mgr, ok
}
