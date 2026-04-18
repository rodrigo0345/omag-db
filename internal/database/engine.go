package database

import (
	"context"
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
	"github.com/rodrigo0345/omag/internal/txn/recovery"
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
	storageEngine storage.IStorageEngine
	lsmDataDir    string
	isolationMgr  txn.IIsolationManager
	bufferPool    buffer.IBufferPoolManager
	diskMgr       *buffer.DiskManager
	walMgr        log.ILogManager
	schemaManager *schema.SchemaManager
	indexManagers map[string]*schema.SecondaryIndexManager
	tableEngines  map[string]storage.IStorageEngine
	rollbackMgr   *rollback.RollbackManager
	mu            sync.RWMutex
}

var _ Database = (*Engine)(nil)

// OpenMVCCLSM opens a database engine using MVCC and an LSM-tree backend.
func OpenMVCCLSM(opts Options) (_ *Engine, err error) {
	if opts.DBPath == "" {
		opts.DBPath = filepath.Join("/var/data/inesdb/", "test.db")
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
	defer func() {
		if bufferPool != nil {
			_ = bufferPool.Close()
		}
	}()

	walMgr, err := log.NewWALManager(opts.WALPath)
	if err != nil {
		err = fmt.Errorf("open WAL manager: %w", err)
		return nil, err
	}

	storageEngine := lsm.NewLSMTreeBackendWithDataDir(walMgr, bufferPool, opts.LSMDataDir)
	rollbackMgr := rollback.NewRollbackManager(bufferPool)
	writeHandler := write_handler.NewDefaultWriteHandler(storageEngine, rollbackMgr, bufferPool, walMgr)

	indexManagers := make(map[string]*schema.SecondaryIndexManager)
	tableEngines := make(map[string]storage.IStorageEngine)
	isolationMgr := isolation.NewMVCCManager(
		walMgr,
		bufferPool,
		writeHandler,
		rollbackMgr,
		storageEngine,
		indexManagers,
	)

	engine := &Engine{
		storageEngine: storageEngine,
		lsmDataDir:    opts.LSMDataDir,
		isolationMgr:  isolationMgr,
		bufferPool:    bufferPool,
		diskMgr:       diskMgr,
		walMgr:        walMgr,
		schemaManager: schema.NewSchemaManager(storageEngine),
		indexManagers: indexManagers,
		tableEngines:  tableEngines,
		rollbackMgr:   rollbackMgr,
	}

	if tableNames, err := engine.schemaManager.LoadAllSchemas(); err != nil {
		return nil, err
	} else {
		for _, tableName := range tableNames {
			if err := engine.restoreTableBackend(tableName); err != nil {
				return nil, err
			}
		}
	}

	recoveryCoordinator := recovery.NewDefaultRecoveryCoordinator(
		walMgr,
		storageEngine,
		func(tableName string) storage.IStorageEngine {
			if tableName == "" {
				return storageEngine
			}
			return engine.tableStorageEngine(tableName)
		},
		bufferPool,
		rollbackMgr,
	)
	recoveryState, err := recoveryCoordinator.RecoverFromCrash(context.Background())
	if err != nil {
		return nil, fmt.Errorf("startup recovery failed: %w", err)
	}
	isolationMgr.EnsureMinNextTxnID(recoveryState.MaxTxnID)

	writeHandler.SetStorageResolver(func(tableName string) storage.IStorageEngine {
		if tableName == "" {
			return storageEngine
		}
		return engine.tableStorageEngine(tableName)
	})
	isolationMgr.SetStorageResolver(func(tableName string) storage.IStorageEngine {
		if tableName == "" {
			return storageEngine
		}
		return engine.tableStorageEngine(tableName)
	})
	bufferPool = nil

	return engine, nil
}

func (e *Engine) restoreTableBackend(tableName string) error {
	tableSchema, err := e.schemaManager.GetTable(tableName)
	if err != nil {
		return err
	}

	tableEngine := lsm.NewLSMTreeBackendWithDataDir(e.walMgr, e.bufferPool, filepath.Join(e.lsmDataDir, "tables", fmt.Sprintf("table_%s", tableName)))

	e.mu.Lock()
	defer e.mu.Unlock()
	e.tableEngines[tableName] = tableEngine
	e.indexManagers[tableName] = schema.NewSecondaryIndexManager(tableName, tableSchema, tableEngine)
	return nil
}

func (e *Engine) Close() error {
	if e == nil {
		return nil
	}

	var errs []error
	e.mu.RLock()
	tableEngines := make([]storage.IStorageEngine, 0, len(e.tableEngines))
	for _, engine := range e.tableEngines {
		tableEngines = append(tableEngines, engine)
	}
	e.mu.RUnlock()

	for _, engine := range tableEngines {
		if closer, ok := engine.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
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
	if e.bufferPool != nil {
		if err := e.bufferPool.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (e *Engine) StorageEngine() storage.IStorageEngine {
	return e.storageEngine
}

func (e *Engine) TableStorageEngine(tableName string) storage.IStorageEngine {
	return e.tableStorageEngine(tableName)
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

func (e *Engine) Scan(lower []byte, upper []byte) ([]storage.ScanEntry, error) {
	if e == nil || e.storageEngine == nil {
		return nil, fmt.Errorf("storage engine is nil")
	}

	e.mu.RLock()
	tableEngines := make([]storage.IStorageEngine, 0, len(e.tableEngines))
	for _, engine := range e.tableEngines {
		tableEngines = append(tableEngines, engine)
	}
	e.mu.RUnlock()

	if len(tableEngines) == 0 {
		return e.storageEngine.Scan(lower, upper)
	}

	results := make([]storage.ScanEntry, 0)
	for _, engine := range tableEngines {
		entries, err := engine.Scan(lower, upper)
		if err != nil {
			return nil, err
		}
		results = append(results, entries...)
	}

	return results, nil
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
	if e == nil {
		return fmt.Errorf("engine is nil")
	}

	tableEngine := lsm.NewLSMTreeBackendWithDataDir(e.walMgr, e.bufferPool, filepath.Join(e.lsmDataDir, "tables", fmt.Sprintf("table_%s", tableSchema.Name)))

	if err := e.schemaManager.CreateTable(tableSchema); err != nil {
		_ = tableEngine.Close()
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.tableEngines[tableSchema.Name] = tableEngine
	e.indexManagers[tableSchema.Name] = schema.NewSecondaryIndexManager(tableSchema.Name, tableSchema, tableEngine)
	return nil
}

func (e *Engine) DropTable(tableName string) error {
	if err := e.schemaManager.DropTable(tableName); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.indexManagers, tableName)
	if engine, ok := e.tableEngines[tableName]; ok {
		delete(e.tableEngines, tableName)
		if closer, ok := engine.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				return err
			}
		}
	}
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

	storageEngine := e.tableStorageEngine(tableName)
	e.mu.Lock()
	if storageEngine == nil {
		storageEngine = e.storageEngine
	}
	e.indexManagers[tableName] = schema.NewSecondaryIndexManager(tableName, tableSchema, storageEngine)
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

	storageEngine := e.tableStorageEngine(tableName)
	e.mu.Lock()
	if storageEngine == nil {
		storageEngine = e.storageEngine
	}
	e.indexManagers[tableName] = schema.NewSecondaryIndexManager(tableName, tableSchema, storageEngine)
	e.mu.Unlock()
	return nil
}

func (e *Engine) tableStorageEngine(tableName string) storage.IStorageEngine {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.tableEngines[tableName]
}

func (e *Engine) GetIndexManager(tableName string) (*schema.SecondaryIndexManager, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	mgr, ok := e.indexManagers[tableName]
	return mgr, ok
}
