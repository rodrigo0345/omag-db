package write_handler

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

type DefaultWriteHandler struct {
	storageEngine   storage.IStorageEngine
	storageResolver func(tableName string) storage.IStorageEngine
	rollbackManager *rollback.RollbackManager
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	indexManager    *schema.SecondaryIndexManager
	tableSchema     *schema.TableSchema
	replicationIntentEnabled bool
}

func NewDefaultWriteHandler(
	storage storage.IStorageEngine,
	rollbackMgr *rollback.RollbackManager,
	bufferMgr buffer.IBufferPoolManager,
	logMgr log.ILogManager,
) *DefaultWriteHandler {
	return &DefaultWriteHandler{
		storageEngine:   storage,
		rollbackManager: rollbackMgr,
		bufferManager:   bufferMgr,
		logManager:      logMgr,
		replicationIntentEnabled: true,
	}
}

func (dh *DefaultWriteHandler) SetReplicationIntentEnabled(enabled bool) {
	dh.replicationIntentEnabled = enabled
}

func (dh *DefaultWriteHandler) SetStorageResolver(resolver func(tableName string) storage.IStorageEngine) {
	dh.storageResolver = resolver
}

func (dh *DefaultWriteHandler) resolveStorageEngine(tableName string) storage.IStorageEngine {
	if dh.storageResolver != nil {
		if engine := dh.storageResolver(tableName); engine != nil {
			return engine
		}
	}
	return dh.storageEngine
}

func (dh *DefaultWriteHandler) HandleWrite(txn *txn_unit.Transaction, writeOp WriteOperation) error {
	storageEngine := dh.resolveStorageEngine(writeOp.TableName)
	if storageEngine == nil {
		return fmt.Errorf("storage engine is nil")
	}

	var beforeImage []byte
	if writeOp.IsDelete {
		var err error
		beforeImage, err = storageEngine.Get(writeOp.Key)
		if err != nil {
			return fmt.Errorf("failed to get value before delete: %w", err)
		}
	}

	if dh.logManager != nil {
		walRecord := &log.WALRecord{
			TxnID:  txn.GetID(),
			Type:   log.UPDATE,
			PageID: writeOp.PageID,
			Before: beforeImage,
			After:  writeOp.Value,
		}
		if _, err := dh.logManager.AppendLogRecord(walRecord); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if writeOp.IsDelete && dh.indexManager != nil && dh.tableSchema != nil && beforeImage != nil {
		indexValues, err := schema.ExtractIndexValues(dh.tableSchema, beforeImage)
		if err != nil {
			return fmt.Errorf("failed to extract index values before delete: %w", err)
		}

		for indexName, indexValue := range indexValues {
			if err := dh.indexManager.RemoveFromIndex(indexName, indexValue, writeOp.PrimaryKey); err != nil {
				return fmt.Errorf("failed to remove from index %q: %w", indexName, err)
			}

			capturedIndexName := indexName
			capturedIndexValue := indexValue
			capturedPrimaryKey := writeOp.PrimaryKey
			dh.rollbackManager.RegisterIndexCleanup(txn, func() error {
				return dh.indexManager.AddToIndex(capturedIndexName, capturedIndexValue, capturedPrimaryKey)
			})
		}
	}

	if writeOp.IsDelete {
		if err := storageEngine.Delete(writeOp.Key); err != nil {
			return fmt.Errorf("storage delete failed: %w", err)
		}
		// Record deletion operation for crash recovery
		txn.RecordRecoveryOperation(writeOp.TableName, log.DELETE, writeOp.Key, nil)
		if dh.logManager != nil {
			dh.logManager.AddTransactionOperation(txn.GetID(), writeOp.TableName, log.DELETE, writeOp.Key, nil)
			if dh.replicationIntentEnabled {
				if intentLogger, ok := dh.logManager.(log.ReplicationIntentLogger); ok {
					intentLogger.LogReplicationIntent(txn.GetID(), writeOp.TableName, log.DELETE, writeOp.Key, nil)
				}
			}
		}
	} else {
		if err := storageEngine.Put(writeOp.Key, writeOp.Value); err != nil {
			return fmt.Errorf("storage put failed: %w", err)
		}
		// Record put operation for crash recovery
		txn.RecordRecoveryOperation(writeOp.TableName, log.PUT, writeOp.Key, writeOp.Value)
		if dh.logManager != nil {
			dh.logManager.AddTransactionOperation(txn.GetID(), writeOp.TableName, log.PUT, writeOp.Key, writeOp.Value)
			if dh.replicationIntentEnabled {
				if intentLogger, ok := dh.logManager.(log.ReplicationIntentLogger); ok {
					intentLogger.LogReplicationIntent(txn.GetID(), writeOp.TableName, log.PUT, writeOp.Key, writeOp.Value)
				}
			}
		}

		if dh.indexManager != nil && dh.tableSchema != nil {
			indexValues, err := schema.ExtractIndexValues(dh.tableSchema, writeOp.Value)
			if err != nil {
				return fmt.Errorf("failed to extract index values: %w", err)
			}

			for indexName, indexValue := range indexValues {
				if err := dh.indexManager.AddToIndex(indexName, indexValue, writeOp.PrimaryKey); err != nil {
					return fmt.Errorf("failed to add to index %q: %w", indexName, err)
				}

				capturedIndexName := indexName
				capturedIndexValue := indexValue
				capturedPrimaryKey := writeOp.PrimaryKey
				dh.rollbackManager.RegisterIndexCleanup(txn, func() error {
					return dh.indexManager.RemoveFromIndex(capturedIndexName, capturedIndexValue, capturedPrimaryKey)
				})
			}
		}
	}

	if _, err := dh.rollbackManager.RecordPageWrite(
		txn,
		writeOp.PageID,
		writeOp.Offset,
		beforeImage,
	); err != nil {
		return fmt.Errorf("failed to record undo operation: %w", err)
	}

	return nil
}

func (dh *DefaultWriteHandler) GetStorageEngine() storage.IStorageEngine {
	return dh.storageEngine
}

func (dh *DefaultWriteHandler) SetIndexContext(tableSchema *schema.TableSchema, indexMgr *schema.SecondaryIndexManager) error {
	dh.tableSchema = tableSchema
	dh.indexManager = indexMgr
	return nil
}
