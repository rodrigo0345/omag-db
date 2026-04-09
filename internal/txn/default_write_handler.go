package txn

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type DefaultWriteHandler struct {
	storageEngine   storage.IStorageEngine
	rollbackManager *RollbackManager
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
}

func NewDefaultWriteHandler(
	storage storage.IStorageEngine,
	rollbackMgr *RollbackManager,
	bufferMgr buffer.IBufferPoolManager,
	logMgr log.ILogManager,
) *DefaultWriteHandler {
	return &DefaultWriteHandler{
		storageEngine:   storage,
		rollbackManager: rollbackMgr,
		bufferManager:   bufferMgr,
		logManager:      logMgr,
	}
}

func (dh *DefaultWriteHandler) HandleWrite(txn *Transaction, writeOp WriteOperation) error {
	var beforeImage []byte
	var err error

	if !writeOp.IsDelete {
		beforeImage, err = dh.storageEngine.Get(writeOp.Key)
		if err != nil {
			beforeImage = nil
		}
	} else {
		beforeImage, err = dh.storageEngine.Get(writeOp.Key)
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

	if writeOp.IsDelete {
		if err := dh.storageEngine.Delete(writeOp.Key); err != nil {
			return fmt.Errorf("storage delete failed: %w", err)
		}
	} else {
		if err := dh.storageEngine.Put(writeOp.Key, writeOp.Value); err != nil {
			return fmt.Errorf("storage put failed: %w", err)
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
