package txn

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type MVCCWriteHandler struct {
	storageEngine storage.IStorageEngine
	logManager    log.ILogManager
	bufferManager buffer.IBufferPoolManager
}

func NewMVCCWriteHandler(
	storage storage.IStorageEngine,
	bufferMgr buffer.IBufferPoolManager,
	logMgr log.ILogManager,
) *MVCCWriteHandler {
	return &MVCCWriteHandler{
		storageEngine: storage,
		logManager:    logMgr,
		bufferManager: bufferMgr,
	}
}

func (mh *MVCCWriteHandler) HandleWrite(txn *Transaction, writeOp WriteOperation) error {
	if mh.logManager != nil {
		beforeImage, _ := mh.storageEngine.Get(writeOp.Key)
		walRecord := log.WALRecord{
			TxnID:  txn.GetID(),
			Type:   log.UPDATE,
			PageID: writeOp.PageID,
			Before: beforeImage,
			After:  writeOp.Value,
		}
		if _, err := mh.logManager.AppendLogRecord(walRecord); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if writeOp.IsDelete {
		if err := mh.storageEngine.Delete(writeOp.Key); err != nil {
			return fmt.Errorf("storage delete failed: %w", err)
		}
	} else {
		if err := mh.storageEngine.Put(writeOp.Key, writeOp.Value); err != nil {
			return fmt.Errorf("storage put failed: %w", err)
		}
	}

	return nil
}
