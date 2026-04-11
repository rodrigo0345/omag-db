package isolation

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/log"
)

type TransactionID uint64

type TwoPhaseLockingManager struct {
	transactions    map[TransactionID]*txn.Transaction
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    txn.WriteHandler
	rollbackManager *txn.RollbackManager
	primaryIndex    storage.IStorageEngine
}

func NewTwoPhaseLockingManager(
	logManager log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler txn.WriteHandler,
	rollbackMgr *txn.RollbackManager,
	primaryIndex storage.IStorageEngine,
) *TwoPhaseLockingManager {
	return &TwoPhaseLockingManager{
		transactions:    make(map[TransactionID]*txn.Transaction),
		logManager:      logManager,
		bufferManager:   bufferMgr,
		writeHandler:    writeHandler,
		rollbackManager: rollbackMgr,
		primaryIndex:    primaryIndex,
	}
}

func (m *TwoPhaseLockingManager) BeginTransaction(isolationLevel uint8) int64 {
	// TODO: uuid7 generate key
	txnID := int64(len(m.transactions) + 1) // Simple ID generation for now
	txn := txn.NewTransaction(uint64(txnID), isolationLevel)
	m.transactions[TransactionID(txnID)] = txn
	return txnID
}

func (m *TwoPhaseLockingManager) Read(txnID int64, Key []byte) ([]byte, error) {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	transaction.AddSharedLock(Key)
	return m.primaryIndex.Get(Key)
}

func (m *TwoPhaseLockingManager) Write(txnID int64, Key []byte, Value []byte) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	transaction.AddExclusiveLock(Key)

	writeOp := txn.WriteOperation{
		Key:    Key,
		Value:  Value,
		PageID: 0,
		Offset: 0,
	}

	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *TwoPhaseLockingManager) Commit(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	transaction.Commit()

	if m.logManager != nil {
		rec := &log.WALRecord{
			TxnID: transaction.GetID(),
			Type:  log.COMMIT,
		}
		lsn, err := m.logManager.AppendLogRecord(rec)
		if err != nil {
			return err
		}

		m.logManager.Flush(lsn)
	}

	if m.bufferManager != nil {
		if bpm, ok := m.bufferManager.(interface{ FlushAll() error }); ok {
			bpm.FlushAll()
		}
	}
	return nil
}

func (m *TwoPhaseLockingManager) Abort(txnID int64) error {
	transaction, ok := m.transactions[TransactionID(txnID)]
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	return m.rollbackManager.RollbackTransaction(
		transaction,
		nil,
		nil,
	)
}

func (m *TwoPhaseLockingManager) Close() error {
	return nil
}
