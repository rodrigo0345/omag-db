package isolation

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/lock"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

type TransactionID uint64

// DEPRECATED: This is a simplified 2PL manager for demonstration purposes. It does not handle deadlocks, timeouts, or other complexities of a production-grade 2PL implementation.
type TwoPhaseLockingManager struct {
	mu              sync.RWMutex
	transactions    map[TransactionID]*txn_unit.Transaction
	lockManager     *lock.LockManager
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    write_handler.IWriteHandler
	rollbackManager *rollback.RollbackManager
	primaryIndex    storage.IStorageEngine
	indexManagers   map[string]*schema.SecondaryIndexManager
}

func NewTwoPhaseLockingManager(
	logManager log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler write_handler.IWriteHandler,
	rollbackMgr *rollback.RollbackManager,
	primaryIndex storage.IStorageEngine,
	indexManagers map[string]*schema.SecondaryIndexManager,
) *TwoPhaseLockingManager {
	return &TwoPhaseLockingManager{
		transactions:    make(map[TransactionID]*txn_unit.Transaction),
		lockManager:     lock.NewLockManager(),
		logManager:      logManager,
		bufferManager:   bufferMgr,
		writeHandler:    writeHandler,
		rollbackManager: rollbackMgr,
		primaryIndex:    primaryIndex,
		indexManagers:   indexManagers,
	}
}

func (m *TwoPhaseLockingManager) BeginTransaction(isolationLevel uint8, tableName string, tableSchema *schema.TableSchema) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := int64(len(m.transactions) + 1)
	txn := txn_unit.NewTransaction(uint64(txnID), isolationLevel)
	txn.SetTableContext(tableName, tableSchema)
	m.transactions[TransactionID(txnID)] = txn
	return txnID
}

func (m *TwoPhaseLockingManager) Read(txnID int64, Key []byte) ([]byte, error) {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	switch transaction.GetIsolationLevel() {
	case txn_unit.READ_UNCOMMITTED:
		transaction.RecordReadKey(Key)

		return m.primaryIndex.Get(Key)

	case txn_unit.READ_COMMITTED:

		if err := m.lockManager.LockShared(transaction, Key); err != nil {
			return nil, err
		}
		defer m.lockManager.Unlock(transaction, Key)
		transaction.RecordReadKey(Key)

		return m.primaryIndex.Get(Key)

	case txn_unit.REPEATABLE_READ:

		if err := m.lockManager.LockShared(transaction, Key); err != nil {
			return nil, err
		}
		transaction.RecordReadKey(Key)
		return m.primaryIndex.Get(Key)

	case txn_unit.SERIALIZABLE:

		if err := m.lockManager.LockShared(transaction, Key); err != nil {
			return nil, err
		}
		transaction.RecordReadKey(Key)
		return m.primaryIndex.Get(Key)

	default:
		return nil, fmt.Errorf("unsupported isolation level: %d", transaction.GetIsolationLevel())
	}
}

func (m *TwoPhaseLockingManager) Write(txnID int64, Key []byte, Value []byte) error {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	if err := m.lockManager.LockExclusive(transaction, Key); err != nil {
		return err
	}
	transaction.RecordWriteKey(Key)

	tableName, tableSchema := transaction.GetTableContext()
	transaction.RecordTableAccess(tableName)

	if err := m.acquireIndexLocks(transaction, tableName, tableSchema, Value); err != nil {
		return fmt.Errorf("failed to acquire index locks: %w", err)
	}

	if tableSchema != nil && m.indexManagers != nil {
		if indexMgr, exists := m.indexManagers[tableName]; exists && indexMgr != nil {
			if err := m.writeHandler.SetIndexContext(tableSchema, indexMgr); err != nil {
				return fmt.Errorf("failed to set index context: %w", err)
			}
		}
	}

	writeOp := write_handler.WriteOperation{
		Key:        Key,
		Value:      Value,
		PageID:     0,
		Offset:     0,
		TableName:  tableName,
		SchemaInfo: tableSchema,
		PrimaryKey: Key,
	}

	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *TwoPhaseLockingManager) Delete(txnID int64, Key []byte) error {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	if err := m.lockManager.LockExclusive(transaction, Key); err != nil {
		return err
	}
	transaction.RecordWriteKey(Key)

	tableName, tableSchema := transaction.GetTableContext()
	transaction.RecordTableAccess(tableName)

	beforeImage, _ := m.primaryIndex.Get(Key)
	if err := m.acquireIndexLocks(transaction, tableName, tableSchema, beforeImage); err != nil {
		return fmt.Errorf("failed to acquire index locks: %w", err)
	}

	if tableSchema != nil && m.indexManagers != nil {
		if indexMgr, exists := m.indexManagers[tableName]; exists && indexMgr != nil {
			if err := m.writeHandler.SetIndexContext(tableSchema, indexMgr); err != nil {
				return fmt.Errorf("failed to set index context: %w", err)
			}
		}
	}

	writeOp := write_handler.WriteOperation{
		Key:        Key,
		Value:      nil,
		PageID:     0,
		Offset:     0,
		IsDelete:   true,
		TableName:  tableName,
		SchemaInfo: tableSchema,
		PrimaryKey: Key,
	}

	return m.writeHandler.HandleWrite(transaction, writeOp)
}

func (m *TwoPhaseLockingManager) Commit(txnID int64) error {
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	transaction.Commit()

	for _, key := range transaction.GetSharedLocks() {
		m.lockManager.Unlock(transaction, key)
	}
	for _, key := range transaction.GetExclusiveLocks() {
		m.lockManager.Unlock(transaction, key)
	}

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
	m.mu.RLock()
	transaction, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	// Clean up transaction operations since this transaction is being aborted
	if m.logManager != nil {
		m.logManager.CleanupTransactionOperations(transaction.GetID())
	}

	err := m.rollbackManager.RollbackTransaction(
		transaction,
		nil,
		nil,
	)

	// Release all locks
	for _, key := range transaction.GetSharedLocks() {
		m.lockManager.Unlock(transaction, key)
	}
	for _, key := range transaction.GetExclusiveLocks() {
		m.lockManager.Unlock(transaction, key)
	}

	return err
}

func (m *TwoPhaseLockingManager) Close() error {
	return nil
}

func makeIndexLockKey(tableName, indexName string, indexValue []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString("__index:")
	buf.WriteString(tableName)
	buf.WriteString(":")
	buf.WriteString(indexName)
	buf.WriteString(":")
	buf.Write(indexValue)
	return buf.Bytes()
}

func (m *TwoPhaseLockingManager) acquireIndexLocks(transaction *txn_unit.Transaction, tableName string, tableSchema *schema.TableSchema, rowData []byte) error {
	if tableSchema == nil || len(tableSchema.Indexes) == 0 {
		return nil
	}

	indexValues, err := schema.ExtractIndexValues(tableSchema, rowData)
	if err != nil {
		return fmt.Errorf("failed to extract index values for locking: %w", err)
	}

	for indexName, indexValue := range indexValues {
		lockKey := makeIndexLockKey(tableName, indexName, indexValue)
		if err := m.lockManager.LockExclusive(transaction, lockKey); err != nil {
			return err
		}
	}

	return nil
}

func (m *TwoPhaseLockingManager) releaseIndexLocks(transaction *txn_unit.Transaction, tableName string, tableSchema *schema.TableSchema, rowData []byte) error {
	if tableSchema == nil || len(tableSchema.Indexes) == 0 {
		return nil
	}

	indexValues, err := schema.ExtractIndexValues(tableSchema, rowData)
	if err != nil {
		return fmt.Errorf("failed to extract index values for lock release: %w", err)
	}

	for indexName, indexValue := range indexValues {
		lockKey := makeIndexLockKey(tableName, indexName, indexValue)
		transaction.RemoveExclusiveLock(lockKey)
	}

	return nil
}
