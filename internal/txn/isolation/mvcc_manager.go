package isolation

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

const (
	Separator uint8 = 0x00
	OpInsert  uint8 = 0x01
	OpDelete  uint8 = 0x02
)

type MVCCManager struct {
	mu              sync.RWMutex
	transactions    map[txn.TransactionID]*txn_unit.Transaction
	committedTxns   map[txn.TransactionID]bool
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	rollbackManager *rollback.RollbackManager
	tableManager    schema.ITableManager
	nextTxnID       atomic.Int64
}

func NewMVCCManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	rollbackMgr *rollback.RollbackManager,
	tableManager schema.ITableManager,
) *MVCCManager {
	mvcc := &MVCCManager{
		transactions:    make(map[txn.TransactionID]*txn_unit.Transaction),
		committedTxns:   make(map[txn.TransactionID]bool),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		rollbackManager: rollbackMgr,
		tableManager:    tableManager,
	}

	// TODO: add compaction

	return mvcc
}

func (m *MVCCManager) BeginTransaction(isolationLevel uint8) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	txnID := m.nextTxnID.Add(1)
	transaction := txn_unit.NewTransaction(uint64(txnID), isolationLevel)

	// Snapshot Isolation: Capture active transaction IDs
	activeIDs := make([]int64, 0, len(m.transactions))
	for id := range m.transactions {
		activeIDs = append(activeIDs, int64(id))
	}
	transaction.SetSnapshot(activeIDs)

	m.transactions[txn.TransactionID(txnID)] = transaction
	return txnID
}

func (m *MVCCManager) Commit(txnID txn.TransactionID) error {
	m.mu.Lock()
	txn, ok := m.transactions[txn.TransactionID(txnID)]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("transaction %d not found", txnID)
	}

	m.committedTxns[txnID] = true
	m.mu.Unlock()

	txn.Commit()

	if m.logManager != nil {
		rec := log.WALRecord{TxnID: txn.GetID(), Type: log.COMMIT}
		lsn, _ := m.logManager.AppendLogRecord(rec)
		m.logManager.Flush(lsn)
	}

	m.mu.Lock()
	delete(m.transactions, txnID)
	m.mu.Unlock()
	return nil
}

func (m *MVCCManager) Abort(txnID txn.TransactionID) error {
	m.mu.Lock()
	txn, ok := m.transactions[txn.TransactionID(txnID)]
	delete(m.transactions, txnID)
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	if m.logManager != nil {
		m.logManager.CleanupTransactionOperations(txn.GetID())
	}

	return m.rollbackManager.RollbackTransaction(txn, nil, nil)
}

func (m *MVCCManager) Read(txnID txn.TransactionID, tableName, indexName string, key []byte) ([]byte, error) {
	// Point read executes as a range scan for [UserKey + \x00 + 0] to [UserKey + \x00 + ^0]
	opts := storage.ScanOptions{
		LowerBound: m.encodeKey(key, math.MaxUint64),
		UpperBound: m.encodeKey(key, 0),
		Inclusive:  true,
	}

	cursor, err := m.Scan(txnID, tableName, indexName, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	if cursor.Next() {
		return cursor.Entry().Value, nil
	}
	return nil, fmt.Errorf("key not found")
}

func (m *MVCCManager) Write(txnID txn.TransactionID, tableName, indexName string, key []byte, value []byte) error {
	m.mu.RLock()
	txn, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))
	payload := append([]byte{OpInsert}, value...)

	if m.logManager != nil {
		walRecord := log.WALRecord{
			TxnID: txn.GetID(),
			Type:  log.UPDATE,
			After: payload,
		}
		if _, err := m.logManager.AppendLogRecord(walRecord); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if err := m.tableManager.Write(schema.WriteOperation{
		TableName: tableName,
		Key:       internalKey,
		Value:     payload,
	}); err != nil {
		return fmt.Errorf("table manager write failed: %w", err)
	}

	txn.RecordRecoveryOperation(tableName, log.PUT, internalKey, payload)
	if m.logManager != nil {
		m.logManager.AddTransactionOperation(txn.GetID(), tableName, log.PUT, internalKey, payload)
		if intentLogger, ok := m.logManager.(log.ReplicationIntentLogger); ok {
			intentLogger.LogReplicationIntent(txn.GetID(), tableName, log.PUT, internalKey, payload)
		}
	}

	return nil
}

func (m *MVCCManager) Delete(txnID txn.TransactionID, tableName, indexName string, key []byte) error {
	m.mu.RLock()
	txn, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))
	payload := []byte{OpDelete}

	var beforeImage []byte
	if existingVal, err := m.Read(txnID, tableName, indexName, key); err == nil {
		beforeImage = existingVal
	}

	if m.logManager != nil {
		walRecord := log.WALRecord{
			TxnID:  txn.GetID(),
			Type:   log.UPDATE,
			Before: beforeImage,
			After:  payload,
		}
		if _, err := m.logManager.AppendLogRecord(walRecord); err != nil {
			return fmt.Errorf("WAL write failed: %w", err)
		}
	}

	if err := m.tableManager.Write(schema.WriteOperation{
		TableName: tableName,
		Key:       internalKey,
		Value:     payload,
	}); err != nil {
		return fmt.Errorf("table manager delete write failed: %w", err)
	}

	txn.RecordRecoveryOperation(tableName, log.DELETE, internalKey, nil)
	if m.logManager != nil {
		m.logManager.AddTransactionOperation(txn.GetID(), tableName, log.DELETE, internalKey, nil)
		if intentLogger, ok := m.logManager.(log.ReplicationIntentLogger); ok {
			intentLogger.LogReplicationIntent(txn.GetID(), tableName, log.DELETE, internalKey, nil)
		}
	}

	return nil
}

func (m *MVCCManager) Scan(txnID txn.TransactionID, tableName, indexName string, opts storage.ScanOptions) (storage.ICursor, error) {
	m.mu.RLock()
	txn, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	raw, err := m.tableManager.Scan(tableName, indexName, opts)
	if err != nil {
		return nil, err
	}

	return &MVCCCursor{
		raw:      raw,
		manager:  m,
		txn:      txn,
		seenKeys: make(map[string]bool),
	}, nil
}

func (m *MVCCManager) isVisible(txn *txn_unit.Transaction, xmin txn.TransactionID) bool {
	if xmin == txn.GetID() {
		return true
	}

	m.mu.RLock()
	committed := m.committedTxns[xmin]
	m.mu.RUnlock()

	switch txn.GetIsolationLevel() {
	case txn_unit.READ_COMMITTED:
		return committed
	case txn_unit.REPEATABLE_READ:
		if !committed || xmin > txn.GetID() {
			return false
		}
		return txn.IsVisibleInSnapshot(int64(xmin))
	default:
		return committed
	}
}

func (m *MVCCManager) encodeKey(userKey []byte, txnID uint64) []byte {
	buf := make([]byte, len(userKey)+9)
	copy(buf, userKey)
	buf[len(userKey)] = Separator
	binary.BigEndian.PutUint64(buf[len(userKey)+1:], ^txnID)
	return buf
}

func (m *MVCCManager) decodeKey(internalKey []byte) ([]byte, uint64) {
	sepIdx := bytes.LastIndexByte(internalKey, Separator)
	userKey := internalKey[:sepIdx]
	invertedID := binary.BigEndian.Uint64(internalKey[sepIdx+1:])
	return userKey, ^invertedID
}
