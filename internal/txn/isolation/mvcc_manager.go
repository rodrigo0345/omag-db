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
	"github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
	"github.com/rodrigo0345/omag/internal/txn/write_handler"
)

const (
	Separator uint8 = 0x00
	OpInsert  uint8 = 0x01
	OpDelete  uint8 = 0x02
)

type MVCCManager struct {
	mu              sync.RWMutex
	transactions    map[TransactionID]*txn_unit.Transaction
	committedTxns   map[int64]bool
	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	writeHandler    write_handler.IWriteHandler
	rollbackManager *rollback.RollbackManager
	tableManager    schema.ITableManager
	nextTxnID       atomic.Int64
}

func NewMVCCManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	writeHandler write_handler.IWriteHandler,
	rollbackMgr *rollback.RollbackManager,
	tableManager schema.ITableManager,
) *MVCCManager {
	mvcc := &MVCCManager{
		transactions:    make(map[TransactionID]*txn_unit.Transaction),
		committedTxns:   make(map[int64]bool),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		writeHandler:    writeHandler,
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

	m.transactions[TransactionID(txnID)] = transaction
	return txnID
}

func (m *MVCCManager) Commit(txnID int64) error {
	m.mu.Lock()
	txn, ok := m.transactions[TransactionID(txnID)]
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
	delete(m.transactions, TransactionID(txnID))
	m.mu.Unlock()
	return nil
}

func (m *MVCCManager) Abort(txnID int64) error {
	m.mu.Lock()
	txn, ok := m.transactions[TransactionID(txnID)]
	delete(m.transactions, TransactionID(txnID))
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	if m.logManager != nil {
		m.logManager.CleanupTransactionOperations(txn.GetID())
	}

	return m.rollbackManager.RollbackTransaction(txn, nil, nil)
}

func (m *MVCCManager) Read(txnID int64, target storage.IStorageEngine, key []byte) ([]byte, error) {
	// Point read executes as a range scan for [UserKey + \x00 + 0] to [UserKey + \x00 + ^0]
	opts := storage.ScanOptions{
		LowerBound: m.encodeKey(key, math.MaxUint64),
		UpperBound: m.encodeKey(key, 0),
		Inclusive:  true,
	}

	cursor, err := m.Scan(txnID, target, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	if cursor.Next() {
		return cursor.Entry().Value, nil
	}
	return nil, fmt.Errorf("key not found")
}

func (m *MVCCManager) Write(txnID int64, target storage.IStorageEngine, key []byte, value []byte) error {
	m.mu.RLock()
	txn, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))
	payload := append([]byte{OpInsert}, value...)

	writeOp := write_handler.WriteOperation{
		Key:   internalKey,
		Value: payload,
	}
	return m.writeHandler.HandleWrite(txn, writeOp)
}

func (m *MVCCManager) Delete(txnID int64, target storage.IStorageEngine, key []byte) error {
	m.mu.RLock()
	txn, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("transaction %d not found", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))
	payload := []byte{OpDelete}

	writeOp := write_handler.WriteOperation{
		Key:      internalKey,
		Value:    payload,
		IsDelete: true,
	}
	return m.writeHandler.HandleWrite(txn, writeOp)
}

func (m *MVCCManager) Scan(txnID int64, target storage.IStorageEngine, opts storage.ScanOptions) (storage.ICursor, error) {
	m.mu.RLock()
	txn, ok := m.transactions[TransactionID(txnID)]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("transaction %d not found", txnID)
	}

	raw, err := target.Scan(opts)
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

func (m *MVCCManager) compaction(target storage.IStorageEngine) error {
	m.mu.RLock()
	minActiveID := int64(math.MaxInt64)
	for id := range m.transactions {
		if int64(id) < minActiveID {
			minActiveID = int64(id)
		}
	}
	m.mu.RUnlock()

	// If no transactions active, we can compact up to the current nextTxnID
	if minActiveID == math.MaxInt64 {
		minActiveID = m.nextTxnID.Load()
	}

	raw, err := target.Scan(storage.ScanOptions{})
	if err != nil {
		return err
	}
	defer raw.Close()

	var lastUserKey string
	var foundVisible bool

	for raw.Next() {
		entry := raw.Entry()
		userKey, xmin := m.decodeKey(entry.Key)
		uKeyStr := string(userKey)

		if uKeyStr != lastUserKey {
			lastUserKey = uKeyStr
			foundVisible = false
		}

		// Keep if xmin >= minActiveID (still potentially needed for snapshots)
		if int64(xmin) >= minActiveID {
			continue
		}

		// If xmin < minActiveID and we already found a visible version for this key,
		// this version is logically dead and can be purged.
		if foundVisible {
			target.Delete(entry.Key)
			continue
		}

		// Mark first encountered version < minActiveID as the "base" version to keep
		foundVisible = true

		// If the base version is a delete, we can purge the tombstone itself
		if entry.Value[0] == OpDelete {
			target.Delete(entry.Key)
		}
	}
	return nil
}

func (m *MVCCManager) isVisible(txn *txn_unit.Transaction, xmin uint64) bool {
	if xmin == txn.GetID() {
		return true
	}

	m.mu.RLock()
	committed := m.committedTxns[int64(xmin)]
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
