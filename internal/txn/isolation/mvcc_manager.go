package isolation

import (
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

type txnStatus uint8

const (
	statusActive txnStatus = iota
	statusCommitted
	statusAborted
)

func (s txnStatus) String() string {
	switch s {
	case statusActive:
		return "active"
	case statusCommitted:
		return "committed"
	case statusAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

type txnMeta struct {
	txn    *txn_unit.Transaction
	status txnStatus
	// writtenUserKeys is the set of user-space keys this transaction wrote or
	// deleted. Populated by Write/Delete, consumed by conflict detection at
	// commit time. Keys are stored in user-key form (without the txnID suffix)
	// so they can be compared across transactions.
	writtenUserKeys map[string]struct{}
}

type MVCCManager struct {
	mu sync.RWMutex

	// transactions holds every transaction that has not been GC'd.
	// Committed and aborted entries stay here until GC sweeps them — their
	// status lets isVisible answer without a separate committedTxns map, and
	// lets GC know the low-water mark.
	transactions map[txn.TransactionID]*txnMeta

	logManager      log.ILogManager
	bufferManager   buffer.IBufferPoolManager
	rollbackManager *rollback.RollbackManager
	tableManager    schema.ITableManager

	nextTxnID      atomic.Int64
	minActiveTxnID atomic.Int64
}

func NewMVCCManager(
	logMgr log.ILogManager,
	bufferMgr buffer.IBufferPoolManager,
	rollbackMgr *rollback.RollbackManager,
	tableManager schema.ITableManager,
) *MVCCManager {
	m := &MVCCManager{
		transactions:    make(map[txn.TransactionID]*txnMeta),
		logManager:      logMgr,
		bufferManager:   bufferMgr,
		rollbackManager: rollbackMgr,
		tableManager:    tableManager,
	}
	m.minActiveTxnID.Store(math.MaxInt64)
	return m
}

// ---------------------------------------------------------------------------
// Transaction lifecycle
// ---------------------------------------------------------------------------

func (m *MVCCManager) BeginTransaction(isolationLevel uint8) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	id := m.nextTxnID.Add(1)
	t := txn_unit.NewTransaction(uint64(id), isolationLevel)

	// Snapshot: capture only the IDs of transactions that are still active.
	// A version written by txn X is invisible under REPEATABLE_READ /
	// SERIALIZABLE if X was in-flight (active) when we took this snapshot,
	// even if X commits later.
	activeIDs := make([]int64, 0, len(m.transactions))
	for tid, meta := range m.transactions {
		if meta.status == statusActive {
			activeIDs = append(activeIDs, int64(tid))
		}
	}
	t.SetSnapshot(activeIDs) // store which txns were active at the time of this txn's start for visibility checks

	m.transactions[txn.TransactionID(id)] = &txnMeta{
		txn:             t,
		status:          statusActive,
		writtenUserKeys: make(map[string]struct{}),
	}
	m.updateMinActive()
	return id
}

// Commit validates the transaction's write set against concurrent committed
// writers and, if no conflict is found, marks it committed. If a conflict is
// found the transaction is automatically aborted and an error is returned.
//
// Conflict rule (first-writer-wins / snapshot isolation):
//
//	For every user key this transaction wrote, if any other transaction with a
//	strictly higher ID than ours has already committed a write to that same
//	key, we lose — our view of the key was stale when we started writing.
//
// Why higher ID = later writer?
//
//	Transaction IDs are allocated monotonically at BeginTransaction. A higher
//	ID means the transaction started after ours. If such a transaction already
//	committed a write to a key we also wrote, it got there first (it
//	committed while we were still running). Allowing both commits would
//	silently discard the other writer's update — a lost update anomaly.
func (m *MVCCManager) Commit(txnID txn.TransactionID) error {
	m.mu.Lock()

	meta, ok := m.transactions[txnID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if meta.status != statusActive {
		m.mu.Unlock()
		return fmt.Errorf("transaction %d is already %s", txnID, meta.status)
	}

	// --- Conflict detection (under the write lock) ---
	//
	// Must hold the lock during detection so that no concurrent Commit can
	// sneak in between the check and our own status flip. Without the lock,
	// two conflicting transactions could both pass the check simultaneously
	// and both commit — defeating the purpose.
	if conflictID := m.detectWriteConflict(txnID, meta); conflictID != 0 {
		// Abort atomically while still holding the lock.
		meta.status = statusAborted
		m.updateMinActive()
		m.mu.Unlock()

		m.physicalRollback(meta)
		return fmt.Errorf(
			"write conflict: transaction %d aborted — transaction %d committed "+
				"a write to the same key(s) after we began",
			txnID, conflictID,
		)
	}

	// No conflict — flip to committed atomically before releasing the lock.
	// This closes the TOCTOU window: a reader cannot observe this txn as
	// committed before the status is fully set.
	meta.status = statusCommitted
	m.updateMinActive()
	m.mu.Unlock()

	meta.txn.Commit()

	if m.logManager != nil {
		rec := log.WALRecord{TxnID: meta.txn.GetID(), Type: log.COMMIT}
		lsn, _ := m.logManager.AppendLogRecord(rec)
		m.logManager.Flush(lsn)
	}

	return nil
}

// detectWriteConflict checks whether any committed transaction with an ID
// strictly greater than txnID has written to any of the same user keys.
// Returns the conflicting transaction's ID, or 0 if there is no conflict.
//
// Must be called with m.mu held (write lock).
func (m *MVCCManager) detectWriteConflict(txnID txn.TransactionID, meta *txnMeta) txn.TransactionID {
	if len(meta.writtenUserKeys) == 0 {
		return 0
	}

	snapshot := meta.txn.GetSnapshot() // returns all active transactions at the start of this transaction
	activeAtStart := make(map[int64]struct{})
	for id, active := range snapshot {
		if !active {
			continue
		}
		activeAtStart[id] = struct{}{}
	}

	for otherID, otherMeta := range m.transactions {
		if txn.TransactionID(otherID) == txnID {
			continue
		}

		if otherMeta.status != statusCommitted {
			continue
		}

		_, wasActive := activeAtStart[int64(otherID)]
		isLaterTxn := int64(otherID) > int64(txnID)

		if wasActive || isLaterTxn {
			for key := range meta.writtenUserKeys {
				if _, exists := otherMeta.writtenUserKeys[key]; exists {
					return otherID
				}
			}
		}
	}
	return 0
}

func (m *MVCCManager) Abort(txnID txn.TransactionID) error {
	m.mu.Lock()
	meta, ok := m.transactions[txnID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("transaction %d not found", txnID)
	}
	if meta.status != statusActive {
		m.mu.Unlock()
		return fmt.Errorf("transaction %d is already %s", txnID, meta.status)
	}

	meta.status = statusAborted
	m.updateMinActive()
	m.mu.Unlock()

	m.physicalRollback(meta)
	return nil
}

// physicalRollback removes WAL entries and undoes storage writes for an
// aborted transaction. Safe to call without holding m.mu.
func (m *MVCCManager) physicalRollback(meta *txnMeta) {
	if m.logManager != nil {
		m.logManager.CleanupTransactionOperations(meta.txn.GetID())
	}
	m.rollbackManager.RollbackTransaction(meta.txn, nil, nil)
}

func (m *MVCCManager) Read(txnID txn.TransactionID, tableName, indexName string, key []byte) ([]byte, error) {
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
	meta, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok || meta.status != statusActive {
		return fmt.Errorf("transaction %d is not active", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))
	payload := append([]byte{OpInsert}, value...)

	if m.logManager != nil {
		rec := log.WALRecord{TxnID: meta.txn.GetID(), Type: log.UPDATE, After: payload}
		if _, err := m.logManager.AppendLogRecord(rec); err != nil {
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

	// Track the user-space key (not the internal versioned key) so that
	// conflict detection at commit time can compare write sets across txns.
	meta.writtenUserKeys[string(key)] = struct{}{}

	meta.txn.RecordRecoveryOperation(tableName, log.PUT, internalKey, payload)
	if m.logManager != nil {
		m.logManager.AddTransactionOperation(meta.txn.GetID(), tableName, log.PUT, internalKey, payload)
		if il, ok := m.logManager.(log.ReplicationIntentLogger); ok {
			il.LogReplicationIntent(meta.txn.GetID(), tableName, log.PUT, internalKey, payload)
		}
	}
	return nil
}

func (m *MVCCManager) Delete(txnID txn.TransactionID, tableName, indexName string, key []byte) error {
	m.mu.RLock()
	meta, ok := m.transactions[txnID]
	m.mu.RUnlock()
	if !ok || meta.status != statusActive {
		return fmt.Errorf("transaction %d is not active", txnID)
	}

	internalKey := m.encodeKey(key, uint64(txnID))
	payload := []byte{OpDelete}

	var beforeImage []byte
	if v, err := m.Read(txnID, tableName, indexName, key); err == nil {
		beforeImage = v
	}

	if m.logManager != nil {
		rec := log.WALRecord{TxnID: meta.txn.GetID(), Type: log.UPDATE, Before: beforeImage, After: payload}
		if _, err := m.logManager.AppendLogRecord(rec); err != nil {
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

	// A delete is a write — track it for conflict detection.
	meta.writtenUserKeys[string(key)] = struct{}{}

	meta.txn.RecordRecoveryOperation(tableName, log.DELETE, internalKey, nil)
	if m.logManager != nil {
		m.logManager.AddTransactionOperation(meta.txn.GetID(), tableName, log.DELETE, internalKey, nil)
		if il, ok := m.logManager.(log.ReplicationIntentLogger); ok {
			il.LogReplicationIntent(meta.txn.GetID(), tableName, log.DELETE, internalKey, nil)
		}
	}
	return nil
}

func (m *MVCCManager) Scan(txnID txn.TransactionID, tableName, indexName string, opts storage.ScanOptions) (storage.ICursor, error) {
	m.mu.RLock()
	meta, ok := m.transactions[txnID]
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
		txn:      meta.txn,
		seenKeys: make(map[string]bool),
	}, nil
}

// ---------------------------------------------------------------------------
// Visibility
// ---------------------------------------------------------------------------

// isVisible answers: can transaction t see the version written by xmin?
//
//  1. Own write: always visible.
//  2. READ_COMMITTED: visible iff the writer has committed (re-evaluated per
//     read — no snapshot pinning).
//  3. REPEATABLE_READ / SERIALIZABLE: visible iff the writer committed AND
//     was NOT in the active set at snapshot time (i.e. had already committed
//     before this transaction began).
//
// Note on IsVisibleInSnapshot: must return true when xmin was NOT in the
// active-at-begin set — meaning it had already committed before our snapshot.
// If xmin WAS in the active set (in-flight at our begin) it must return false.
func (m *MVCCManager) isVisible(t *txn_unit.Transaction, xmin txn.TransactionID) bool {
	if xmin == t.GetID() {
		return true
	}

	m.mu.RLock()
	xminMeta, exists := m.transactions[xmin]
	m.mu.RUnlock()

	// Entry was GC'd — only evicted after all live snapshots have advanced
	// past it, so treat as committed long ago.
	if !exists {
		return true
	}

	committed := xminMeta.status == statusCommitted

	switch t.GetIsolationLevel() {
	case txn_unit.READ_COMMITTED:
		return committed

	case txn_unit.REPEATABLE_READ, txn_unit.SERIALIZABLE:
		if !committed {
			return false
		}
		return t.IsVisibleInSnapshot(int64(xmin))

	default:
		return committed
	}
}

// ---------------------------------------------------------------------------
// GC / version pruning
// ---------------------------------------------------------------------------

func (m *MVCCManager) GC() {
	m.mu.Lock()
	lowWater := m.computeMinActive()
	for id, meta := range m.transactions {
		if meta.status != statusActive && int64(id) < lowWater {
			delete(m.transactions, id)
		}
	}
	m.mu.Unlock()

	pruneKeys, err := m.collectPruneableKeys(lowWater)
	if err != nil || len(pruneKeys) == 0 {
		return
	}
	for _, k := range pruneKeys {
		_ = m.tableManager.Delete(k)
	}
}

func (m *MVCCManager) collectPruneableKeys(lowWater int64) ([]schema.DeleteOperation, error) {
	opts := storage.ScanOptions{Inclusive: true}
	tables := m.tableManager.GetAllTables()
	var toDelete []schema.DeleteOperation

	for _, tableName := range tables {
		tableSchema, err := m.tableManager.GetTableSchema(tableName)
		if err != nil {
			continue
		}
		for _, index := range tableSchema.GetAllIndexes() {
			indexName := index.Name
			raw, err := m.tableManager.Scan(tableName, indexName, opts)
			if err != nil {
				continue
			}

			type versionRecord struct {
				internalKey schema.DeleteOperation
				txnID       int64
				status      txnStatus
			}
			groupByUserKey := make(map[string][]versionRecord)

			for raw.Next() {
				entry := raw.Entry()
				userKey, writerID, ok := m.decodeKey(entry.Key)
				if !ok {
					panic(fmt.Sprintf("unexpected key format during GC: %s", string(entry.Key)))
				}

				m.mu.RLock()
				xminMeta, exists := m.transactions[txn.TransactionID(writerID)]
				m.mu.RUnlock()

				var st txnStatus
				if !exists {
					st = statusCommitted
				} else {
					st = xminMeta.status
				}

				uk := string(userKey)
				groupByUserKey[uk] = append(groupByUserKey[uk], versionRecord{
					internalKey: schema.DeleteOperation{TableName: tableName, Key: entry.Key},
					txnID:       int64(writerID),
					status:      st,
				})
			}
			raw.Close()

			for _, versions := range groupByUserKey {
				// Versions are newest-first due to ^txnID encoding.
				// Find the youngest committed version.
				youngestCommitted := -1
				for i, v := range versions {
					if v.status == statusCommitted {
						youngestCommitted = i
						break
					}
				}
				if youngestCommitted < 0 {
					continue
				}

				// Everything older and below the low-water mark can be pruned.
				for i := youngestCommitted + 1; i < len(versions); i++ {
					if versions[i].txnID < lowWater {
						toDelete = append(toDelete, versions[i].internalKey)
					}
				}

				// Aborted versions above the youngest committed can also be
				// pruned if they are below the low-water mark.
				for i := 0; i < youngestCommitted; i++ {
					v := versions[i]
					if v.status == statusAborted && v.txnID < lowWater {
						toDelete = append(toDelete, v.internalKey)
					}
				}
			}
		}
	}
	return toDelete, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (m *MVCCManager) updateMinActive() {
	m.minActiveTxnID.Store(m.computeMinActive())
}

func (m *MVCCManager) computeMinActive() int64 {
	min := int64(math.MaxInt64)
	for id, meta := range m.transactions {
		if meta.status == statusActive && int64(id) < min {
			min = int64(id)
		}
	}
	return min
}

func (m *MVCCManager) encodeKey(key []byte, ts uint64) []byte {
	buf := make([]byte, len(key)+8)
	copy(buf, key)
	binary.BigEndian.PutUint64(buf[len(key):], math.MaxUint64-ts)
	return buf
}
func (m *MVCCManager) decodeKey(fullKey []byte) ([]byte, uint64, bool) {
	if len(fullKey) < 8 {
		return nil, 0, false
	}

	// Extract the user-facing key (everything before the last 8 bytes)
	userKey := fullKey[:len(fullKey)-8]

	// Read the inverted timestamp
	invertedTs := binary.BigEndian.Uint64(fullKey[len(fullKey)-8:])

	// Undo the inversion to get the original Transaction ID
	ts := math.MaxUint64 - invertedTs

	return userKey, ts, true
}
