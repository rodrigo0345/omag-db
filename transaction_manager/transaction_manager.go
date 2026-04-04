package transaction_manager

import (
	"github.com/rodrigo0345/omag/wal"
)

type TransactionManager struct {
	nextTxnID  uint64
	walMgr     *wal.WALManager
	bufferPool interface{} // BufferPoolManager reference (to avoid circular imports)
}

func NewTransactionManager(walMgr *wal.WALManager) *TransactionManager {
	return &TransactionManager{
		walMgr: walMgr,
	}
}

// SetBufferPool sets the buffer pool manager reference
func (tm *TransactionManager) SetBufferPool(bufferPool interface{}) {
	tm.bufferPool = bufferPool
}

func (tm *TransactionManager) Begin() *Transaction {
	tm.nextTxnID++
	return &Transaction{
		txnID:          tm.nextTxnID,
		state:          ACTIVE,
		sharedLocks:    make([][]byte, 0),
		exclusiveLocks: make([][]byte, 0),
		undoLog:        make([]UndoEntry, 0),
	}
}

func (tm *TransactionManager) Commit(txn *Transaction) error {
	txn.state = COMMITTED

	if tm.walMgr != nil {
		rec := wal.WALRecord{
			TxnID: txn.GetID(),
			Type:  wal.COMMIT,
		}
		lsn := tm.walMgr.AppendLog(rec)
		// Force flush on commit to ensure durability
		tm.walMgr.Flush(lsn)
	}

	// Flush all dirty pages from buffer pool to disk
	if tm.bufferPool != nil {
		if bpm, ok := tm.bufferPool.(interface{ FlushAll() error }); ok {
			bpm.FlushAll()
		}
	}

	// Sync disk manager
	// (This is done via flush above)

	return nil
}

func (tm *TransactionManager) Abort(txn *Transaction) error {
	tm.rollbackChanges(txn)
	txn.state = ABORTED

	if tm.walMgr != nil {
		rec := wal.WALRecord{
			TxnID: txn.GetID(),
			Type:  wal.ABORT,
		}
		lsn := tm.walMgr.AppendLog(rec)
		tm.walMgr.Flush(lsn)
	}

	return nil
}

func (tm *TransactionManager) rollbackChanges(txn *Transaction) {
	// Revert logged changes in reverse order
	for i := len(txn.undoLog) - 1; i >= 0; i-- {
		// entry := txn.undoLog[i]
		// process undo entry
	}
}
