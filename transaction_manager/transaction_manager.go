package transaction_manager

type TransactionManager struct {
	nextTxnID uint64
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{}
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
	// Here logic to unlock and WAL commit
	return nil
}

func (tm *TransactionManager) Abort(txn *Transaction) error {
	tm.rollbackChanges(txn)
	txn.state = ABORTED
	// Here logic to unlock and WAL abort
	return nil
}

func (tm *TransactionManager) rollbackChanges(txn *Transaction) {
	// Revert logged changes in reverse order
	for i := len(txn.undoLog) - 1; i >= 0; i-- {
		// entry := txn.undoLog[i]
		// process undo entry
	}
}
