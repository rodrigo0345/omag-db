package log

type LSN uint64

type ILogRecord interface {
}

type ILogManager interface {
	AppendLogRecord(record ILogRecord) (LSN, error)
	Flush(upToLSN LSN) error
	Recover() (*RecoveryState, error)
	Checkpoint() error
	GetLastCheckpointLSN() uint64
	Close() error

	ReadAllRecords() ([]WALRecord, error)

	// Operation tracking for crash recovery
	AddTransactionOperation(txnID uint64, opType RecordType, key []byte, value []byte)
	CleanupTransactionOperations(txnID uint64)
}
