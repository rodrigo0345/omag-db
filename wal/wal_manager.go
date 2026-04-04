package wal

import (
	"encoding/binary"
	"os"
	"sync"
)

type RecordType uint8

const (
	UPDATE RecordType = iota
	COMMIT
	ABORT
	CHECKPOINT
)

type PageID uint32

type WALRecord struct {
	LSN    uint64
	TxnID  uint64
	Type   RecordType
	PageID PageID
	Offset uint16
	Before []byte
	After  []byte
}

type WALManager struct {
	logFile *os.File
	lsn     uint64 // monotonically increasing
	mu      sync.Mutex
}

func NewWALManager(filePath string) (*WALManager, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &WALManager{
		logFile: file,
		lsn:     0,
	}, nil
}

func (wm *WALManager) AppendLog(rec WALRecord) uint64 {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.lsn++
	rec.LSN = wm.lsn

	// Serialize the WALRecord to bytes
	buf := make([]byte, 0, 256)

	// Write LSN (8 bytes)
	lsnBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(lsnBuf, rec.LSN)
	buf = append(buf, lsnBuf...)

	// Write TxnID (8 bytes)
	txnBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(txnBuf, rec.TxnID)
	buf = append(buf, txnBuf...)

	// Write Type (1 byte)
	buf = append(buf, byte(rec.Type))

	// Write PageID (4 bytes)
	pageBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(pageBuf, uint32(rec.PageID))
	buf = append(buf, pageBuf...)

	// Write Offset (2 bytes)
	offsetBuf := make([]byte, 2)
	binary.LittleEndian.PutUint16(offsetBuf, rec.Offset)
	buf = append(buf, offsetBuf...)

	// Write Before length and data
	beforeLenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(beforeLenBuf, uint32(len(rec.Before)))
	buf = append(buf, beforeLenBuf...)
	buf = append(buf, rec.Before...)

	// Write After length and data
	afterLenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(afterLenBuf, uint32(len(rec.After)))
	buf = append(buf, afterLenBuf...)
	buf = append(buf, rec.After...)

	// Write to file
	wm.logFile.Write(buf)

	return wm.lsn
}

func (wm *WALManager) Flush(upToLSN uint64) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Flush the OS buffer to disk
	return wm.logFile.Sync()
}

func (wm *WALManager) Recover() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Implement ARIES-style redo/undo recovery here
	return nil
}

func (wm *WALManager) Checkpoint() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Create a checkpoint record and append it
	return wm.logFile.Sync()
}

func (wm *WALManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return wm.logFile.Close()
}
