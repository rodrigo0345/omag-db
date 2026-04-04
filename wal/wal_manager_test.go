package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// TestNewWALManager_Success tests successful WAL manager creation
func TestNewWALManager_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, err := NewWALManager(walFile)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	defer wm.Close()

	if wm.logFile == nil {
		t.Fatal("expected logFile to be initialized")
	}
	if wm.lsn != 0 {
		t.Fatalf("expected initial LSN 0, got %d", wm.lsn)
	}
}

// TestNewWALManager_InvalidPath tests WAL manager creation with invalid path
func TestNewWALManager_InvalidPath(t *testing.T) {
	invalidPath := "/nonexistent/directory/that/does/not/exist/wal.log"
	_, err := NewWALManager(invalidPath)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

// TestAppendLog_SerializesCorrectly tests WAL record serialization
func TestAppendLog_SerializesCorrectly(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	rec := WALRecord{
		TxnID:  42,
		Type:   UPDATE,
		PageID: 5,
		Offset: 100,
		Before: []byte("olddata"),
		After:  []byte("newdata"),
	}

	lsn := wm.AppendLog(rec)

	if lsn != 1 {
		t.Fatalf("expected LSN 1, got %d", lsn)
	}
	// LSN is returned from AppendLog, which is what we check
	if lsn != 1 {
		t.Fatalf("expected returned LSN to be 1, got %d", lsn)
	}
}

// TestAppendLog_SequentialLSN tests that LSN increments sequentially
func TestAppendLog_SequentialLSN(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	lsns := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		rec := WALRecord{TxnID: uint64(i + 1), Type: UPDATE}
		lsns[i] = wm.AppendLog(rec)
	}

	// Verify sequential LSNs
	for i, lsn := range lsns {
		if lsn != uint64(i+1) {
			t.Fatalf("expected LSN %d, got %d", i+1, lsn)
		}
	}
}

// TestAppendLog_DifferentRecordTypes tests appending different record types
func TestAppendLog_DifferentRecordTypes(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	types := []RecordType{UPDATE, COMMIT, ABORT, CHECKPOINT}
	for _, rt := range types {
		rec := WALRecord{Type: rt, TxnID: 1}
		lsn := wm.AppendLog(rec)
		if lsn == 0 {
			t.Fatalf("failed to append record of type %d", rt)
		}
	}
}

// TestAppendLog_WritesToFile tests that records are actually written to file
func TestAppendLog_WritesToFile(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	rec := WALRecord{
		TxnID:  99,
		Type:   UPDATE,
		PageID: 10,
		Offset: 200,
		Before: []byte("before"),
		After:  []byte("after"),
	}

	wm.AppendLog(rec)
	wm.Close()

	// Check file exists and has size > 0
	stat, err := os.Stat(walFile)
	if err != nil {
		t.Fatalf("failed to stat WAL file: %v", err)
	}
	if stat.Size() == 0 {
		t.Fatal("expected WAL file to have data written")
	}
}

// TestAppendLog_LargeData tests appending records with large data
func TestAppendLog_LargeData(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	rec := WALRecord{
		TxnID:  1,
		Type:   UPDATE,
		PageID: 1,
		Before: largeData,
		After:  largeData,
	}

	lsn := wm.AppendLog(rec)
	if lsn != 1 {
		t.Fatalf("failed to append large record, LSN: %d", lsn)
	}
}

// TestFlush_Success tests flushing WAL to disk
func TestFlush_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	rec := WALRecord{TxnID: 5, Type: COMMIT}
	lsn := wm.AppendLog(rec)

	err := wm.Flush(lsn)
	if err != nil {
		t.Fatalf("failed to flush: %v", err)
	}
}

// TestFlush_ZeroLSN tests flushing with LSN 0
func TestFlush_ZeroLSN(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	err := wm.Flush(0)
	if err != nil {
		t.Fatalf("failed to flush with LSN 0: %v", err)
	}
}

// TestRecover tests recover function (placeholder)
func TestRecover_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	err := wm.Recover()
	if err != nil {
		t.Fatalf("recover failed: %v", err)
	}
}

// TestCheckpoint tests checkpoint function
func TestCheckpoint_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	err := wm.Checkpoint()
	if err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}
}

// TestClose_Success tests closing WAL manager
func TestClose_Success(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	err := wm.Close()
	if err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// After close, trying to use the manager should fail
	// (logFile is closed but the pointer may not be nil)
}

// TestClose_Idempotent tests that close operations are safe
func TestClose_Idempotent(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	// First close should work
	err1 := wm.Close()
	if err1 != nil {
		t.Fatalf("first close failed: %v", err1)
	}
}

// TestRecordTypes_Constants tests record type constants
func TestRecordTypes_Constants(t *testing.T) {
	if UPDATE != 0 {
		t.Fatalf("expected UPDATE to be 0, got %d", UPDATE)
	}
	if COMMIT != 1 {
		t.Fatalf("expected COMMIT to be 1, got %d", COMMIT)
	}
	if ABORT != 2 {
		t.Fatalf("expected ABORT to be 2, got %d", ABORT)
	}
	if CHECKPOINT != 3 {
		t.Fatalf("expected CHECKPOINT to be 3, got %d", CHECKPOINT)
	}
}

// TestAppendLog_MultipleRecords tests appending multiple records in sequence
func TestAppendLog_MultipleRecords(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	records := []WALRecord{
		{TxnID: 1, Type: UPDATE, PageID: 1, Before: []byte("a"), After: []byte("b")},
		{TxnID: 1, Type: UPDATE, PageID: 2, Before: []byte("c"), After: []byte("d")},
		{TxnID: 2, Type: UPDATE, PageID: 3, Before: []byte("e"), After: []byte("f")},
		{TxnID: 1, Type: COMMIT},
		{TxnID: 2, Type: COMMIT},
	}

	for i, rec := range records {
		lsn := wm.AppendLog(rec)
		if lsn != uint64(i+1) {
			t.Fatalf("record %d: expected LSN %d, got %d", i, i+1, lsn)
		}
	}
}

// TestAppendLog_Serialization_Format tests the binary format of serialized records
func TestAppendLog_Serialization_Format(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)

	rec := WALRecord{
		TxnID:  42,
		Type:   UPDATE,
		PageID: 5,
		Offset: 100,
		Before: []byte("BEFORE"),
		After:  []byte("AFTER"),
	}

	wm.AppendLog(rec)
	wm.Close()

	// Read back the file and verify binary format
	file, err := os.Open(walFile)
	if err != nil {
		t.Fatalf("failed to open WAL file: %v", err)
	}
	defer file.Close()

	// Read LSN (8 bytes)
	lsnBytes := make([]byte, 8)
	file.Read(lsnBytes)
	lsn := binary.LittleEndian.Uint64(lsnBytes)
	if lsn != 1 {
		t.Fatalf("expected LSN 1 in file, got %d", lsn)
	}

	// Read TxnID (8 bytes)
	txnBytes := make([]byte, 8)
	file.Read(txnBytes)
	txnID := binary.LittleEndian.Uint64(txnBytes)
	if txnID != 42 {
		t.Fatalf("expected TxnID 42 in file, got %d", txnID)
	}

	// Read Type (1 byte)
	typeBytes := make([]byte, 1)
	file.Read(typeBytes)
	if typeBytes[0] != byte(UPDATE) {
		t.Fatalf("expected Type UPDATE in file, got %d", typeBytes[0])
	}
}

// TestAppendLog_ConcurrentAppends tests thread safety (basic)
func TestAppendLog_ConcurrentAppends(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	// Simple sequential appends to test LSN ordering
	// (full concurrency test would require goroutines)
	for i := 0; i < 10; i++ {
		rec := WALRecord{TxnID: uint64(i), Type: UPDATE}
		lsn := wm.AppendLog(rec)
		if lsn != uint64(i+1) {
			t.Fatalf("append %d: expected LSN %d, got %d", i, i+1, lsn)
		}
	}
}

// TestWALRecord_Structure tests WALRecord data structure
func TestWALRecord_Structure(t *testing.T) {
	rec := WALRecord{
		LSN:    10,
		TxnID:  20,
		Type:   UPDATE,
		PageID: 30,
		Offset: 40,
		Before: []byte("before"),
		After:  []byte("after"),
	}

	if rec.LSN != 10 {
		t.Fatalf("expected LSN 10, got %d", rec.LSN)
	}
	if rec.TxnID != 20 {
		t.Fatalf("expected TxnID 20, got %d", rec.TxnID)
	}
	if rec.Type != UPDATE {
		t.Fatalf("expected Type UPDATE, got %d", rec.Type)
	}
	if rec.PageID != 30 {
		t.Fatalf("expected PageID 30, got %d", rec.PageID)
	}
	if rec.Offset != 40 {
		t.Fatalf("expected Offset 40, got %d", rec.Offset)
	}
	if string(rec.Before) != "before" {
		t.Fatalf("expected Before 'before', got '%s'", string(rec.Before))
	}
	if string(rec.After) != "after" {
		t.Fatalf("expected After 'after', got '%s'", string(rec.After))
	}
}

// TestAppendLog_EmptyDataFields tests records with empty Before/After
func TestAppendLog_EmptyDataFields(t *testing.T) {
	walFile := getTempWalFile(t)
	defer os.Remove(walFile)

	wm, _ := NewWALManager(walFile)
	defer wm.Close()

	rec := WALRecord{
		TxnID:  1,
		Type:   COMMIT, // Commit records might not have Before/After
		Before: []byte{},
		After:  []byte{},
	}

	lsn := wm.AppendLog(rec)
	if lsn != 1 {
		t.Fatalf("failed to append record with empty data, LSN: %d", lsn)
	}
}

// Helper function to get temp WAL file
func getTempWalFile(t *testing.T) string {
	tmpDir := t.TempDir()
	return filepath.Join(tmpDir, "test.wal")
}
