package testutil

import (
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	wallog "github.com/rodrigo0345/omag/internal/txn/log"
)

func NewTestDiskManager(t *testing.T, tempDir string) *buffer.DiskManager {
	dbPath := TempFile(t, tempDir, "test.db")
	dm, err := buffer.NewDiskManager(dbPath)
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}
	t.Cleanup(func() {
		dm.Close()
	})
	return dm
}

func NewTestBufferPoolManager(t *testing.T, tempDir string) buffer.IBufferPoolManager {
	dm := NewTestDiskManager(t, tempDir)
	bpm := buffer.NewBufferPoolManager(100, dm)
	t.Cleanup(func() {
		bpm.Close()
	})
	return bpm
}

func NewTestWALManager(t *testing.T, tempDir string) wallog.ILogManager {
	walPath := TempFile(t, tempDir, "test.wal")
	wm, err := wallog.NewWALManager(walPath)
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}
	t.Cleanup(func() {
		wm.Close()
	})
	return wm
}
