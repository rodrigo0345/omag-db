package btree

import (
	"bytes"
	"os"
	"testing"

	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/logmanager"
	"github.com/rodrigo0345/omag/transaction_manager"
)

// setupBTreeComponents creates all necessary components for testing
func setupBTreeComponents(t *testing.T, pageSize uint32) (*BTree, *transaction_manager.TransactionManager, *buffermanager.BufferPoolManager) {
	// Create temporary files for testing
	dbFile, err := os.CreateTemp("", "omag-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp db file: %v", err)
	}
	dbFile.Close()
	t.Cleanup(func() { os.Remove(dbFile.Name()) })

	walFile, err := os.CreateTemp("", "omag-test-*.wal")
	if err != nil {
		t.Fatalf("failed to create temp wal file: %v", err)
	}
	walFile.Close()
	t.Cleanup(func() { os.Remove(walFile.Name()) })

	// Create disk manager
	diskMgr, err := buffermanager.NewDiskManager(dbFile.Name())
	if err != nil {
		t.Fatalf("failed to create disk manager: %v", err)
	}

	// Create buffer pool manager
	bufferPool := buffermanager.NewBufferPoolManager(10, diskMgr)

	// Create lock manager
	lockMgr := transaction_manager.NewLockManager()

	// Create WAL manager
	walMgr, err := logmanager.NewWALManager(walFile.Name())
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}

	// Allocate first page for the disk (meta)
	diskMgr.AllocatePage()

	// Create BTree
	tree, err := NewBTree(bufferPool, lockMgr, walMgr, pageSize)
	if err != nil {
		t.Fatalf("failed to create BTree: %v", err)
	}

	// Create transaction manager
	txnMgr := transaction_manager.NewTransactionManager(walMgr)

	t.Cleanup(func() {
		walMgr.Close()
		diskMgr.Close()
	})

	return tree, txnMgr, bufferPool
}

func TestNewBTree_Empty(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	if tree.meta.RootPage() != 1 {
		t.Fatalf("expected root page ID 1, got %d", tree.meta.RootPage())
	}

	// Just verify the tree was initialized properly
	if tree.bufferPool == nil {
		t.Fatalf("bufferPool not initialized")
	}
	if tree.lockManager == nil {
		t.Fatalf("lockManager not initialized")
	}
	if tree.logManager == nil {
		t.Fatalf("walMgr not initialized")
	}

	_ = txnMgr
}

func TestNewBTree_Existing(t *testing.T) {
	tree1, txnMgr, bufferPool := setupBTreeComponents(t, 4096)

	if tree1.meta.RootPage() != 1 {
		t.Fatalf("expected root page ID 1, got %d", tree1.meta.RootPage())
	}

	// Create a new lock manager and WAL manager for the second tree instance
	lockMgr := transaction_manager.NewLockManager()

	// Create temporary WAL file
	walFile2, err := os.CreateTemp("", "omag-test2-*.wal")
	if err != nil {
		t.Fatalf("failed to create temp wal file: %v", err)
	}
	walFile2.Close()
	t.Cleanup(func() { os.Remove(walFile2.Name()) })

	walMgr2, err := logmanager.NewWALManager(walFile2.Name())
	if err != nil {
		t.Fatalf("failed to create WAL manager: %v", err)
	}

	// Use the same buffer pool with new lock and WAL managers
	tree2, err := NewBTree(bufferPool, lockMgr, walMgr2, 4096)
	if err != nil {
		t.Fatalf("unexpected error reopening: %v", err)
	}
	if tree2.meta.RootPage() != 1 {
		t.Fatalf("expected root page ID 1, got %d", tree2.meta.RootPage())
	}

	_ = txnMgr
}

func TestBTree_InsertAndFind(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	err := tree.Insert(txn, []byte("keyA"), []byte("valA"))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	err = tree.Insert(txn, []byte("keyB"), []byte("valB"))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	val, err := tree.Find(txn, []byte("keyA"))
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if !bytes.Equal(val, []byte("valA")) {
		t.Fatalf("expected 'valA', got '%s'", val)
	}

	val, err = tree.Find(txn, []byte("keyB"))
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if !bytes.Equal(val, []byte("valB")) {
		t.Fatalf("expected 'valB', got '%s'", val)
	}

	txnMgr.Commit(txn)
}

func TestBTree_Find_NotFound(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	tree.Insert(txn, []byte("key1"), []byte("val1"))

	_, err := tree.Find(txn, []byte("key2"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	txnMgr.Commit(txn)
}

func TestBTree_Delete(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)

	txn := txnMgr.Begin()

	// Insert a value
	err := tree.Insert(txn, []byte("key1"), []byte("val1"))
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Find it
	val, err := tree.Find(txn, []byte("key1"))
	if err != nil {
		t.Fatalf("find failed: %v", err)
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Fatalf("expected 'val1', got '%s'", val)
	}

	// Delete it
	err = tree.Delete(txn, []byte("key1"))
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Try to find it again - should fail
	_, err = tree.Find(txn, []byte("key1"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound after delete, got %v", err)
	}

	txnMgr.Commit(txn)
}
