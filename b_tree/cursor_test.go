package btree

import (
	"bytes"
	"fmt"
	"testing"
)

func TestCursor_NextEmpty(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)
	txn := txnMgr.Begin()

	cursor, _ := tree.Cursor()
	err := cursor.First()
	if err != nil {
		t.Fatalf("first failed: %v", err)
	}

	if cursor.Valid() {
		t.Fatalf("cursor should not be valid on empty tree")
	}
	txnMgr.Commit(txn)
}

func TestCursor_FirstNextScan(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 4096)
	txn := txnMgr.Begin()

	numEntries := 50
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		val := []byte(fmt.Sprintf("val%03d", i))
		tree.Insert(txn, key, val)
	}

	cursor, _ := tree.Cursor()
	err := cursor.First()
	if err != nil {
		t.Fatalf("first failed: %v", err)
	}

	count := 0
	for ; cursor.Valid(); cursor.Next() {
		expectedKey := []byte(fmt.Sprintf("key%03d", count))
		if !bytes.Equal(cursor.Key(), expectedKey) {
			t.Fatalf("expected key %s, got %s", expectedKey, cursor.Key())
		}
		count++
	}

	if count != numEntries {
		t.Fatalf("expected %d entries, found %d", numEntries, count)
	}
	txnMgr.Commit(txn)
}

func TestCursor_Seek(t *testing.T) {
	tree, txnMgr, _ := setupBTreeComponents(t, 512)
	txn := txnMgr.Begin()

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%02d", i*2)) // key00, key02, key04
		val := []byte(fmt.Sprintf("val%02d", i*2))
		tree.Insert(txn, key, val)
	}

	cursor, _ := tree.Cursor()

	// Seek exact match
	err := cursor.Seek([]byte("key10"))
	if err != nil {
		t.Fatalf("seek failed: %v", err)
	}
	if !cursor.Valid() || string(cursor.Key()) != "key10" {
		t.Fatalf("seek key10 failed, found %s", cursor.Key())
	}

	// Seek immediately after
	err = cursor.Seek([]byte("key11"))
	if err != nil {
		t.Fatalf("seek failed: %v", err)
	}
	if !cursor.Valid() || string(cursor.Key()) != "key12" {
		t.Fatalf("seek key11 failed, found %s", cursor.Key())
	}

	// Seek beyond end
	err = cursor.Seek([]byte("key99"))
	if err != nil {
		t.Fatalf("seek failed: %v", err)
	}
	if cursor.Valid() {
		t.Fatalf("seek key99 should be invalid, but found %s", cursor.Key())
	}
	txnMgr.Commit(txn)
}
