package database

import (
	"path/filepath"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

func TestOpenMVCCLSM(t *testing.T) {
	tmp := t.TempDir()

	engine, err := OpenMVCCLSM(Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	if engine == nil {
		t.Fatal("OpenMVCCLSM() returned nil engine")
	}
	if engine.IsolationManager() == nil {
		t.Fatal("expected isolation manager")
	}
	if engine.StorageEngine() == nil {
		t.Fatal("expected storage engine")
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestEngineCreateTableAndTxnRoundTrip(t *testing.T) {
	tmp := t.TempDir()

	engine, err := OpenMVCCLSM(Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer engine.Close()

	ts := schema.NewTableSchema("users", "id")
	if err := ts.AddColumn("id", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn() error = %v", err)
	}
	if err := ts.AddColumn("name", schema.DataTypeString, false); err != nil {
		t.Fatalf("AddColumn() error = %v", err)
	}

	if err := engine.CreateTable(ts); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	txnID := engine.BeginTransaction(txn_unit.SERIALIZABLE, "users", ts)
	if err := engine.Write(txnID, []byte("user-1"), []byte(`{"id":"user-1","name":"Ada"}`)); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := engine.Commit(txnID); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	readTxn := engine.BeginTransaction(txn_unit.READ_COMMITTED, "users", ts)
	value, err := engine.Read(readTxn, []byte("user-1"))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(value) != `{"id":"user-1","name":"Ada"}` {
		t.Fatalf("unexpected value: %s", string(value))
	}
	if err := engine.Commit(readTxn); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}
