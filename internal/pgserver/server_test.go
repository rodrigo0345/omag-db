package pgserver

import (
	"path/filepath"
	"testing"

	"github.com/jackc/pgproto3/v2"
	"github.com/rodrigo0345/omag/internal/database"
)

func TestServerExecuteSimpleRoundTrip(t *testing.T) {
	tmp := t.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	if _, err := srv.executeStatement(sess, "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "INSERT INTO users (id, name) VALUES ('user-1', 'Ada')"); err != nil {
		t.Fatalf("INSERT error = %v", err)
	}

	res, err := srv.executeStatement(sess, "SELECT id, name FROM users WHERE id = 'user-1'")
	if err != nil {
		t.Fatalf("SELECT error = %v", err)
	}
	if len(res.messages) != 3 {
		t.Fatalf("expected 3 messages (row description, row, completion), got %d", len(res.messages))
	}

	if _, ok := res.messages[0].(*pgproto3.RowDescription); !ok {
		t.Fatalf("expected first message to be RowDescription, got %T", res.messages[0])
	}
	dr, ok := res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected second message to be DataRow, got %T", res.messages[1])
	}
	if len(dr.Values) != 2 || string(dr.Values[0]) != "user-1" || string(dr.Values[1]) != "Ada" {
		t.Fatalf("unexpected row values: %#v", dr.Values)
	}

	if _, err := srv.executeStatement(sess, "UPDATE users SET name = 'Bea' WHERE id = 'user-1'"); err != nil {
		t.Fatalf("UPDATE error = %v", err)
	}
	res, err = srv.executeStatement(sess, "SELECT name FROM users WHERE id = 'user-1'")
	if err != nil {
		t.Fatalf("SELECT after update error = %v", err)
	}
	dr, ok = res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected DataRow after update, got %T", res.messages[1])
	}
	if string(dr.Values[0]) != "Bea" {
		t.Fatalf("expected updated name Bea, got %q", string(dr.Values[0]))
	}

	if _, err := srv.executeStatement(sess, "DELETE FROM users WHERE id = 'user-1'"); err != nil {
		t.Fatalf("DELETE error = %v", err)
	}
	res, err = srv.executeStatement(sess, "SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT after delete error = %v", err)
	}
	if cc, ok := res.messages[len(res.messages)-1].(*pgproto3.CommandComplete); !ok || string(cc.CommandTag) != "SELECT 0" {
		t.Fatalf("expected SELECT 0 command complete, got %T %#v", res.messages[len(res.messages)-1], res.messages[len(res.messages)-1])
	}
}

func TestServerHandlesTypedOrderingAndConstraints(t *testing.T) {
	tmp := t.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	})
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	defer func() { _ = engine.Close() }()

	srv := New(engine)
	sess := &connSession{status: 'I'}

	if _, err := srv.executeStatement(sess, "CREATE TABLE metrics (id INT PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "INSERT INTO metrics (id, value) VALUES (10, 'ten')"); err != nil {
		t.Fatalf("INSERT 10 error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "INSERT INTO metrics (id, value) VALUES (2, 'two')"); err != nil {
		t.Fatalf("INSERT 2 error = %v", err)
	}

	if _, err := srv.executeStatement(sess, "INSERT INTO metrics (id, value) VALUES (2, 'duplicate')"); err == nil {
		t.Fatalf("expected duplicate primary key insert to fail")
	}

	res, err := srv.executeStatement(sess, "SELECT id FROM metrics")
	if err != nil {
		t.Fatalf("SELECT error = %v", err)
	}
	if len(res.messages) != 4 {
		t.Fatalf("expected 4 messages for 2-row select, got %d", len(res.messages))
	}
	dr1, ok := res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected first data row, got %T", res.messages[1])
	}
	dr2, ok := res.messages[2].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected second data row, got %T", res.messages[2])
	}
	if string(dr1.Values[0]) != "2" || string(dr2.Values[0]) != "10" {
		t.Fatalf("expected numeric ordering 2 then 10, got %q then %q", string(dr1.Values[0]), string(dr2.Values[0]))
	}

	res, err = srv.executeStatement(sess, "SHOW DateStyle")
	if err != nil {
		t.Fatalf("SHOW DateStyle error = %v", err)
	}
	dr, ok := res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected SHOW to return a data row, got %T", res.messages[1])
	}
	if string(dr.Values[0]) != "ISO, MDY" {
		t.Fatalf("expected DateStyle value ISO, MDY, got %q", string(dr.Values[0]))
	}

	if _, err := srv.executeStatement(sess, "UPDATE metrics SET id = 3 WHERE id = 2"); err == nil {
		t.Fatalf("expected primary key update to fail")
	}
}

func TestServerPersistsCommittedDataAcrossRestart(t *testing.T) {
	tmp := t.TempDir()

	opts := database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   8,
		ReplacerCapacity: 4,
	}

	engine, err := database.OpenMVCCLSM(opts)
	if err != nil {
		t.Fatalf("OpenMVCCLSM() error = %v", err)
	}

	srv := New(engine)
	sess := &connSession{status: 'I'}

	if _, err := srv.executeStatement(sess, "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "BEGIN"); err != nil {
		t.Fatalf("BEGIN error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "INSERT INTO users (id, name) VALUES ('user-1', 'Grace')"); err != nil {
		t.Fatalf("INSERT inside transaction error = %v", err)
	}
	if _, err := srv.executeStatement(sess, "COMMIT"); err != nil {
		t.Fatalf("COMMIT error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() first engine error = %v", err)
	}

	engine2, err := database.OpenMVCCLSM(opts)
	if err != nil {
		t.Fatalf("OpenMVCCLSM() restart error = %v", err)
	}
	defer func() { _ = engine2.Close() }()

	srv2 := New(engine2)
	sess2 := &connSession{status: 'I'}
	res, err := srv2.executeStatement(sess2, "SELECT id, name FROM users WHERE id = 'user-1'")
	if err != nil {
		t.Fatalf("SELECT after restart error = %v", err)
	}
	if len(res.messages) < 3 {
		t.Fatalf("expected select response with data row, got %d messages", len(res.messages))
	}
	dr, ok := res.messages[1].(*pgproto3.DataRow)
	if !ok {
		t.Fatalf("expected DataRow after restart, got %T", res.messages[1])
	}
	if string(dr.Values[0]) != "user-1" || string(dr.Values[1]) != "Grace" {
		t.Fatalf("unexpected recovered row: %#v", dr.Values)
	}
}

