package pgserver

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rodrigo0345/omag/internal/database"
)

func benchmarkServerFixture(b *testing.B, rowCount int, enableFastPath bool) (*Server, *connSession) {
	b.Helper()
	tmp := b.TempDir()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:           filepath.Join(tmp, "db.db"),
		LSMDataDir:       filepath.Join(tmp, "lsm"),
		WALPath:          filepath.Join(tmp, "wal.log"),
		BufferPoolSize:   32,
		ReplacerCapacity: 32,
	})
	if err != nil {
		b.Fatalf("OpenMVCCLSM() error = %v", err)
	}
	b.Cleanup(func() { _ = engine.Close() })

	srv := New(engine)
	srv.enablePrimaryKeyFastPath = enableFastPath
	sess := &connSession{status: 'I'}

	if _, err := srv.executeStatement(sess, "CREATE TABLE bench_users (id TEXT PRIMARY KEY, name TEXT)"); err != nil {
		b.Fatalf("CREATE TABLE error = %v", err)
	}

	const batchSize = 100
	for start := 0; start < rowCount; start += batchSize {
		end := start + batchSize
		if end > rowCount {
			end = rowCount
		}
		var sqlBatch strings.Builder
		for i := start; i < end; i++ {
			if sqlBatch.Len() > 0 {
				sqlBatch.WriteByte(';')
			}
			sqlBatch.WriteString(fmt.Sprintf("INSERT INTO bench_users (id, name) VALUES ('user-%04d', 'seed')", i))
		}
		if _, err := srv.executeStatement(sess, sqlBatch.String()); err != nil {
			b.Fatalf("seed batch error: %v", err)
		}
	}

	return srv, sess
}

func BenchmarkServerPKUpdate(b *testing.B) {
	const rowCount = 2000
	const targetID = "user-1500"

	for _, enableFastPath := range []bool{true, false} {
		name := "fastpath_off"
		if enableFastPath {
			name = "fastpath_on"
		}
		b.Run(name, func(b *testing.B) {
			srv, sess := benchmarkServerFixture(b, rowCount, enableFastPath)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stmt := fmt.Sprintf("UPDATE bench_users SET name = 'name-%d' WHERE id = '%s'", i, targetID)
				if _, err := srv.executeStatement(sess, stmt); err != nil {
					b.Fatalf("UPDATE error = %v", err)
				}
			}
		})
	}
}

func BenchmarkServerPKDelete(b *testing.B) {
	const rowCount = 2000
	const targetID = "user-1500"

	for _, enableFastPath := range []bool{true, false} {
		name := "fastpath_off"
		if enableFastPath {
			name = "fastpath_on"
		}
		b.Run(name, func(b *testing.B) {
			srv, sess := benchmarkServerFixture(b, rowCount, enableFastPath)
			deleteStmt := fmt.Sprintf("DELETE FROM bench_users WHERE id = '%s'", targetID)
			restoreStmt := fmt.Sprintf("INSERT INTO bench_users (id, name) VALUES ('%s', 'seed')", targetID)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := srv.executeStatement(sess, deleteStmt); err != nil {
					b.Fatalf("DELETE error = %v", err)
				}
				b.StopTimer()
				if _, err := srv.executeStatement(sess, restoreStmt); err != nil {
					b.Fatalf("restore INSERT error = %v", err)
				}
				b.StartTimer()
			}
		})
	}
}
