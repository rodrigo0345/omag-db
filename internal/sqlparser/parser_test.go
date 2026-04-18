package sqlparser

import "testing"

func TestParseSelectTables(t *testing.T) {
	stmt, err := Parse("SELECT t.name, u.email FROM tests t, users u WHERE t.id = u.test_id;")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}

	if selectStmt.Kind() != StatementSelect {
		t.Fatalf("unexpected kind: %s", selectStmt.Kind())
	}

	tables := selectStmt.Tables()
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d (%v)", len(tables), tables)
	}
	if tables[0] != "tests" || tables[1] != "users" {
		t.Fatalf("unexpected tables: %v", tables)
	}

	if len(selectStmt.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(selectStmt.Columns))
	}
}

func TestParseCreateIndex(t *testing.T) {
	stmt, err := Parse("CREATE INDEX email_idx ON users (email)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	idxStmt, ok := stmt.(*CreateIndexStatement)
	if !ok {
		t.Fatalf("expected *CreateIndexStatement, got %T", stmt)
	}

	if idxStmt.Index != "email_idx" {
		t.Fatalf("expected index email_idx, got %s", idxStmt.Index)
	}
	if idxStmt.Table != "users" {
		t.Fatalf("expected table users, got %s", idxStmt.Table)
	}
}
