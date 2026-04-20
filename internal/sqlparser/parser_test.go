package sqlparser

import (
	"testing"

	vitesssqlparser "github.com/xwb1989/sqlparser"
)

func TestParseSelectTables(t *testing.T) {
	stmt, err := Parse("SELECT t.name, u.email FROM tests t, users u WHERE t.id = u.test_id;")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	selectStmt, ok := stmt.(*vitesssqlparser.Select)
	if !ok {
		t.Fatalf("expected *sqlparser.Select, got %T", stmt)
	}

	if len(selectStmt.SelectExprs) != 2 {
		t.Fatalf("expected 2 select expressions, got %d", len(selectStmt.SelectExprs))
	}

	tables, err := ExtractTables("SELECT t.name, u.email FROM tests t, users u WHERE t.id = u.test_id;")
	if err != nil {
		t.Fatalf("ExtractTables() error = %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d (%v)", len(tables), tables)
	}
	if tables[0] != "tests" || tables[1] != "users" {
		t.Fatalf("unexpected tables: %v", tables)
	}
}

func TestParseCreateIndex(t *testing.T) {
	stmt, err := Parse("CREATE INDEX email_idx ON users (email)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	ddlStmt, ok := stmt.(*vitesssqlparser.DDL)
	if !ok {
		t.Fatalf("expected *sqlparser.DDL, got %T", stmt)
	}
	_ = ddlStmt

	tables, err := ExtractTables("CREATE INDEX email_idx ON users (email)")
	if err != nil {
		t.Fatalf("ExtractTables() error = %v", err)
	}
	if len(tables) != 1 || tables[0] != "users" {
		t.Fatalf("expected users table extraction, got %v", tables)
	}
}
