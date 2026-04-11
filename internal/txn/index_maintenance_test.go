package txn

import (
	"encoding/json"
	"testing"

	"github.com/rodrigo0345/omag/internal/storage/schema"
)

func TestIndexValueExtraction(t *testing.T) {
	tableSchema := schema.NewTableSchema("users", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("email", schema.DataTypeString, false)
	tableSchema.AddColumn("age", schema.DataTypeInt64, true)
	tableSchema.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)
	tableSchema.AddIndex("age_idx", schema.IndexTypeSecondary, []string{"age"}, false)

	rowData := map[string]interface{}{
		"id":    int64(1),
		"email": "alice@example.com",
		"age":   int64(30),
	}
	rowJSON, _ := json.Marshal(rowData)

	indexValues, err := ExtractIndexValues(tableSchema, rowJSON)
	if err != nil {
		t.Fatalf("failed to extract index values: %v", err)
	}

	if len(indexValues) != 2 {
		t.Errorf("expected 2 index values, got %d", len(indexValues))
	}

	if _, exists := indexValues["email_idx"]; !exists {
		t.Error("expected email_idx in extracted values")
	}

	if _, exists := indexValues["age_idx"]; !exists {
		t.Error("expected age_idx in extracted values")
	}
}

func TestMultipleIndexesSchema(t *testing.T) {
	tableSchema := schema.NewTableSchema("users", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("email", schema.DataTypeString, false)
	tableSchema.AddColumn("username", schema.DataTypeString, false)
	tableSchema.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)
	tableSchema.AddIndex("username_idx", schema.IndexTypeSecondary, []string{"username"}, false)

	if len(tableSchema.Indexes) != 2 {
		t.Errorf("expected 2 secondary indexes, got %d", len(tableSchema.Indexes))
	}
}

func TestTransactionWithTableContext(t *testing.T) {
	tableSchema := schema.NewTableSchema("users", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("email", schema.DataTypeString, false)
	tableSchema.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)

	txn := NewTransaction(1, READ_COMMITTED)
	txn.SetTableContext("users", tableSchema)

	tableName, retrievedSchema := txn.GetTableContext()
	if retrievedSchema == nil {
		t.Error("expected table context to be retrievable")
	}

	if tableName != "users" {
		t.Errorf("expected table name 'users', got %s", tableName)
	}

	if len(retrievedSchema.Indexes) != 1 {
		t.Errorf("expected 1 secondary index in context, got %d", len(retrievedSchema.Indexes))
	}
}

func TestCompoundIndexExtraction(t *testing.T) {
	tableSchema := schema.NewTableSchema("orders", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("customer_id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("status", schema.DataTypeString, false)
	tableSchema.AddIndex("customer_status_idx", schema.IndexTypeSecondary, []string{"customer_id", "status"}, false)

	rowData := map[string]interface{}{
		"id":          int64(100),
		"customer_id": int64(5),
		"status":      "active",
	}
	rowJSON, _ := json.Marshal(rowData)

	indexValues, err := ExtractIndexValues(tableSchema, rowJSON)
	if err != nil {
		t.Fatalf("failed to extract compound index values: %v", err)
	}

	if len(indexValues) != 1 {
		t.Errorf("expected 1 compound index, got %d", len(indexValues))
	}
}

func TestTransactionCleanupCallbacks(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	callbackRan := false
	txn.RegisterCleanupCallback(func() error {
		callbackRan = true
		return nil
	})

	if len(txn.cleanupCallbacks) != 1 {
		t.Errorf("expected 1 cleanup callback, got %d", len(txn.cleanupCallbacks))
	}

	err := txn.ExecuteCleanupCallbacks()
	if err != nil {
		t.Errorf("cleanup callbacks should execute without error, got %v", err)
	}

	if !callbackRan {
		t.Error("expected cleanup callback to execute")
	}
}

func TestTransactionStateManagement(t *testing.T) {
	txn := NewTransaction(1, READ_COMMITTED)

	if txn.GetState() != ACTIVE {
		t.Errorf("expected ACTIVE state, got %v", txn.GetState())
	}

	txn.Commit()
	if txn.GetState() != COMMITTED {
		t.Errorf("expected COMMITTED state, got %v", txn.GetState())
	}

	txn2 := NewTransaction(2, READ_UNCOMMITTED)
	txn2.Abort()
	if txn2.GetState() != ABORTED {
		t.Errorf("expected ABORTED state, got %v", txn2.GetState())
	}
}

func TestIndexConflictDetection(t *testing.T) {
	tableSchema := schema.NewTableSchema("users", "id")
	tableSchema.AddColumn("id", schema.DataTypeInt64, false)
	tableSchema.AddColumn("email", schema.DataTypeString, false)
	tableSchema.AddIndex("email_idx", schema.IndexTypeSecondary, []string{"email"}, false)

	txn1 := NewTransaction(1, SERIALIZABLE)
	txn1.SetTableContext("users", tableSchema)

	txn2 := NewTransaction(2, SERIALIZABLE)
	txn2.SetTableContext("users", tableSchema)

	name1, schema1 := txn1.GetTableContext()
	name2, schema2 := txn2.GetTableContext()

	if name1 != "users" || name2 != "users" {
		t.Fatal("expected table names to be 'users'")
	}

	if schema1 == nil || schema2 == nil {
		t.Fatal("expected table schemas to be set")
	}

	if len(schema1.Indexes) != len(schema2.Indexes) {
		t.Errorf("expected same number of indexes, got %d and %d", len(schema1.Indexes), len(schema2.Indexes))
	}
}

func TestTransactionIsolationLevels(t *testing.T) {
	tests := []struct {
		level uint8
		name  string
	}{
		{READ_UNCOMMITTED, "READ_UNCOMMITTED"},
		{READ_COMMITTED, "READ_COMMITTED"},
		{REPEATABLE_READ, "REPEATABLE_READ"},
		{SERIALIZABLE, "SERIALIZABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := NewTransaction(1, tt.level)
			if txn.GetIsolationLevel() != tt.level {
				t.Errorf("expected isolation level %d, got %d", tt.level, txn.GetIsolationLevel())
			}
		})
	}
}
