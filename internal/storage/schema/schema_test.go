package schema

import (
	"testing"
)

func TestTableSchema(t *testing.T) {
	columns := map[string]string{
		"id":   "int",
		"name": "string",
	}
	primaryKey := []string{"id"}

	schema := NewTableSchema("users", primaryKey, columns)

	if schema.GetName() != "users" {
		t.Errorf("expected name 'users', got %s", schema.GetName())
	}

	if len(schema.GetPrimaryKey()) != 1 || schema.GetPrimaryKey()[0] != "id" {
		t.Errorf("expected primary key [id], got %v", schema.GetPrimaryKey())
	}

	if len(schema.GetColumns()) != 2 {
		t.Errorf("expected 2 columns, got %d", len(schema.GetColumns()))
	}

	schema.AddIndex("idx_name", []string{"name"})
	if len(schema.GetIndexes()) != 1 {
		t.Errorf("expected 1 index, got %d", len(schema.GetIndexes()))
	}

	data, err := schema.ToJSON()
	if err != nil {
		t.Errorf("ToJSON failed: %v", err)
	}
	if len(data) == 0 {
		t.Errorf("ToJSON returned empty data")
	}
}

func TestTableManager(t *testing.T) {
	manager := NewTableManager()

	columns := map[string]string{
		"id":    "int",
		"email": "string",
	}
	primaryKey := []string{"id"}
	schema := NewTableSchema("accounts", primaryKey, columns)

	// Test CreateTable
	err := manager.CreateTable(schema, true)
	if err != nil {
		t.Errorf("CreateTable failed: %v", err)
	}

	// Test duplicate table with errorOnExists
	err = manager.CreateTable(schema, true)
	if err == nil {
		t.Errorf("expected error for duplicate table, got nil")
	}

	// Test CreateIndex
	err = manager.CreateIndex("accounts", "idx_email", BTree, []string{"email"})
	if err != nil {
		t.Errorf("CreateIndex failed: %v", err)
	}

	// Test GetTableSchema
	retrieved, err := manager.GetTableSchema("accounts")
	if err != nil {
		t.Errorf("GetTableSchema failed: %v", err)
	}
	if retrieved.GetName() != "accounts" {
		t.Errorf("expected table name 'accounts', got %s", retrieved.GetName())
	}

	// Test DropTable
	err = manager.DropTable("accounts")
	if err != nil {
		t.Errorf("DropTable failed: %v", err)
	}

	// Test getting dropped table
	_, err = manager.GetTableSchema("accounts")
	if err == nil {
		t.Errorf("expected error for dropped table, got nil")
	}
}
