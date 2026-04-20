package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

type SchemaJSON struct {
	Name       string                `json:"name"`
	Columns    map[string]ColumnJSON `json:"columns"`
	ColumnList []string              `json:"columnList"`
	PrimaryKey string                `json:"primaryKey"`
	Indexes    map[string]IndexJSON  `json:"indexes"`
	CreatedAt  int64                 `json:"createdAt"`
	ModifiedAt int64                 `json:"modifiedAt"`
}

type ColumnJSON struct {
	Name     string `json:"name"`
	DataType string `json:"dataType"`
	Nullable bool   `json:"nullable"`
}

type IndexJSON struct {
	Name     string   `json:"name"`
	Type     string   `json:"type"`
	Columns  []string `json:"columns"`
	IsUnique bool     `json:"isUnique"`
	IsSparse bool     `json:"isSparse"`
}

func (ts *TableSchema) ToJSON() ([]byte, error) {
	if err := ts.Validate(); err != nil {
		return nil, fmt.Errorf("schema validation failed: %w", err)
	}

	colsJSON := make(map[string]ColumnJSON, len(ts.Columns))
	for name, col := range ts.Columns {
		colsJSON[name] = ColumnJSON{
			Name:     col.Name,
			DataType: string(col.DataType),
			Nullable: col.Nullable,
		}
	}

	idxsJSON := make(map[string]IndexJSON, len(ts.Indexes))
	for name, idx := range ts.Indexes {
		idxsJSON[name] = IndexJSON{
			Name:     idx.Name,
			Type:     string(idx.Type),
			Columns:  idx.Columns,
			IsUnique: idx.IsUnique,
			IsSparse: idx.IsSparse,
		}
	}

	schemaJSON := SchemaJSON{
		Name:       ts.Name,
		Columns:    colsJSON,
		ColumnList: ts.ColumnList,
		PrimaryKey: ts.PrimaryKey,
		Indexes:    idxsJSON,
		CreatedAt:  ts.CreatedAt,
		ModifiedAt: ts.ModifiedAt,
	}

	return json.Marshal(schemaJSON)
}

func FromJSON(data []byte) (*TableSchema, error) {
	var schemaJSON SchemaJSON
	if err := json.Unmarshal(data, &schemaJSON); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema JSON: %w", err)
	}

	ts := NewTableSchema(schemaJSON.Name, schemaJSON.PrimaryKey)
	ts.Columns = make(map[string]*Column, len(schemaJSON.Columns))
	ts.ColumnList = make([]string, 0, len(schemaJSON.ColumnList))
	ts.Indexes = make(map[string]*Index, len(schemaJSON.Indexes))
	ts.CreatedAt = schemaJSON.CreatedAt
	ts.ModifiedAt = schemaJSON.ModifiedAt

	for _, colName := range schemaJSON.ColumnList {
		colJSON, exists := schemaJSON.Columns[colName]
		if !exists {
			return nil, fmt.Errorf("column %q in columnList does not exist", colName)
		}

		if err := ts.AddColumn(colName, DataType(colJSON.DataType), colJSON.Nullable); err != nil {
			return nil, err
		}
	}

	for idxName, idxJSON := range schemaJSON.Indexes {
		isUnique := idxJSON.IsUnique
		if idxJSON.Type == "primary" {
			isUnique = false
		}

		if err := ts.AddIndex(idxName, IndexType(idxJSON.Type), idxJSON.Columns, isUnique); err != nil {
			return nil, err
		}
	}

	return ts, nil
}

func (ts *TableSchema) String() string {
	var b strings.Builder
	b.Grow(len(ts.ColumnList)*32 + len(ts.Indexes)*24 + 64)
	b.WriteString("Table: ")
	b.WriteString(ts.Name)
	b.WriteString("\nPrimary Key: ")
	b.WriteString(ts.PrimaryKey)
	b.WriteString("\nColumns:\n")
	for _, colName := range ts.ColumnList {
		col := ts.Columns[colName]
		b.WriteString("  ")
		b.WriteString(col.Name)
		b.WriteString(": ")
		b.WriteString(string(col.DataType))
		if col.Nullable {
			b.WriteString(" (nullable)")
		}
		b.WriteByte('\n')
	}
	if len(ts.Indexes) > 0 {
		b.WriteString("Indexes:\n")
		for _, idx := range ts.Indexes {
			b.WriteString("  ")
			b.WriteString(idx.Name)
			b.WriteString(" (")
			b.WriteString(string(idx.Type))
			b.WriteString(") on columns: [")
			b.WriteString(strings.Join(idx.Columns, " "))
			b.WriteString("]\n")
		}
	}
	return b.String()
}
