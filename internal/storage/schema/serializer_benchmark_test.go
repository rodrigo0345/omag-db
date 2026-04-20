package schema

import (
	"fmt"
	"testing"
)

func benchmarkSchema(colCount, idxCount int) *TableSchema {
	ts := NewTableSchema("bench_table", "id")
	_ = ts.AddColumn("id", DataTypeInt64, false)

	for i := 0; i < colCount-1; i++ {
		name := fmt.Sprintf("col_%03d", i)
		dt := DataTypeString
		if i%3 == 0 {
			dt = DataTypeInt64
		}
		_ = ts.AddColumn(name, dt, i%2 == 0)
	}

	_ = ts.AddIndex("id_pk", IndexTypePrimary, []string{"id"}, false)
	for i := 0; i < idxCount; i++ {
		name := fmt.Sprintf("idx_%03d", i)
		col := fmt.Sprintf("col_%03d", i%(colCount-1))
		_ = ts.AddIndex(name, IndexTypeSecondary, []string{col}, i%4 == 0)
	}
	return ts
}

func BenchmarkTableSchemaToJSON_Medium(b *testing.B) {
	ts := benchmarkSchema(32, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := ts.ToJSON()
		if err != nil {
			b.Fatalf("ToJSON() error = %v", err)
		}
		if len(data) == 0 {
			b.Fatalf("ToJSON() returned empty payload")
		}
	}
}

func BenchmarkFromJSON_Medium(b *testing.B) {
	ts := benchmarkSchema(32, 8)
	payload, err := ts.ToJSON()
	if err != nil {
		b.Fatalf("setup ToJSON() error = %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := FromJSON(payload)
		if err != nil {
			b.Fatalf("FromJSON() error = %v", err)
		}
		if got.PrimaryKey != "id" {
			b.Fatalf("unexpected primary key %q", got.PrimaryKey)
		}
	}
}

func BenchmarkTableSchemaString_Medium(b *testing.B) {
	ts := benchmarkSchema(32, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := ts.String()
		if len(s) == 0 {
			b.Fatalf("String() returned empty output")
		}
	}
}
