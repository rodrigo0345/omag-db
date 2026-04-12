package testutil

import (
	"github.com/rodrigo0345/omag/internal/storage/schema"
)

func NewUserTableSchema() *schema.TableSchema {
	tbl := schema.NewTableSchema("users", "id")
	tbl.AddColumn("id", schema.DataTypeInt64, false)
	tbl.AddColumn("name", schema.DataTypeString, false)
	tbl.AddColumn("email", schema.DataTypeString, false)
	return tbl
}

func NewOrderTableSchema() *schema.TableSchema {
	tbl := schema.NewTableSchema("orders", "order_id")
	tbl.AddColumn("order_id", schema.DataTypeInt64, false)
	tbl.AddColumn("user_id", schema.DataTypeInt64, false)
	tbl.AddColumn("amount", schema.DataTypeFloat64, false)
	tbl.AddColumn("status", schema.DataTypeString, false)
	return tbl
}
