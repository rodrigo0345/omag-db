package schema

import "github.com/rodrigo0345/omag/internal/storage"

type IndexType int

const (
	BTree IndexType = iota
	Hash
	GiST
	GIN
)

type IndexDefinition struct {
	Name    string
	Type    IndexType
	Columns []string
}

type ITableSchema interface {
	AddIndex(name string, columns []string, engine storage.IStorageEngine)

	GetName() string
	GetColumns() []Column
	GetIndex(name string) *Index
	GetAllIndexes() []*Index
	GetIndexesByColumn(columnName string) []*Index
	GetColumnValue(columnName string, row []byte) ([]byte, error)

	ToJSON() ([]byte, error)
	ExtractIndexValues(value []byte) (map[string][]byte, error)
}

type WriteOperation struct {
	TableName string
	Key       []byte
	Value     []byte
}

type DeleteOperation struct {
	TableName   string
	Key         []byte
	BeforeImage []byte // The old payload required to purge secondary index entries
}

type ReadOperation struct {
	TableName string
	IndexName string
	Key       []byte
}

type ITableManager interface {
	CreateTable(schema ITableSchema, errorOnExists bool) error
	DropTable(tableName string) error
	CreateIndex(tableName string, index IndexDefinition, indexStorage storage.IStorageEngine) error
	GetTableSchema(tableName string) (ITableSchema, error)
	GetAllTables() []string

	Write(op WriteOperation) error
	Delete(op DeleteOperation) error

	Scan(tableName string, indexName string, opts storage.ScanOptions) (storage.ICursor, error)
	FullTableScan(tableName string, opts storage.ScanOptions) (storage.ICursor, error)
}
