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
	GetName() string
	GetPrimaryKey() []string
	GetColumns() map[string]string
	GetIndexes() map[string][]string
	GetIndexedStorage(column []string) storage.IStorageEngine // can return nil if the column is not indexed
	SetIndexedStorage(column []string, engine storage.IStorageEngine)
	GetAllStorages() []storage.IStorageEngine
	ToJSON() ([]byte, error)
}

type ITableManager interface {
	CreateTable(schema ITableSchema, errorOnExists bool) error
	DropTable(tableName string) error
	CreateIndex(tableName string, index IndexDefinition, indexStorage storage.IStorageEngine) error
	GetTableSchema(tableName string) (ITableSchema, error)
}
