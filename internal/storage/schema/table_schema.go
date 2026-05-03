package schema

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/rodrigo0345/omag/internal/storage"
)

type DataType uint8

const TRANSACTIONAL_OP = "_txn_op"

const (
	TypeMetadata DataType = iota // Internal 1-byte column for transactional OpCodes
	TypeInt32
	TypeInt64
	TypeFloat64
	TypeBool
	TypeString
)

type Column struct {
	Name   string
	Type   DataType
	MaxLen uint32
}

type Index struct {
	Name    string
	Columns []string
	Engine  storage.IStorageEngine
}

type TableSchema struct {
	Name    string
	Columns []Column
	Indexes map[string]*Index
}

func NewTableSchema(name string, columns []Column) *TableSchema {

	actualColumns := append([]Column{
		{Name: TRANSACTIONAL_OP, Type: TypeMetadata},
	}, columns...)

	return &TableSchema{
		Name:    name,
		Columns: actualColumns,
		Indexes: make(map[string]*Index),
	}
}

func (ts *TableSchema) GetName() string {
	return ts.Name
}

func (ts *TableSchema) GetColumns() []Column {
	if len(ts.Columns) <= 1 {
		return nil
	}
	return ts.Columns[1:]
}

func (ts *TableSchema) AddIndex(name string, columns []string, engine storage.IStorageEngine) {
	ts.Indexes[name] = &Index{
		Name:    name,
		Columns: columns,
		Engine:  engine,
	}
}

func (ts *TableSchema) GetIndex(name string) *Index {
	return ts.Indexes[name]
}

func (ts *TableSchema) GetAllIndexes() []*Index {
	indexes := make([]*Index, 0, len(ts.Indexes))
	for _, idx := range ts.Indexes {
		indexes = append(indexes, idx)
	}
	return indexes
}

func (ts *TableSchema) GetIndexesByColumn(columnName string) []*Index {
	var matched []*Index
	for _, idx := range ts.Indexes {
		if len(idx.Columns) > 0 && idx.Columns[0] == columnName {
			matched = append(matched, idx)
		}
	}
	return matched
}

func (ts *TableSchema) ToJSON() ([]byte, error) {
	idxMap := make(map[string][]string)
	for name, idx := range ts.Indexes {
		idxMap[name] = idx.Columns
	}

	return json.Marshal(map[string]interface{}{
		"name":    ts.Name,
		"columns": ts.Columns,
		"indexes": idxMap,
	})
}

type RowFormatError struct {
	ColumnName string
	Issue      string
	Detail     string
}

func (e *RowFormatError) Error() string {
	if e.ColumnName == "" {
		return fmt.Sprintf("row format error: %s %s", e.Issue, e.Detail)
	}
	return fmt.Sprintf("invalid value for column '%s': %s %s", e.ColumnName, e.Issue, e.Detail)
}

func MinRowSize(columns []Column) int {
	size := 0
	for _, col := range columns {
		switch col.Type {
		case TypeInt32:
			size += 4
		case TypeInt64, TypeFloat64:
			size += 8
		case TypeBool, TypeMetadata:
			size += 1
		case TypeString:
			size += 4
		}
	}
	return size
}

func (ts *TableSchema) ExtractIndexValues(value []byte) (map[string][]byte, error) {
	err := ValidateRowFormat(ts.Columns, value)
	if err != nil {
		return nil, fmt.Errorf("%s|%s|%s", err.Issue, err.Detail, err.ColumnName)
	}

	colBytes := make(map[string][]byte, len(ts.Columns))
	offset := 0
	payloadLen := len(value)

	for i := range ts.Columns {
		col := &ts.Columns[i]
		start := offset

		switch col.Type {
		case TypeInt32:
			offset += 4
		case TypeInt64, TypeFloat64:
			offset += 8
		case TypeBool, TypeMetadata:
			offset += 1
		case TypeString:
			if offset+4 > payloadLen {
				return nil, fmt.Errorf("unexpected EOF reading string header for %q", col.Name)
			}
			strLen := int(binary.BigEndian.Uint32(value[offset : offset+4]))
			offset += 4 + strLen
		}

		if offset > payloadLen {
			return nil, fmt.Errorf("unexpected EOF reading column %q", col.Name)
		}

		colBytes[col.Name] = value[start:offset]
	}

	indexValues := make(map[string][]byte, len(ts.Indexes))
	for name, idx := range ts.Indexes {
		var keyBuf []byte
		for _, colName := range idx.Columns {
			b, ok := colBytes[colName]
			if !ok {
				// Safety check: users should not index the dummy column
				return nil, fmt.Errorf("indexed column %q not found in schema", colName)
			}
			keyBuf = append(keyBuf, b...)
		}
		indexValues[name] = keyBuf
	}

	return indexValues, nil
}

func (ts *TableSchema) GetColumnValue(columnName string, row []byte) ([]byte, error) {
	offset := 0
	payloadLen := len(row)

	for i := range ts.Columns {
		col := &ts.Columns[i]
		start := offset

		// Calculate size of current column
		var size int
		switch col.Type {
		case TypeMetadata, TypeBool:
			size = 1
		case TypeInt32:
			size = 4
		case TypeInt64, TypeFloat64:
			size = 8
		case TypeString:
			if offset+4 > payloadLen {
				return nil, fmt.Errorf("truncated string header for %s", col.Name)
			}
			strLen := int(binary.BigEndian.Uint32(row[offset : offset+4]))
			size = 4 + strLen
		}

		if offset+size > payloadLen {
			return nil, fmt.Errorf("row data too short for column %s", col.Name)
		}

		// If this is the column we want, return its slice
		if col.Name == columnName {
			return row[start : offset+size], nil
		}

		offset += size
	}

	return nil, fmt.Errorf("column %s not found in schema", columnName)
}

func ValidateRowFormat(columns []Column, value []byte) *RowFormatError {
	payloadLen := len(value)

	if payloadLen < MinRowSize(columns) {
		return &RowFormatError{Issue: "payload too short", Detail: "below minimum possible row size"}
	}

	offset := 0

	for i := range columns {
		col := &columns[i]

		switch col.Type {
		case TypeMetadata:
			// Just ensure the byte exists. We don't restrict the value
			// because it can be OpInsert (0x01) or OpDelete (0x02).
			if offset+1 > payloadLen {
				return &RowFormatError{col.Name, "missing metadata", "expected 1 byte"}
			}
			offset += 1

		case TypeInt32:
			if offset+4 > payloadLen {
				return &RowFormatError{col.Name, "missing data", "expected 4 bytes for Int32"}
			}
			offset += 4

		case TypeInt64, TypeFloat64:
			if offset+8 > payloadLen {
				return &RowFormatError{col.Name, "missing data", "expected 8 bytes"}
			}
			offset += 8

		case TypeBool:
			if offset+1 > payloadLen {
				return &RowFormatError{col.Name, "missing data", "expected 1 byte for Bool"}
			}
			if value[offset] > 1 {
				return &RowFormatError{col.Name, "corrupt boolean", "value must be 0x00 or 0x01"}
			}
			offset++

		case TypeString:
			if offset+4 > payloadLen {
				return &RowFormatError{col.Name, "missing string length header", "expected 4 bytes"}
			}
			strLen := int(binary.BigEndian.Uint32(value[offset : offset+4]))
			offset += 4

			if col.MaxLen > 0 && uint32(strLen) > col.MaxLen {
				return &RowFormatError{
					ColumnName: col.Name,
					Issue:      "string exceeds maximum length",
					Detail:     "max " + strconv.FormatUint(uint64(col.MaxLen), 10),
				}
			}
			if offset+strLen > payloadLen {
				return &RowFormatError{col.Name, "truncated string data",
					"header claims " + strconv.Itoa(strLen) + " bytes"}
			}
			offset += strLen

		default:
			return &RowFormatError{col.Name, "unsupported data type",
				"type ID " + strconv.Itoa(int(col.Type))}
		}
	}

	if offset != payloadLen {
		return &RowFormatError{
			Issue:  "trailing garbage",
			Detail: strconv.Itoa(payloadLen-offset) + " unexpected bytes",
		}
	}
	return nil
}
