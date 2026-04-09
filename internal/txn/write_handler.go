package txn

import (
	"github.com/rodrigo0345/omag/internal/storage/page"
)

type WriteOperation struct {
	Key      []byte
	Value    []byte
	PageID   page.ResourcePageID
	Offset   uint16
	IsDelete bool
}

type WriteHandler interface {
	HandleWrite(txn *Transaction, writeOp WriteOperation) error
}
