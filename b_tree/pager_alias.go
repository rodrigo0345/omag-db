package btree

import "github.com/rodrigo0345/omag/buffermanager"

// Keep the btree package API stable while the pager lives in buffermanager.
type Pager = buffermanager.Pager
type BufferManager = buffermanager.Pager

func NewPager(filePath string, pageSize uint32, inMemory bool) (*Pager, error) {
	return buffermanager.NewPager(filePath, pageSize, inMemory)
}
