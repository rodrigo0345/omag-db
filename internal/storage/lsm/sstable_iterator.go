package lsm

type sstableIter struct {
	rows     []sstableRow
	pos      int
	priority int
}

func newSSTableIter(ss *SSTable, priority int) *sstableIter {
	if len(ss.sortedRows) != len(ss.data) {
		// opt: build the contiguous row view once; scans then walk linear memory.
		ss.rebuildSortedRows()
	}

	return &sstableIter{
		rows:     ss.sortedRows,
		pos:      -1,
		priority: priority,
	}
}

func (it *sstableIter) next() bool {
	it.pos++
	return it.pos < len(it.rows)
}

func (it *sstableIter) prev() bool {
	it.pos--
	return it.pos >= 0
}

func (it *sstableIter) key() string { return it.rows[it.pos].key }
func (it *sstableIter) val() []byte { return it.rows[it.pos].value }

func (it *sstableIter) IsTombstoned() bool {
	if it.pos < 0 || it.pos >= len(it.rows) {
		return false
	}
	return it.rows[it.pos].tombstone
}
