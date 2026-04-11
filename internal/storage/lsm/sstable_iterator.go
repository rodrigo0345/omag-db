package lsm

import "sort"

type sstableIter struct {
	keys       []string
	vals       [][]byte
	tombstones map[string]bool // Tombstone flags from SSTable
	pos        int
	priority   int // index in the allTables slice; higher = newer
}

func newSSTableIter(ss *SSTable, priority int) *sstableIter {
	keys := make([]string, 0, len(ss.data))
	for k := range ss.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	vals := make([][]byte, len(keys))
	for i, k := range keys {
		vals[i] = ss.data[k]
	}

	return &sstableIter{
		keys:       keys,
		vals:       vals,
		tombstones: ss.tombstones,
		pos:        -1, // next() moves to 0
		priority:   priority,
	}
}

func (it *sstableIter) next() bool {
	it.pos++
	return it.pos < len(it.keys)
}

func (it *sstableIter) key() string { return it.keys[it.pos] }
func (it *sstableIter) val() []byte { return it.vals[it.pos] }

func (it *sstableIter) IsTombstoned() bool {
	if it.pos < 0 || it.pos >= len(it.keys) {
		return false
	}
	return it.tombstones[it.keys[it.pos]]
}
