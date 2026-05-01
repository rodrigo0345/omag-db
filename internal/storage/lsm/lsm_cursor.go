package lsm

import (
	"bytes"
	"container/heap"

	"github.com/rodrigo0345/omag/internal/storage"
)

type LSMCursor struct {
	h            *iterHeap
	opts         storage.ScanOptions
	seenKeys     map[string]bool
	currentEntry storage.ScanEntry
	count        int // For Limit
	skipped      int // For Offset
	err          error
	closed       bool
}

func (c *LSMCursor) Next() bool {
	if c.closed || c.err != nil {
		return false
	}

	// If we hit the limit, stop immediately
	if c.opts.Limit > 0 && c.count >= c.opts.Limit {
		return false
	}

	for c.h.Len() > 0 {
		// Pop the iterator with the smallest key
		it := heap.Pop(c.h).(*sstableIter)
		key := it.key()

		// Check Upper Bound
		if len(c.opts.UpperBound) > 0 {
			cmp := bytes.Compare([]byte(key), c.opts.UpperBound)
			// If key > upper, or (key == upper and !Inclusive), we are done
			if cmp > 0 || (cmp == 0 && !c.opts.Inclusive) {
				c.Close()
				return false
			}
		}

		// Deduplication (LSM Shadowing)
		isNewKey := !c.seenKeys[key]
		if isNewKey {
			c.seenKeys[key] = true
		}

		// Advance the sub-iterator and put back in heap if it has more
		if it.next() {
			heap.Push(c.h, it)
		}

		// If we've seen this key already, skip it (it's an older version)
		if !isNewKey {
			continue
		}

		// Filter Tombstones
		if it.IsTombstoned() {
			continue
		}

		// Build Entry (apply KeyOnly optimization)
		entry := storage.ScanEntry{Key: []byte(key)}
		if !c.opts.KeyOnly {
			entry.Value = append([]byte(nil), it.val()...)
		}

		// Apply Complex Filter Pushdown
		if c.opts.ComplexFilter != nil && !c.opts.ComplexFilter(entry) {
			continue
		}

		// Handle Offset
		if c.skipped < c.opts.Offset {
			c.skipped++
			continue
		}

		// Success: Capture entry and increment count
		c.currentEntry = entry
		c.count++
		return true
	}

	return false
}

func (c *LSMCursor) Entry() storage.ScanEntry {
	return c.currentEntry
}

func (c *LSMCursor) Error() error {
	return c.err
}

func (c *LSMCursor) Close() error {
	c.closed = true
	c.seenKeys = nil // Release memory
	return nil
}
