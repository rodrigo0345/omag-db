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

	for c.h.Len() > 0 {
		// If opts.Reverse is true, the heap is a Max-Heap (returns largest key).
		// If opts.Reverse is false, the heap is a Min-Heap (returns smallest key).
		it := heap.Pop(c.h).(*sstableIter)
		key := it.key()

		if c.opts.Reverse {
			// In Reverse, we stop if we go BELOW the LowerBound
			if len(c.opts.LowerBound) > 0 && bytes.Compare([]byte(key), c.opts.LowerBound) < 0 {
				c.Close()
				return false
			}
		} else {
			// In Forward, we stop if we go ABOVE the UpperBound
			if len(c.opts.UpperBound) > 0 {
				cmp := bytes.Compare([]byte(key), c.opts.UpperBound)
				if cmp > 0 || (cmp == 0 && !c.opts.Inclusive) {
					c.Close()
					return false
				}
			}
		}

		// We return EVERY version found.
		c.currentEntry = storage.ScanEntry{
			Key:   []byte(key),
			Value: append([]byte(nil), it.val()...),
		}

		// Advance the iterator in the correct direction
		advanced := false
		if c.opts.Reverse {
			advanced = it.prev()
		} else {
			advanced = it.next()
		}

		if advanced {
			heap.Push(c.h, it)
		}

		if c.skipped < c.opts.Offset {
			c.skipped++
			continue
		}

		c.count++
		if c.opts.Limit > 0 && c.count > c.opts.Limit {
			c.Close()
			return false
		}

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
