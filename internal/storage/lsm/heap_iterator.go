package lsm

import "bytes"

type iterHeap struct {
	iters   []*sstableIter
	reverse bool // Controls heap direction
}

func (h *iterHeap) Len() int      { return len(h.iters) }
func (h *iterHeap) Swap(i, j int) { h.iters[i], h.iters[j] = h.iters[j], h.iters[i] }

func (h *iterHeap) Less(i, j int) bool {
	// Ensure we are comparing bytes directly
	ki, kj := h.iters[i].key(), h.iters[j].key()
	cmp := bytes.Compare([]byte(ki), []byte(kj))

	if h.reverse {
		if cmp != 0 {
			return cmp > 0 // Max-Heap for Reverse
		}
		// If keys/timestamps are identical, youngest component (highest priority) wins
		return h.iters[i].priority > h.iters[j].priority
	}

	if cmp != 0 {
		return cmp < 0 // Min-Heap for Forward
	}
	// Youngest component wins tie-break
	return h.iters[i].priority > h.iters[j].priority
}

func (h *iterHeap) Push(x any) {
	h.iters = append(h.iters, x.(*sstableIter))
}

func (h *iterHeap) Pop() any {
	old := h.iters
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	h.iters = old[:n-1]
	return it
}
