package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/page"
)

// ============================================================================
// OVERFLOW PAGE MANAGEMENT
// ============================================================================
//
// For values larger than can fit in a leaf page, overflow pages are used.
// Each overflow page stores a chunk of the value and a pointer to the next
// overflow page (if any), similar to SQLite's overflow handling.
//
// Overflow Page Layout:
// +------------------+ 0
// | Next Overflow ID | 8 bytes (0 if last page)
// | Value Data      | Remaining bytes
// +------------------+ 4096
//
// Cell Layout with Overflow:
// +------------------+
// | Key Length      | 2 bytes
// | Value Length    | 4 bytes
// | Overflow ID     | 8 bytes (0 if no overflow)
// | Value Data      | Min(Value, available space)
// +------------------+
//
// When a value is too large to fit in remaining page space after header and
// key, it's stored across multiple overflow pages.

const (
	// OverflowThreshold is the max value size stored inline in a cell
	// If value is larger, it goes to overflow pages
	OverflowThreshold = 100 // bytes - values > 100 bytes go to overflow

	// OverflowHeaderSize is the size of overflow page header
	OverflowHeaderSize = 8 // Next overflow page ID (8 bytes)

	// OverflowDataSize is how much data fits in one overflow page
	OverflowDataSize = DefaultPageSize - OverflowHeaderSize
)

// OverflowPage represents an overflow page structure
type OverflowPage struct {
	data []byte
}

// NewOverflowPage creates a new overflow page with the given data size
func NewOverflowPage(size int) *OverflowPage {
	return &OverflowPage{
		data: make([]byte, size),
	}
}

// SetNextOverflowID sets the ID of the next overflow page (0 if none)
func (op *OverflowPage) SetNextOverflowID(nextID uint64) {
	binary.LittleEndian.PutUint64(op.data[0:8], nextID)
}

// GetNextOverflowID returns the ID of the next overflow page
func (op *OverflowPage) GetNextOverflowID() uint64 {
	return binary.LittleEndian.Uint64(op.data[0:8])
}

// WriteData writes data to the overflow page, returning actual bytes written
func (op *OverflowPage) WriteData(offset int, data []byte) int {
	availableSpace := len(op.data) - OverflowHeaderSize - offset
	if availableSpace <= 0 {
		return 0
	}

	bytesToWrite := len(data)
	if bytesToWrite > availableSpace {
		bytesToWrite = availableSpace
	}

	writePos := OverflowHeaderSize + offset
	copy(op.data[writePos:writePos+bytesToWrite], data)
	return bytesToWrite
}

// ReadData reads data from the overflow page
func (op *OverflowPage) ReadData(offset int, length int) []byte {
	startPos := OverflowHeaderSize + offset
	endPos := startPos + length

	// Clamp to available data
	if startPos < OverflowHeaderSize {
		startPos = OverflowHeaderSize
	}
	if endPos > len(op.data) {
		endPos = len(op.data)
	}

	if startPos >= endPos {
		return []byte{}
	}

	result := make([]byte, endPos-startPos)
	copy(result, op.data[startPos:endPos])
	return result
}

// GetData returns the raw page data
func (op *OverflowPage) GetData() []byte {
	return op.data
}

// ============================================================================
// OVERFLOW CHAIN MANAGEMENT (in BPlusTreeBackend)
// ============================================================================

// writeValueWithOverflow stores a value, using overflow pages if necessary.
// Returns (numBytesInline, firstOverflowPageID)
func (b *BPlusTreeBackend) writeValueWithOverflow(value []byte) (int, uint64, error) {
	// If value fits in inline threshold, no overflow needed
	if len(value) <= OverflowThreshold {
		return len(value), 0, nil
	}

	// Need overflow pages
	var firstOverflowPageID uint64
	remaining := value[:]
	var prevOverflowPageID uint64

	for len(remaining) > 0 {
		// Allocate new overflow page
		pageRef, err := b.bufferManager.NewPage()
		if err != nil {
			return 0, 0, fmt.Errorf("failed to allocate overflow page: %w", err)
		}
		resourcePage := *pageRef
		overflowPageID := resourcePage.GetID()

		// Setup overflow page
		overflowPage := NewOverflowPage(len(resourcePage.GetData()))

		// Write data to this overflow page
		bytesWritten := overflowPage.WriteData(0, remaining)

		// Link from previous overflow page, if any
		if prevOverflowPageID != 0 {
			prevPage, err := b.bufferManager.PinPage(page.ResourcePageID(prevOverflowPageID))
			if err != nil {
				return 0, 0, fmt.Errorf("failed to pin previous overflow page: %w", err)
			}
			prevOverflowPage := &OverflowPage{data: prevPage.GetData()}
			prevOverflowPage.SetNextOverflowID(uint64(overflowPageID))
			prevPage.SetDirty(true)
			b.bufferManager.UnpinPage(page.ResourcePageID(prevOverflowPageID), true)
		} else {
			// This is the first overflow page
			firstOverflowPageID = uint64(overflowPageID)
		}

		// Write overflow page
		copy(resourcePage.GetData(), overflowPage.data)
		resourcePage.SetDirty(true)
		b.bufferManager.UnpinPage(overflowPageID, true)

		// Continue with remaining data
		remaining = remaining[bytesWritten:]
		prevOverflowPageID = uint64(overflowPageID)
	}

	return OverflowThreshold, firstOverflowPageID, nil
}

// readValueWithOverflow reads a value, following overflow pages if necessary.
func (b *BPlusTreeBackend) readValueWithOverflow(inlineData []byte, firstOverflowPageID uint64) ([]byte, error) {
	// Start with inline data
	result := make([]byte, len(inlineData))
	copy(result, inlineData)

	// If no overflow, return inline data
	if firstOverflowPageID == 0 {
		return result, nil
	}

	// Follow overflow chain
	currentOverflowPageID := firstOverflowPageID
	for currentOverflowPageID != 0 {
		overflowResourcePage, err := b.bufferManager.PinPage(page.ResourcePageID(currentOverflowPageID))
		if err != nil {
			return nil, fmt.Errorf("failed to pin overflow page: %w", err)
		}

		overflowPage := &OverflowPage{data: overflowResourcePage.GetData()}
		overflowData := overflowPage.ReadData(0, OverflowDataSize)
		result = append(result, overflowData...)

		// Move to next overflow page
		nextOverflowPageID := overflowPage.GetNextOverflowID()
		b.bufferManager.UnpinPage(page.ResourcePageID(currentOverflowPageID), false)
		currentOverflowPageID = nextOverflowPageID
	}

	return result, nil
}

// deleteValueWithOverflow frees overflow pages for a value
// Note: Currently this just unpins the pages - in a real implementation,
// these pages could be tracked for reuse or garbage collection
func (b *BPlusTreeBackend) deleteValueWithOverflow(firstOverflowPageID uint64) error {
	currentOverflowPageID := firstOverflowPageID

	for currentOverflowPageID != 0 {
		overflowResourcePage, err := b.bufferManager.PinPage(page.ResourcePageID(currentOverflowPageID))
		if err != nil {
			return fmt.Errorf("failed to pin overflow page for deletion: %w", err)
		}

		overflowPage := &OverflowPage{data: overflowResourcePage.GetData()}
		nextOverflowPageID := overflowPage.GetNextOverflowID()

		// Unpin this overflow page (buffer manager will handle eviction)
		b.bufferManager.UnpinPage(page.ResourcePageID(currentOverflowPageID), false)
		currentOverflowPageID = nextOverflowPageID
	}

	return nil
}

// ============================================================================
// PAGE MERGING (for sparse pages)
// ============================================================================

// shouldMergePage returns true if this page should be merged with a sibling
// Criteria: page has less than 25% capacity used
func (b *BPlusTreeBackend) shouldMergePage(page page.IResourcePage) bool {
	pageType := getPageType(page, 0, 2)

	if pageType == TypeLeaf {
		leaf := &LeafLogicPage{data: page.GetData()}
		// Calculate usage percentage
		usedSpace := int(leaf.FreeSpacePointer()) - LeafHeaderSize
		totalSpace := len(page.GetData()) - LeafHeaderSize
		if totalSpace == 0 {
			return false
		}
		usagePercent := (len(page.GetData()) - usedSpace) * 100 / len(page.GetData())
		return usagePercent < 25
	}

	return false
}

// getRightSibling returns the right sibling page ID for a leaf page
func (b *BPlusTreeBackend) getRightSibling(leafPage page.IResourcePage) uint64 {
	leaf := &LeafLogicPage{data: leafPage.GetData()}
	return leaf.RightSibling()
}

// getLeftSibling traverses the tree to find the left sibling of a page
// This is O(height) but page merging is rare
// getLeftSibling returns the ID of the left sibling of a leaf page
// This traverses the tree to find it - O(height) operation
func (b *BPlusTreeBackend) getLeftSibling(breadcrumbs []uint64, leafID uint64) (uint64, error) {
	if len(breadcrumbs) <= 1 {
		// Root or only page, no left sibling
		return 0, nil
	}

	// Get parent page
	parentID := breadcrumbs[len(breadcrumbs)-1]
	parentPage, err := b.bufferManager.PinPage(page.ResourcePageID(parentID))
	if err != nil {
		return 0, err
	}
	defer b.bufferManager.UnpinPage(page.ResourcePageID(parentID), false)

	parent := &InternalLogicPage{data: parentPage.GetData()}

	// Find the child pointer for our leaf by checking cells
	for i := uint16(0); i < parent.CellCount(); i++ {
		cell := parent.GetCell(parent.GetCellOffset(i))
		if cell.ChildPointer == uint64(leafID) {
			// Return the left sibling (previous pointer)
			if i == 0 {
				return 0, nil // No left sibling
			}
			prevCell := parent.GetCell(parent.GetCellOffset(i - 1))
			return prevCell.ChildPointer, nil
		}
	}

	// Check rightmost pointer
	if parent.RightmostPointer() == uint64(leafID) {
		cell := parent.GetCell(parent.GetCellOffset(parent.CellCount() - 1))
		return cell.ChildPointer, nil
	}

	return 0, fmt.Errorf("leaf not found in parent")
}

// mergeLeafPages merges a sparse leaf page with its right sibling
// Returns true if merge was successful
func (b *BPlusTreeBackend) mergeLeafPages(
	breadcrumbs []uint64,
	leafPage page.IResourcePage,
	leafID uint64,
) (bool, error) {
	rightSiblingID := b.getRightSibling(leafPage)
	if rightSiblingID == 0 {
		// Try left sibling instead
		leftSiblingID, err := b.getLeftSibling(breadcrumbs, uint64(leafID))
		if err != nil || leftSiblingID == 0 {
			return false, err // Can't merge
		}

		// Merge with left sibling: move all entries from current to left
		leftSiblingPage, err := b.bufferManager.PinPage(page.ResourcePageID(leftSiblingID))
		if err != nil {
			return false, err
		}
		defer b.bufferManager.UnpinPage(page.ResourcePageID(leftSiblingID), false)

		leftLeaf := &LeafLogicPage{data: leftSiblingPage.GetData()}
		currentLeaf := &LeafLogicPage{data: leafPage.GetData()}

		// Copy all cells from current leaf to left leaf
		for i := uint16(0); i < currentLeaf.CellCount(); i++ {
			cell := currentLeaf.GetCell(currentLeaf.GetCellOffset(i))
			if err := leftLeaf.Insert(cell.Key, cell.Value); err != nil {
				// Can't fit all data
				return false, nil
			}
		}

		// Update left sibling's right pointer to point past current leaf
		leftLeaf.SetRightSibling(currentLeaf.RightSibling())
		leftSiblingPage.SetDirty(true)

		// Delete current leaf page
		if err := b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false); err != nil {
			return false, err
		}

		return true, nil
	}

	// Merge with right sibling
	rightSiblingPage, err := b.bufferManager.PinPage(page.ResourcePageID(rightSiblingID))
	if err != nil {
		return false, err
	}
	defer b.bufferManager.UnpinPage(page.ResourcePageID(rightSiblingID), false)

	rightLeaf := &LeafLogicPage{data: rightSiblingPage.GetData()}
	currentLeaf := &LeafLogicPage{data: leafPage.GetData()}

	// Copy all cells from current leaf to right leaf (prepend)
	for i := uint16(0); i < currentLeaf.CellCount(); i++ {
		cell := currentLeaf.GetCell(currentLeaf.GetCellOffset(i))
		// This will fail if combined size exceeds page - that's OK, merge not needed
		if err := rightLeaf.Insert(cell.Key, cell.Value); err != nil {
			return false, nil
		}
	}

	// Update current leaf's right pointer to skip to right sibling's right sibling
	currentLeaf.SetRightSibling(rightLeaf.RightSibling())
	leafPage.SetDirty(true)

	// Update parent to remove the separator key
	// (This is simplified - full implementation would need to update parent)

	return true, nil
}
