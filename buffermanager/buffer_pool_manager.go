package buffermanager

import (
	"errors"
	"sync"
)

var (
	ErrBufferFull    = errors.New("buffer pool is full")
	ErrPageNotFound  = errors.New("page not found in buffer pool")
	ErrInvalidPageID = errors.New("invalid page ID")
)

type BufferPoolManager struct {
	poolSize    int
	frames      []*Page            // Array of page frames
	pageTable   map[PageID]FrameID // Hash table: page ID -> frame ID
	freeList    []FrameID          // List of free frame IDs
	replacer    Replacer           // Page replacement policy
	diskManager *DiskManager
	walMgr      interface{} // WALManager reference (to avoid circular imports, use interface{})
	mu          sync.Mutex
}

// NewBufferPoolManager creates a new buffer pool manager
func NewBufferPoolManager(poolSize int, diskManager *DiskManager) *BufferPoolManager {
	frames := make([]*Page, poolSize)
	freeList := make([]FrameID, poolSize)

	// Initialize all frames and free list
	for i := 0; i < poolSize; i++ {
		frames[i] = NewPage(PageID(i))
		freeList[i] = FrameID(i)
	}

	return &BufferPoolManager{
		poolSize:    poolSize,
		frames:      frames,
		pageTable:   make(map[PageID]FrameID),
		freeList:    freeList,
		replacer:    NewLRUReplacer(poolSize),
		diskManager: diskManager,
	}
}

// SetWALManager sets the WAL manager reference
func (bpm *BufferPoolManager) SetWALManager(walMgr interface{}) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	bpm.walMgr = walMgr
}

// FetchPage fetches a page from the buffer pool, reading from disk if necessary
func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Page, error) {
	bpm.mu.Lock()

	// Check if page is already in buffer pool
	if frameID, exists := bpm.pageTable[pageID]; exists {
		frame := bpm.frames[frameID]
		frame.SetPinCount(frame.GetPinCount() + 1)
		bpm.replacer.Pin(frameID)
		bpm.mu.Unlock()
		return frame, nil
	}

	// Need to load from disk or allocate new page
	var frameID FrameID

	// Try to get a free frame
	if len(bpm.freeList) > 0 {
		frameID = bpm.freeList[0]
		bpm.freeList = bpm.freeList[1:]
	} else {
		// Need to evict a page
		var ok bool
		frameID, ok = bpm.replacer.Victim()
		if !ok {
			bpm.mu.Unlock()
			return nil, ErrBufferFull
		}

		// Flush the victim page if it's dirty
		victimPage := bpm.frames[frameID]
		if victimPage.IsDirty() {
			if err := bpm.diskManager.WritePage(victimPage.GetID(), victimPage.GetData()); err != nil {
				bpm.mu.Unlock()
				return nil, err
			}
		}

		// Remove the victim from the page table
		delete(bpm.pageTable, victimPage.GetID())
	}

	// Load the page from disk
	frame := bpm.frames[frameID]
	frame.id = pageID
	frame.SetPinCount(1)
	frame.SetDirty(false)

	// Read from disk
	if err := bpm.diskManager.ReadPage(pageID, frame.GetData()); err != nil {
		// If it's a new page, just clear the data
		// This is acceptable for pages that don't exist yet
		for i := range frame.data {
			frame.data[i] = 0
		}
	}

	// Update page table
	bpm.pageTable[pageID] = frameID
	bpm.replacer.Pin(frameID)

	bpm.mu.Unlock()
	return frame, nil
}

// UnpinPage unpins a page (decreases pin count)
func (bpm *BufferPoolManager) UnpinPage(pageID PageID, isDirty bool) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	frameID, exists := bpm.pageTable[pageID]
	if !exists {
		return ErrPageNotFound
	}

	frame := bpm.frames[frameID]

	// Decrease pin count
	if frame.GetPinCount() > 0 {
		frame.SetPinCount(frame.GetPinCount() - 1)
	}

	// Mark as dirty if needed
	if isDirty {
		frame.SetDirty(true)
	}

	// If pin count reaches 0, make it available for eviction
	if frame.GetPinCount() == 0 {
		bpm.replacer.Unpin(frameID)
	}

	return nil
}

// NewPage allocates a new page in the buffer pool
func (bpm *BufferPoolManager) NewPage() (*Page, error) {
	bpm.mu.Lock()

	// Allocate a new page ID from disk manager
	pageID := bpm.diskManager.AllocatePage()

	// Get a free frame or evict one
	var frameID FrameID
	if len(bpm.freeList) > 0 {
		frameID = bpm.freeList[0]
		bpm.freeList = bpm.freeList[1:]
	} else {
		// Need to evict a page
		var ok bool
		frameID, ok = bpm.replacer.Victim()
		if !ok {
			bpm.mu.Unlock()
			return nil, ErrBufferFull
		}

		// Flush the victim page if dirty
		victimPage := bpm.frames[frameID]
		if victimPage.IsDirty() {
			if err := bpm.diskManager.WritePage(victimPage.GetID(), victimPage.GetData()); err != nil {
				bpm.mu.Unlock()
				return nil, err
			}
		}

		// Remove from page table
		delete(bpm.pageTable, victimPage.GetID())
	}

	// Initialize the new frame
	frame := bpm.frames[frameID]
	frame.id = pageID
	frame.SetPinCount(1)
	frame.SetDirty(false)

	// Clear page data
	for i := range frame.data {
		frame.data[i] = 0
	}

	// Add to page table
	bpm.pageTable[pageID] = frameID
	bpm.replacer.Pin(frameID)

	bpm.mu.Unlock()
	return frame, nil
}

// FlushPage flushes a single page to disk
func (bpm *BufferPoolManager) FlushPage(pageID PageID) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	frameID, exists := bpm.pageTable[pageID]
	if !exists {
		return ErrPageNotFound
	}

	frame := bpm.frames[frameID]
	if err := bpm.diskManager.WritePage(pageID, frame.GetData()); err != nil {
		return err
	}

	frame.SetDirty(false)
	return nil
}

// FlushAll flushes all dirty pages to disk
func (bpm *BufferPoolManager) FlushAll() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	for pageID, frameID := range bpm.pageTable {
		frame := bpm.frames[frameID]
		if frame.IsDirty() {
			if err := bpm.diskManager.WritePage(pageID, frame.GetData()); err != nil {
				return err
			}
			frame.SetDirty(false)
		}
	}

	return nil
}

// Close closes the buffer pool manager and underlying disk manager
func (bpm *BufferPoolManager) Close() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	// Flush all dirty pages before closing
	for pageID, frameID := range bpm.pageTable {
		frame := bpm.frames[frameID]
		if frame.IsDirty() {
			if err := bpm.diskManager.WritePage(pageID, frame.GetData()); err != nil {
				return err
			}
		}
	}

	return bpm.diskManager.Close()
}

// GetPoolSize returns the size of the buffer pool
func (bpm *BufferPoolManager) GetPoolSize() int {
	return bpm.poolSize
}
