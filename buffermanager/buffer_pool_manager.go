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

// ReplacerType specifies which page replacement algorithm to use
type ReplacerType int

const (
	ReplacerLRU   ReplacerType = iota // Least Recently Used
	ReplacerClock                     // Clock-sweep algorithm
)

// WALFlusher defines the subset of WAL functionality the BPM needs
type WALFlusher interface {
	Flush(upToLSN uint64) error
}

type BufferPoolManager struct {
	poolSize     int
	frames       []*Page            // actual cache buffer with configurable replacement policy
	pageTable    map[PageID]FrameID // logical PageID to physical FrameID mapping
	freeList     []FrameID
	replacer     Replacer
	replacerType ReplacerType // Track which replacer algorithm is being used
	diskManager  *DiskManager
	walMgr       interface{}
	mu           sync.Mutex
}

func NewBufferPoolManager(poolSize int, diskManager *DiskManager) *BufferPoolManager {
	return NewBufferPoolManagerWithReplacer(poolSize, diskManager, ReplacerClock)
}

// NewBufferPoolManagerWithReplacer creates a buffer pool manager with a specified replacer algorithm
func NewBufferPoolManagerWithReplacer(poolSize int, diskManager *DiskManager, replacerType ReplacerType) *BufferPoolManager {
	frames := make([]*Page, poolSize)
	freeList := make([]FrameID, poolSize)

	for i := 0; i < poolSize; i++ {
		frames[i] = NewPage(PageID(i))
		freeList[i] = FrameID(i)
	}

	var replacer Replacer
	switch replacerType {
	case ReplacerLRU:
		replacer = NewLRUReplacer(poolSize)
	case ReplacerClock:
		replacer = NewClockReplacer(poolSize)
	default:
		replacer = NewLRUReplacer(poolSize) // Default to LRU
	}

	return &BufferPoolManager{
		poolSize:     poolSize,
		frames:       frames,
		pageTable:    make(map[PageID]FrameID),
		freeList:     freeList,
		replacer:     replacer,
		replacerType: replacerType,
		diskManager:  diskManager,
	}
}

func (bpm *BufferPoolManager) SetWALManager(walMgr interface{}) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	bpm.walMgr = walMgr
}

// flushPageInternal handles the WAL protocol + Disk I/O sequence.
// bpm.mu MUST be held when calling this.
func (bpm *BufferPoolManager) flushPageInternal(page *Page) error {

	// if the page is not dirty, we can skip the flush
	if !page.IsDirty() {
		return nil
	}

	// make sure all WAL records up to pageLSN of this page are flushed before writing the page to disk
	if bpm.walMgr != nil {
		if flusher, ok := bpm.walMgr.(WALFlusher); ok {
			if err := flusher.Flush(page.pageLSN); err != nil {
				return err
			}
		}
	}

	// now that every change was appended to the wal, we can safely write the page to disk
	if err := bpm.diskManager.WritePage(page.GetID(), page.GetData()); err != nil {
		return err
	}

	page.SetDirty(false)
	return nil
}

func (bpm *BufferPoolManager) FetchPage(pageID PageID) (*Page, error) {
	bpm.mu.Lock()

	// if the page is already in the buffer pool, just pin and return it
	if frameID, exists := bpm.pageTable[pageID]; exists {
		frame := bpm.frames[frameID]
		frame.SetPinCount(frame.GetPinCount() + 1)
		bpm.replacer.Pin(frameID)
		bpm.mu.Unlock()
		return frame, nil
	}

	frameID, err := bpm.getAvailableFrameIDLocked()
	if err != nil {
		bpm.mu.Unlock()
		return nil, err
	}

	frame := bpm.frames[frameID]
	frame.id = pageID
	frame.SetPinCount(1)
	frame.SetDirty(false)
	frame.pageLSN = 0 // Reset LSN for newly loaded content

	// fill the frame's data with the page content from disk
	if err := bpm.diskManager.ReadPage(pageID, frame.GetData()); err != nil {
		// Clear data if page doesn't exist yet
		frame.DeepClean()
	}

	bpm.pageTable[pageID] = frameID
	bpm.replacer.Pin(frameID)

	bpm.mu.Unlock()
	return frame, nil
}

func (bpm *BufferPoolManager) UnpinPage(pageID PageID, isDirty bool) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	frameID, exists := bpm.pageTable[pageID]
	if !exists {
		return ErrPageNotFound
	}

	frame := bpm.frames[frameID]
	if frame.GetPinCount() > 0 {
		frame.SetPinCount(frame.GetPinCount() - 1)
	}

	if isDirty {
		frame.SetDirty(true)
	}

	if frame.GetPinCount() == 0 {
		bpm.replacer.Unpin(frameID)
	}

	return nil
}

func (bpm *BufferPoolManager) NewPage() (*Page, error) {
	bpm.mu.Lock()

	pageID := bpm.diskManager.AllocatePage()

	frameID, err := bpm.getAvailableFrameIDLocked()
	if err != nil {
		bpm.mu.Unlock()
		return nil, err
	}

	frame := bpm.frames[frameID]
	frame.id = pageID
	frame.SetPinCount(1)
	frame.SetDirty(false)
	frame.pageLSN = 0

	for i := range frame.data {
		frame.data[i] = 0
	}

	bpm.pageTable[pageID] = frameID
	bpm.replacer.Pin(frameID)

	bpm.mu.Unlock()
	return frame, nil
}

func (bpm *BufferPoolManager) FlushPage(pageID PageID) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	frameID, exists := bpm.pageTable[pageID]
	if !exists {
		return ErrPageNotFound
	}

	return bpm.flushPageInternal(bpm.frames[frameID])
}

func (bpm *BufferPoolManager) FlushAll() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	for _, frameID := range bpm.pageTable {
		if err := bpm.flushPageInternal(bpm.frames[frameID]); err != nil {
			return err
		}
	}
	return nil
}

func (bpm *BufferPoolManager) Close() error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	for _, frameID := range bpm.pageTable {
		if err := bpm.flushPageInternal(bpm.frames[frameID]); err != nil {
			return err
		}
	}

	return bpm.diskManager.Close()
}

// getAvailableFrameIDLocked finds a free frame or evicts a victim using the WAL protocol.
func (bpm *BufferPoolManager) getAvailableFrameIDLocked() (FrameID, error) {

	// check if there is already a free frame
	if len(bpm.freeList) > 0 {
		frameID := bpm.freeList[0]
		bpm.freeList = bpm.freeList[1:]
		return frameID, nil
	}

	// if not, we need to evict a victim frame, to create space for the new page
	frameID, ok := bpm.replacer.Victim()
	if !ok {
		return 0, ErrBufferFull
	}

	// we get the victim page and flush it to disk in case it is dirty
	victimPage := bpm.frames[frameID]
	if err := bpm.flushPageInternal(victimPage); err != nil {
		return 0, err
	}

	// now we can reuse the victim frame id for the new page, but first we need to remove the victim page from the page table
	delete(bpm.pageTable, victimPage.GetID())
	return frameID, nil
}

func (bpm *BufferPoolManager) GetPoolSize() int {
	return bpm.poolSize
}

// GetReplacerType returns the type of replacer algorithm currently in use
func (bpm *BufferPoolManager) GetReplacerType() ReplacerType {
	return bpm.replacerType
}

// GetReplacerName returns a human-readable name for the replacer algorithm
func (bpm *BufferPoolManager) GetReplacerName() string {
	switch bpm.replacerType {
	case ReplacerLRU:
		return "LRU"
	case ReplacerClock:
		return "Clock"
	default:
		return "Unknown"
	}
}

// GetReplacerSize returns the number of frames tracked by the replacer
func (bpm *BufferPoolManager) GetReplacerSize() int {
	return bpm.replacer.Size()
}
