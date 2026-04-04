package buffermanager

import (
	"errors"
	"os"
	"sync"
)

var (
	ErrDiskManagerClosed = errors.New("disk manager is closed")
)

type DiskManager struct {
	dbFile   *os.File
	nextPage PageID
	mu       sync.Mutex
}

func NewDiskManager(dbPath string) (*DiskManager, error) {
	file, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	// Determine current number of pages based on file size
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	nextPage := PageID(stat.Size() / PageSize)

	return &DiskManager{
		dbFile:   file,
		nextPage: nextPage,
	}, nil
}

// ReadPage reads a page from disk into the provided buffer
func (dm *DiskManager) ReadPage(pageID PageID, pageData []byte) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	offset := int64(pageID) * PageSize

	// Seek to the page offset
	if _, err := dm.dbFile.Seek(offset, 0); err != nil {
		return err
	}

	// Read the page data
	if _, err := dm.dbFile.Read(pageData); err != nil {
		return err
	}

	return nil
}

// WritePage writes a page to disk
func (dm *DiskManager) WritePage(pageID PageID, pageData []byte) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	if len(pageData) != PageSize {
		return errors.New("page data size mismatch")
	}

	offset := int64(pageID) * PageSize

	// Seek to the page offset
	if _, err := dm.dbFile.Seek(offset, 0); err != nil {
		return err
	}

	// Write the page data
	if _, err := dm.dbFile.Write(pageData); err != nil {
		return err
	}

	return nil
}

// AllocatePage allocates a new page and returns its ID
func (dm *DiskManager) AllocatePage() PageID {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	pageID := dm.nextPage
	dm.nextPage++
	return pageID
}

// Close closes the underlying file
func (dm *DiskManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.dbFile != nil {
		return dm.dbFile.Close()
	}
	return nil
}

// Sync flushes all data to disk
func (dm *DiskManager) Sync() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.dbFile == nil {
		return ErrDiskManagerClosed
	}

	return dm.dbFile.Sync()
}
