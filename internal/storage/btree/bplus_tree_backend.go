package btree

import (
	"fmt"

	"github.com/rodrigo0345/omag/internal/storage/buffer"
	"github.com/rodrigo0345/omag/internal/storage/page"
)

type BPlusTreeBackend struct {
	bufferManager buffer.IBufferPoolManager
	meta          *MetaLogicPage
}

// NewBPlusTreeBackend creates a new B+ tree storage engine
func NewBPlusTreeBackend(bufferMgr buffer.IBufferPoolManager) (*BPlusTreeBackend, error) {
	// Create meta page (first page in tree)
	meta := NewMetaPage()

	return &BPlusTreeBackend{
		bufferManager: bufferMgr,
		meta:          meta,
	}, nil
}

// initializeRoot creates the first root page (a leaf page)
func (b *BPlusTreeBackend) initializeRoot() error {
	// Allocate a new page for the root
	pageRef, err := b.bufferManager.NewPage()
	if err != nil {
		return err
	}

	// pageRef is *page.IResourcePage - use it directly
	rootPage := *pageRef

	// Initialize it as a leaf page
	leaf := NewLeafPage(uint32(len(rootPage.GetData())))
	// Copy the initialized leaf page data into the buffer
	copy(rootPage.GetData(), leaf.data)

	rootPage.SetDirty(true)

	// Get the newly allocated page ID and set it as root in meta
	rootPageID := rootPage.GetID()
	b.meta.SetRootPage(uint64(rootPageID))

	// Unpin the page - this keeps it in the buffer pool
	if err := b.bufferManager.UnpinPage(rootPageID, true); err != nil {
		return err
	}

	return nil
}

// GetRootPageID returns the current root page ID for debugging purposes
func (b *BPlusTreeBackend) GetRootPageID() uint64 {
	return b.meta.RootPage()
}

func (b *BPlusTreeBackend) Get(key []byte) ([]byte, error) {
	rootPage := b.meta.RootPage()
	if rootPage == 0 {
		return nil, nil
	}

	path, err := b.findLeafPage(rootPage, key)
	if err != nil {
		return nil, fmt.Errorf("findLeafPage: %w", err)
	}

	leafPageID := path[len(path)-1]
	leafResourcePage, err := b.bufferManager.PinPage(page.ResourcePageID(leafPageID))
	if err != nil {
		return nil, err
	}
	defer b.bufferManager.UnpinPage(page.ResourcePageID(leafPageID), false)

	leafResourcePage.RLock()
	defer leafResourcePage.RUnlock()

	leafLogicalPage := &LeafLogicPage{data: leafResourcePage.GetData()}
	return leafLogicalPage.Get(key)
}

func (b *BPlusTreeBackend) Put(key []byte, value []byte) error {
	rootID := b.meta.RootPage()

	// Initialize root page if tree is empty
	if rootID == 0 {
		if err := b.initializeRoot(); err != nil {
			return fmt.Errorf("failed to initialize root: %w", err)
		}
		rootID = b.meta.RootPage()
	}

	path, err := b.findLeafPage(rootID, key)
	if err != nil {
		return fmt.Errorf("findLeafPage: %w", err)
	}

	leafID := path[len(path)-1]
	leafPage, err := b.bufferManager.PinPage(page.ResourcePageID(leafID))
	if err != nil {
		return err
	}

	leafPage.WLock()

	leaf := &LeafLogicPage{data: leafPage.GetData()}

	err = leaf.Insert(key, value)
	if err == ErrPageFull {
		// splitLeaf takes ownership of the lock and the pin
		return b.splitLeaf(path, leafPage, leafID, key, value)
	}
	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		return err
	}

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := b.bufferManager.UnpinPage(page.ResourcePageID(leafID), true); err != nil {
		return err
	}

	return nil
}

func (b *BPlusTreeBackend) Delete(key []byte) error {
	rootID := b.meta.RootPage()

	// If tree is empty, nothing to delete
	if rootID == 0 {
		return nil
	}

	path, err := b.findLeafPage(rootID, key)
	if err != nil {
		return err
	}

	leafID := path[len(path)-1]
	leafPage, err := b.bufferManager.PinPage(page.ResourcePageID(leafID))
	if err != nil {
		return err
	}

	// Acquire write latch on page
	leafPage.WLock()

	leaf := &LeafLogicPage{data: leafPage.GetData()}

	err = leaf.Remove(key)

	if err != nil {
		leafPage.WUnlock()
		b.bufferManager.UnpinPage(page.ResourcePageID(leafID), false)
		return err
	}

	leafPage.SetDirty(true)
	leafPage.WUnlock()

	if err := b.bufferManager.UnpinPage(page.ResourcePageID(leafID), true); err != nil {
		return err
	}
	return nil
}
