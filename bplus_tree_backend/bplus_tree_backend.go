package bplus_tree_backend

import (
	"encoding/binary"

	"github.com/rodrigo0345/omag/buffermanager"
	"github.com/rodrigo0345/omag/logmanager"
	"github.com/rodrigo0345/omag/resource_page"
)

type BPlusTreeBackend struct {
	bufferManager buffermanager.IBufferPoolManager
	logManager   logmanager.ILogManager
	meta        *MetaLogicPage
}

func (b *BPlusTreeBackend) Get(key []byte) ([]byte, error) {
	rootPage := b.meta.RootPage()
	if rootPage == 0 {
		return nil, nil // No data
	}

	path, err := b.findLeafPage(rootPage, key)
	if err != nil {
		panic(err)
	}

}

func (b *BPlusTreeBackend) Put(key []byte, value []byte) error {
	return nil
}

func (b *BPlusTreeBackend) Delete(key []byte) error {
	return nil
}

var page_data_type_start_index = 0
var page_data_type_end_index = 2

func (tree *BPlusTreeBackend) findLeafPage(pageID uint64, key []byte) ([]uint64, error) {
	var path []uint64

	for {
		path = append(path, pageID) // breadcrumb

		pageObj, err := tree.bufferManager.PinPage(resource_page.ResourcePageID(pageID))
		if err != nil {
			return nil, err
		}

		logicPageType := getPageType(*pageObj, 0, 2)

		tree.bufferManager.UnpinPage(resource_page.ResourcePageID(pageID), false)

		switch logicPageType {
		case TypeLeaf:
			return path, nil // found what we needed
		case TypeInternal:
			resource_page_internal, err := tree.bufferManager.PinPage(resource_page.ResourcePageID(pageID))
			if err != nil {
				return nil, err
			}
			logicPageID := internalPageNext(*resource_page_internal, key)
			tree.bufferManager.UnpinPage(resource_page.ResourcePageID(logicPageID), false)

			// search this new key
			pageID = logicPageID
			// keep searching
		default:
			return nil, ErrInvalidPageType
		}
	}
}

func getPageType(pageObj resource_page.IResourcePage,
		 type_start_index uint8,
		 type_end_index uint8) (LogicPageType) {
	pageObj.RLock()
	defer pageObj.RUnlock()

	data := pageObj.GetData()
	logicPageType := LogicPageType(binary.LittleEndian.Uint16(data[type_start_index:type_end_index]))
	return logicPageType
}

func internalPageNext(internalPage resource_page.IResourcePage,
		 		key []byte)(uint64){
	internalPage.RLock()
	internal := &InternalLogicPage{data: internalPage.GetData()}
	pageID := internal.Search(key)
	internalPage.RUnlock()
	return pageID
}
