package buffermanager

type IBufferPoolManager interface {
	NewPage() (*Page, error)
	PinPage(pageID PageID) (*Page, error)
	UnpinPage(pageID PageID, isDirty bool) error
}
