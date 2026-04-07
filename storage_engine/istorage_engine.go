package storageengine

type IStorageEngine interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	// TODO: Cursor() (ICursor, error)
}
