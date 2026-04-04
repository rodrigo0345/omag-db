package buffermanager

import (
	"errors"
	"io"
	"os"
	"sync"
)

// Logic responsible for managing page storage location and retrieving
var (
	ErrPageBounds      = errors.New("page ID out of bounds")
	ErrInvalidPageSize = errors.New("invalid page size")
	ErrZeroCacheSize   = errors.New("cache capacity must be greater than zero")
)

const DefaultCacheCapacity = 256

type pageFrame struct {
	data       []byte
	dirty      bool
	lastAccess uint64
}

type Pager struct {
	file     *os.File
	pageSize uint32
	numPages uint64
	inMemory bool
	memPages [][]byte

	mu            sync.RWMutex
	cache         map[uint64]*pageFrame
	cacheCapacity int
	clock         uint64
}

func NewPager(filePath string, pageSize uint32, inMemory bool) (*Pager, error) {
	if pageSize == 0 {
		return nil, ErrInvalidPageSize
	}

	p := &Pager{
		pageSize:      pageSize,
		inMemory:      inMemory,
		cache:         make(map[uint64]*pageFrame),
		cacheCapacity: DefaultCacheCapacity,
		clock:         1,
	}

	if inMemory {
		p.memPages = make([][]byte, 0)
		return p, nil
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	p.file = file

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	p.numPages = uint64(stat.Size()) / uint64(pageSize)
	return p, nil
}

func (p *Pager) FetchPage(pageID uint64) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageID >= p.numPages {
		return nil, ErrPageBounds
	}

	if frame, ok := p.cache[pageID]; ok {
		frame.lastAccess = p.tickLocked()
		return clone(frame.data), nil
	}

	pageData, err := p.fetchFromStorageLocked(pageID)
	if err != nil {
		return nil, err
	}

	if err := p.addToCacheLocked(pageID, pageData, false); err != nil {
		return nil, err
	}

	return clone(pageData), nil
}

func (p *Pager) WritePage(pageID uint64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(data) != int(p.pageSize) {
		return ErrInvalidPageSize
	}

	if pageID >= p.numPages {
		return ErrPageBounds
	}

	if frame, ok := p.cache[pageID]; ok {
		copy(frame.data, data)
		frame.dirty = true
		frame.lastAccess = p.tickLocked()
		return nil
	}

	if err := p.addToCacheLocked(pageID, clone(data), true); err != nil {
		return err
	}

	return nil
}

func (p *Pager) AllocatePage() (uint64, []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pageID := p.numPages
	p.numPages++

	pageData := make([]byte, p.pageSize)

	if p.inMemory {
		p.memPages = append(p.memPages, pageData)
	}

	if err := p.addToCacheLocked(pageID, clone(pageData), true); err != nil {
		p.numPages--
		if p.inMemory {
			p.memPages = p.memPages[:len(p.memPages)-1]
		}
		return 0, nil, err
	}

	return pageID, pageData, nil
}

// write all dirty (pages not saved on storage) pages to disk. For in-memory, this is a no-op.
func (p *Pager) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for pageID, frame := range p.cache {
		if !frame.dirty {
			continue
		}
		if err := p.writeToStorageLocked(pageID, frame.data); err != nil {
			return err
		}
		frame.dirty = false
	}

	if p.inMemory {
		return nil
	}
	return p.file.Sync()
}

func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for pageID, frame := range p.cache {
		if !frame.dirty {
			continue
		}
		if err := p.writeToStorageLocked(pageID, frame.data); err != nil {
			return err
		}
		frame.dirty = false
	}

	if p.inMemory {
		p.memPages = nil
		p.cache = map[uint64]*pageFrame{}
		p.numPages = 0
		return nil
	}
	if p.file != nil {
		err := p.file.Sync()
		if err != nil {
			return err
		}
		return p.file.Close()
	}
	return nil
}

func (p *Pager) PageCount() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.numPages
}

func (p *Pager) PageSize() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.pageSize
}

func (p *Pager) SetCacheCapacity(capacity int) error {
	if capacity <= 0 {
		return ErrZeroCacheSize
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.cacheCapacity = capacity
	for len(p.cache) > p.cacheCapacity {
		if err := p.evictOneLocked(); err != nil {
			return err
		}
	}

	return nil
}

func (p *Pager) tickLocked() uint64 {
	v := p.clock
	p.clock++
	return v
}

func (p *Pager) addToCacheLocked(pageID uint64, data []byte, dirty bool) error {
	if p.cacheCapacity <= 0 {
		return ErrZeroCacheSize
	}

	if frame, ok := p.cache[pageID]; ok {
		copy(frame.data, data)
		frame.dirty = frame.dirty || dirty
		frame.lastAccess = p.tickLocked()
		return nil
	}

	for len(p.cache) >= p.cacheCapacity {
		if err := p.evictOneLocked(); err != nil {
			return err
		}
	}

	p.cache[pageID] = &pageFrame{
		data:       clone(data),
		dirty:      dirty,
		lastAccess: p.tickLocked(),
	}

	return nil
}

func (p *Pager) evictOneLocked() error {
	var victimID uint64
	var victim *pageFrame

	for pageID, frame := range p.cache {
		if victim == nil || frame.lastAccess < victim.lastAccess {
			victimID = pageID
			victim = frame
		}
	}

	if victim == nil {
		return nil
	}

	if victim.dirty {
		if err := p.writeToStorageLocked(victimID, victim.data); err != nil {
			return err
		}
	}

	delete(p.cache, victimID)
	return nil
}

func (p *Pager) fetchFromStorageLocked(pageID uint64) ([]byte, error) {
	pageData := make([]byte, p.pageSize)

	if p.inMemory {
		copy(pageData, p.memPages[pageID])
		return pageData, nil
	}

	offset := int64(pageID) * int64(p.pageSize)
	_, err := p.file.ReadAt(pageData, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return pageData, nil
}

func (p *Pager) writeToStorageLocked(pageID uint64, data []byte) error {
	if p.inMemory {
		copy(p.memPages[pageID], data)
		return nil
	}

	offset := int64(pageID) * int64(p.pageSize)
	_, err := p.file.WriteAt(data, offset)
	return err
}

func clone(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
