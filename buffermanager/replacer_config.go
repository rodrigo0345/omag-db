// buffermanager/replacer_config.go
// Configuration and utilities for selecting page replacement algorithms

package buffermanager

// ReplacerInfo provides information about the configured replacer
type ReplacerInfo struct {
	Type string
	Name string
	Size int
}

// GetReplacerInfo returns detailed information about the buffer pool's replacer
func (bpm *BufferPoolManager) GetReplacerInfo() ReplacerInfo {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	return ReplacerInfo{
		Type: "BufferPoolManager replacer algorithm info",
		Name: bpm.GetReplacerName(),
		Size: bpm.GetReplacerSize(),
	}
}
