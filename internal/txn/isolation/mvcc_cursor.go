package isolation

import (
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/txn/txn_unit"
)

type MVCCCursor struct {
	raw          storage.ICursor
	manager      *MVCCManager
	txn          *txn_unit.Transaction
	seenKeys     map[string]bool
	currentEntry storage.ScanEntry
}

func (c *MVCCCursor) Next() bool {
	for c.raw.Next() {
		entry := c.raw.Entry()
		userKey, xmin := c.manager.decodeKey(entry.Key)
		uKeyStr := string(userKey)

		// Skip if already processed a newer visible version
		if c.seenKeys[uKeyStr] {
			continue
		}

		// Visibility Check
		if !c.manager.isVisible(c.txn, xmin) {
			continue
		}

		// Metadata processing
		c.seenKeys[uKeyStr] = true
		opType := entry.Value[0]

		if opType == OpDelete {
			continue
		}

		c.currentEntry = storage.ScanEntry{
			Key:   userKey,
			Value: entry.Value[1:],
		}
		return true
	}
	return false
}

func (c *MVCCCursor) Entry() storage.ScanEntry { return c.currentEntry }
func (c *MVCCCursor) Close() error             { return c.raw.Close() }
func (c *MVCCCursor) Error() error             { return c.raw.Error() }
