package isolation

import (
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/txn"
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
		userKey, xmin, ok := c.manager.decodeKey(entry.Key)
		if !ok || !c.manager.isVisible(c.txn, txn.TransactionID(xmin)) {
			continue
		}

		keyStr := string(userKey)
		if c.seenKeys[keyStr] {
			continue
		}

		// Lock the key immediately
		c.seenKeys[keyStr] = true

		if len(entry.Value) == 0 {
			continue
		}

		// Identify operation byte
		op := entry.Value[0]
		if op == OpDelete {
			// Key is marked as seen; loop continues but
			// will skip all older versions of keyStr.
			continue
		}

		c.currentEntry = entry
		c.currentEntry.Value = entry.Value[1:] // Strip metadata
		c.currentEntry.Key = userKey
		return true
	}
	return false
}

func (c *MVCCCursor) Entry() storage.ScanEntry { return c.currentEntry }
func (c *MVCCCursor) Close() error             { return c.raw.Close() }
func (c *MVCCCursor) Error() error             { return c.raw.Error() }
