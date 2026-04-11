package lsm

import "errors"

// ErrKeyTombstoned is returned when a Get operation finds a key that has been marked as deleted.
var ErrKeyTombstoned = errors.New("key has been tombstoned (deleted)")
