package transaction_manager

import (
	"bytes"
	"errors"
	"sync"
)

var (
	ErrDeadlock     = errors.New("deadlock detected")
	ErrTxnAborted   = errors.New("transaction aborted")
	ErrLockConflict = errors.New("lock conflict")
)

type LockMode int

const (
	SHARED LockMode = iota
	EXCLUSIVE
)

type LockState int

const (
	GRANTED LockState = iota
	WAITING
)

type LockRequest struct {
	txnID uint64
	mode  LockMode
	state LockState
	ch    chan struct{} // channel to block until lock is granted
}

type LockQueue struct {
	requests []*LockRequest
	// condition variable could also be used, but keeping it simple using channels
	// mu is not needed if LockManager has a global lock, but fine-grained locking is better
	mu sync.Mutex
}

type LockManager struct {
	lockTable map[string]*LockQueue
	mu        sync.Mutex
}

func NewLockManager() *LockManager {
	return &LockManager{
		lockTable: make(map[string]*LockQueue),
	}
}

// LockShared acquires a shared lock on the key.
func (lm *LockManager) LockShared(txn *Transaction, key []byte) error {
	if txn.state == ABORTED {
		return ErrTxnAborted
	}

	keyStr := string(key)

	lm.mu.Lock()
	queue, exists := lm.lockTable[keyStr]
	if !exists {
		queue = &LockQueue{requests: make([]*LockRequest, 0)}
		lm.lockTable[keyStr] = queue
	}
	lm.mu.Unlock()

	req := &LockRequest{
		txnID: txn.GetID(),
		mode:  SHARED,
		state: WAITING,
		ch:    make(chan struct{}),
	}

	queue.mu.Lock()

	// Check if already holding the lock (upgrade logic or idempotent)
	for _, r := range queue.requests {
		if r.txnID == txn.GetID() && r.state == GRANTED {
			if r.mode == EXCLUSIVE {
				queue.mu.Unlock()
				return nil // Already have exclusive, shared is implicitly granted
			}
			if r.mode == SHARED {
				queue.mu.Unlock()
				return nil // Already hold shared
			}
		}
	}

	canGrant := true
	for _, expected := range queue.requests {
		if expected.mode == EXCLUSIVE {
			canGrant = false // Someone holds or is waiting for exclusive
			break
		}
	}

	if canGrant {
		req.state = GRANTED
		txn.sharedLocks = append(txn.sharedLocks, key)
	}

	queue.requests = append(queue.requests, req)
	queue.mu.Unlock()

	if canGrant {
		return nil
	}

	// Wait logic
	// In a real DB, LockManager checks for cycles (deadlocks) asynchronously
	// Here, we wait on channel or cycle detection
	select {
	case <-req.ch:
		if txn.state == ABORTED {
			return ErrTxnAborted
		}
		// Granted, add to txn
		txn.sharedLocks = append(txn.sharedLocks, key)
		return nil
	}
}

// LockExclusive acquires an exclusive lock on the key.
func (lm *LockManager) LockExclusive(txn *Transaction, key []byte) error {
	if txn.state == ABORTED {
		return ErrTxnAborted
	}

	keyStr := string(key)

	lm.mu.Lock()
	queue, exists := lm.lockTable[keyStr]
	if !exists {
		queue = &LockQueue{requests: make([]*LockRequest, 0)}
		lm.lockTable[keyStr] = queue
	}
	lm.mu.Unlock()

	req := &LockRequest{
		txnID: txn.GetID(),
		mode:  EXCLUSIVE,
		state: WAITING,
		ch:    make(chan struct{}),
	}

	queue.mu.Lock()

	// Check if already holding the lock
	isUpgrade := false
	for _, r := range queue.requests {
		if r.txnID == txn.GetID() && r.state == GRANTED {
			if r.mode == EXCLUSIVE {
				queue.mu.Unlock()
				return nil // Already hold exclusive
			}
			if r.mode == SHARED {
				isUpgrade = true
				req.state = WAITING
				break
			}
		}
	}

	canGrant := len(queue.requests) == 0 || (isUpgrade && len(queue.requests) == 1 && queue.requests[0].txnID == txn.GetID())

	if canGrant {
		req.state = GRANTED
		txn.exclusiveLocks = append(txn.exclusiveLocks, key)
		// if upgrade, replace the shared request
		if isUpgrade {
			queue.requests[0] = req
		} else {
			queue.requests = append(queue.requests, req)
		}
	} else {
		queue.requests = append(queue.requests, req)
	}
	queue.mu.Unlock()

	if canGrant {
		return nil
	}

	// Wait
	select {
	case <-req.ch:
		if txn.state == ABORTED {
			return ErrTxnAborted
		}
		txn.exclusiveLocks = append(txn.exclusiveLocks, key)
		return nil
	}
}

// Unlock releases the lock held by the transaction on the key.
func (lm *LockManager) Unlock(txn *Transaction, key []byte) error {
	keyStr := string(key)

	lm.mu.Lock()
	queue, exists := lm.lockTable[keyStr]
	lm.mu.Unlock()

	if !exists {
		return nil
	}

	queue.mu.Lock()
	defer queue.mu.Unlock()

	// Handle removing from txn local tracking (removed simply to save space, but in a real DB loop to remove)
	removeBytesSlice := func(slice [][]byte, k []byte) [][]byte {
		for i, v := range slice {
			if bytes.Equal(v, k) {
				return append(slice[:i], slice[i+1:]...)
			}
		}
		return slice
	}

	txn.sharedLocks = removeBytesSlice(txn.sharedLocks, key)
	txn.exclusiveLocks = removeBytesSlice(txn.exclusiveLocks, key)

	// Remove from queue
	foundIndex := -1
	for i, r := range queue.requests {
		if r.txnID == txn.GetID() && r.state == GRANTED {
			foundIndex = i
			break
		}
	}

	if foundIndex != -1 {
		queue.requests = append(queue.requests[:foundIndex], queue.requests[foundIndex+1:]...)
	}

	// notify next waiting tasks
	lm.notifyNext(queue)
	return nil
}

func (lm *LockManager) notifyNext(queue *LockQueue) {
	if len(queue.requests) == 0 {
		return
	}

	firstWaiting := -1
	for i, r := range queue.requests {
		if !(r.state == GRANTED) {
			firstWaiting = i
			break
		}
	}

	if firstWaiting == -1 {
		return // All are potentially granted (e.g. multiple shared)
	}

	if queue.requests[firstWaiting].mode == EXCLUSIVE {
		// Only grant if no one else is currently holding it
		anyoneHolding := false
		for i := 0; i < firstWaiting; i++ {
			if queue.requests[i].state == GRANTED {
				anyoneHolding = true
				break
			}
		}
		if !anyoneHolding {
			queue.requests[firstWaiting].state = GRANTED
			close(queue.requests[firstWaiting].ch)
		}
	} else if queue.requests[firstWaiting].mode == SHARED {
		// Grant to all subsequent consecutive shared requests
		anyoneHoldingExclusive := false
		for i := 0; i < firstWaiting; i++ {
			if queue.requests[i].state == GRANTED && queue.requests[i].mode == EXCLUSIVE {
				anyoneHoldingExclusive = true
				break
			}
		}

		if !anyoneHoldingExclusive {
			for i := firstWaiting; i < len(queue.requests); i++ {
				if queue.requests[i].mode == SHARED {
					if queue.requests[i].state != GRANTED {
						queue.requests[i].state = GRANTED
						close(queue.requests[i].ch)
					}
				} else {
					break // Stop if we hit an exclusive request
				}
			}
		}
	}
}

// detectCycle checks for deadlocks
func (lm *LockManager) detectCycle() bool {
	// A simple Waits-for graph can be built to detect cycle
	return false
}
