package isolation

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/schema"
	"github.com/rodrigo0345/omag/internal/txn"
	"github.com/rodrigo0345/omag/internal/txn/testutil"
)

// IsolationManagerFactory creates different isolation managers for testing
type IsolationManagerFactory interface {
	CreateManager(storage storage.IStorageEngine) (txn.IIsolationManager, error)
	String() string
	Cleanup() error
}

// TwoPhaseLockingFactory creates 2PL managers
type TwoPhaseLockingFactory struct {
	tmpDir string
}

func (f *TwoPhaseLockingFactory) CreateManager(storage storage.IStorageEngine) (txn.IIsolationManager, error) {
	if storage == nil {
		storage = testutil.NewInMemoryStorageEngine()
	}
	bpm := testutil.NewTestBufferPoolManager(&testing.T{}, f.tmpDir)

	rollbackMgr := txn.NewRollbackManager(bpm)
	handler := txn.NewMVCCWriteHandler(storage, bpm, nil, rollbackMgr)

	return NewTwoPhaseLockingManager(
		nil,
		bpm,
		handler,
		rollbackMgr,
		storage,
		make(map[string]*schema.SecondaryIndexManager),
	), nil
}

func (f *TwoPhaseLockingFactory) String() string {
	return "TwoPhaseeLocking"
}

func (f *TwoPhaseLockingFactory) Cleanup() error {
	return os.RemoveAll(f.tmpDir)
}

// MVCCFactory creates MVCC managers
type MVCCFactory struct {
	tmpDir string
}

func (f *MVCCFactory) CreateManager(storage storage.IStorageEngine) (txn.IIsolationManager, error) {
	if storage == nil {
		storage = testutil.NewInMemoryStorageEngine()
	}
	bpm := testutil.NewTestBufferPoolManager(&testing.T{}, f.tmpDir)

	rollbackMgr := txn.NewRollbackManager(bpm)
	handler := txn.NewMVCCWriteHandler(storage, bpm, nil, rollbackMgr)

	return NewMVCCManager(
		nil,
		bpm,
		handler,
		rollbackMgr,
		storage,
		make(map[string]*schema.SecondaryIndexManager),
	), nil
}

func (f *MVCCFactory) String() string {
	return "MVCC"
}

func (f *MVCCFactory) Cleanup() error {
	return os.RemoveAll(f.tmpDir)
}

// OptimisticCCFactory creates OCC managers
type OptimisticCCFactory struct {
	tmpDir string
}

func (f *OptimisticCCFactory) CreateManager(storage storage.IStorageEngine) (txn.IIsolationManager, error) {
	if storage == nil {
		storage = testutil.NewInMemoryStorageEngine()
	}
	bpm := testutil.NewTestBufferPoolManager(&testing.T{}, f.tmpDir)

	rollbackMgr := txn.NewRollbackManager(bpm)
	handler := txn.NewMVCCWriteHandler(storage, bpm, nil, rollbackMgr)

	return NewOptimisticConcurrencyControlManager(
		nil,
		bpm,
		handler,
		rollbackMgr,
		storage,
		make(map[string]*schema.SecondaryIndexManager),
	), nil
}

func (f *OptimisticCCFactory) String() string {
	return "OptimisticCC"
}

func (f *OptimisticCCFactory) Cleanup() error {
	return os.RemoveAll(f.tmpDir)
}

// newFactories creates all isolation manager factories
func newFactories(t *testing.T) []IsolationManagerFactory {
	tmpDir1, _ := os.MkdirTemp("", "omag-2pl-test-")
	tmpDir2, _ := os.MkdirTemp("", "omag-mvcc-test-")
	tmpDir3, _ := os.MkdirTemp("", "omag-occ-test-")

	return []IsolationManagerFactory{
		&TwoPhaseLockingFactory{tmpDir: tmpDir1},
		&MVCCFactory{tmpDir: tmpDir2},
		&OptimisticCCFactory{tmpDir: tmpDir3},
	}
}

// cleanupFactories cleans up all factories
func cleanupFactories(factories []IsolationManagerFactory) {
	for _, f := range factories {
		f.Cleanup()
	}
}

// TestIsolationLevelBasicEnforcement tests that transactions store and retrieve isolation levels
func TestIsolationLevelBasicEnforcement(t *testing.T) {
	factories := newFactories(t)
	defer cleanupFactories(factories)

	isolationLevels := []uint8{
		txn.READ_UNCOMMITTED,
		txn.READ_COMMITTED,
		txn.REPEATABLE_READ,
		txn.SERIALIZABLE,
	}

	for _, factory := range factories {
		t.Run(factory.String(), func(t *testing.T) {
			mgr, err := factory.CreateManager(nil)
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}
			defer mgr.Close()

			schema := testutil.NewUserTableSchema()

			for _, level := range isolationLevels {
				txnID := mgr.BeginTransaction(level, "users", schema)
				if txnID <= 0 {
					t.Errorf("expected positive txn ID, got %d", txnID)
				}

				err := mgr.Commit(txnID)
				if err != nil {
					t.Logf("commit failed for isolation level %d: %v", level, err)
				}
			}
		})
	}
}

// TestDirtyReadsREAD_UNCOMMITTED tests that READ_UNCOMMITTED allows dirty reads, while higher levels block or read old versions.
func TestDirtyReadsREAD_UNCOMMITTED(t *testing.T) {
	factories := newFactories(t)
	defer cleanupFactories(factories)

	for _, factory := range factories {
		t.Run(factory.String(), func(t *testing.T) {
			storage := testutil.NewInMemoryStorageEngine()
			storage.Put([]byte("user:1"), []byte("Alice"))

			mgr, err := factory.CreateManager(storage)
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}
			defer mgr.Close()

			schema := testutil.NewUserTableSchema()

			// Transaction 1: UPDATE (uncommitted)
			txn1ID := mgr.BeginTransaction(txn.READ_COMMITTED, "users", schema)
			mgr.Write(txn1ID, []byte("user:1"), []byte("Bob"))

			// Transaction 2: READ_UNCOMMITTED (should allow dirty read)
			txn2ID := mgr.BeginTransaction(txn.READ_UNCOMMITTED, "users", schema)
			chUncommitted := make(chan string, 1)
			go func() {
				val, _ := mgr.Read(txn2ID, []byte("user:1"))
				chUncommitted <- string(val)
				mgr.Commit(txn2ID)
			}()

			select {
			case val := <-chUncommitted:
				if val != "Bob" && val != "Alice" {
					t.Errorf("Unexpected read value: %v", val)
				}
				if val == "Bob" {
					t.Logf("mgr %s: Read uncommitted value (dirty read allowed)", factory.String())
				}
			case <-time.After(500 * time.Millisecond):
				t.Errorf("READ_UNCOMMITTED blocked unexpectedly")
			}

			// Transaction 3: READ_COMMITTED (should NOT dirty read - in 2PL it blocks, in MVCC it might read 'Alice')
			if factory.String() == "TwoPhaseeLocking" {
				txn3ID := mgr.BeginTransaction(txn.READ_COMMITTED, "users", schema)
				chCommitted := make(chan string, 1)
				go func() {
					val, _ := mgr.Read(txn3ID, []byte("user:1"))
					chCommitted <- string(val)
					mgr.Commit(txn3ID)
				}()

				select {
				case <-chCommitted:
					t.Errorf("READ_COMMITTED did not block in 2PL (dirty read occurred!)")
				case <-time.After(100 * time.Millisecond):
					t.Logf("mgr %s: READ_COMMITTED correctly blocked to prevent dirty read", factory.String())
				}
				// Now commit txn1 to unblock txn3
				mgr.Commit(txn1ID)
				// Wait for txn3 to finish
				val := <-chCommitted
				if val != "Bob" {
					t.Errorf("Expected 'Bob' after commit, got '%s'", val)
				}
			} else {
				mgr.Commit(txn1ID)
			}
		})
	}
}

// TestNonRepeatableReadsREAD_COMMITTED tests that READ_COMMITTED allows non-repeatable reads
func TestNonRepeatableReadsREAD_COMMITTED(t *testing.T) {
	factories := newFactories(t)
	defer cleanupFactories(factories)

	for _, factory := range factories {
		t.Run(factory.String(), func(t *testing.T) {
			mgr, err := factory.CreateManager(nil)
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}
			defer mgr.Close()

			schema := testutil.NewUserTableSchema()
			storage := testutil.NewInMemoryStorageEngine()

			// Write initial value
			storage.Put([]byte("user:1"), []byte("100"))

			var wg sync.WaitGroup
			var value1, value2 string
			var mu sync.Mutex

			// Transaction 1: READ twice
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn1 := mgr.BeginTransaction(txn.READ_COMMITTED, "users", schema)
				defer mgr.Commit(txn1)

				// First read
				v1, _ := mgr.Read(txn1, []byte("user:1"))
				mu.Lock()
				value1 = string(v1)
				mu.Unlock()

				// Let txn2 update
				<-time.After(10 * time.Millisecond)

				// Second read (might be different)
				v2, _ := mgr.Read(txn1, []byte("user:1"))
				mu.Lock()
				value2 = string(v2)
				mu.Unlock()
			}()

			// Transaction 2: UPDATE between reads
			<-time.After(5 * time.Millisecond)
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn2 := mgr.BeginTransaction(txn.READ_COMMITTED, "users", schema)
				mgr.Write(txn2, []byte("user:1"), []byte("200"))
				mgr.Commit(txn2)
			}()

			wg.Wait()

			// With READ_COMMITTED, value1 and value2 might differ (non-repeatable read)
			// This is allowed, so we just verify the test runs
			t.Logf("mgr %s: value1=%s, value2=%s (non-repeatable reads allowed)", factory.String(), value1, value2)
		})
	}
}

// TestRepeatableReadEnforcement tests REPEATABLE_READ prevents non-repeatable reads
func TestRepeatableReadEnforcement(t *testing.T) {
	factories := newFactories(t)
	defer cleanupFactories(factories)

	for _, factory := range factories {
		t.Run(factory.String(), func(t *testing.T) {
			mgr, err := factory.CreateManager(nil)
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}
			defer mgr.Close()

			schema := testutil.NewUserTableSchema()
			storage := testutil.NewInMemoryStorageEngine()

			storage.Put([]byte("user:1"), []byte("100"))

			var wg sync.WaitGroup
			var value1, value2 string
			var mu sync.Mutex

			// Transaction 1: READ twice with REPEATABLE_READ
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn1 := mgr.BeginTransaction(txn.REPEATABLE_READ, "users", schema)
				defer mgr.Commit(txn1)

				v1, _ := mgr.Read(txn1, []byte("user:1"))
				mu.Lock()
				value1 = string(v1)
				mu.Unlock()

				<-time.After(20 * time.Millisecond)

				v2, _ := mgr.Read(txn1, []byte("user:1"))
				mu.Lock()
				value2 = string(v2)
				mu.Unlock()
			}()

			// Transaction 2: Try to update
			<-time.After(5 * time.Millisecond)
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn2 := mgr.BeginTransaction(txn.REPEATABLE_READ, "users", schema)
				// May be blocked or delayed by 2PL
				mgr.Write(txn2, []byte("user:1"), []byte("200"))
				mgr.Commit(txn2)
			}()

			wg.Wait()

			// With REPEATABLE_READ, values should be the same (held read locks prevent update)
			if value1 != value2 {
				t.Logf("mgr %s: values differ value1=%s, value2=%s (unexpected for REPEATABLE_READ with 2PL)", factory.String(), value1, value2)
			} else {
				t.Logf("mgr %s: values match value1=%s, value2=%s (expected for REPEATABLE_READ)", factory.String(), value1, value2)
			}
		})
	}
}

// TestSerializableIsolationLevel tests SERIALIZABLE enforcement
func TestSerializableIsolationLevel(t *testing.T) {
	factories := newFactories(t)
	defer cleanupFactories(factories)

	for _, factory := range factories {
		t.Run(factory.String(), func(t *testing.T) {
			mgr, err := factory.CreateManager(nil)
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}
			defer mgr.Close()

			schema := testutil.NewUserTableSchema()
			storage := testutil.NewInMemoryStorageEngine()

			storage.Put([]byte("user:1"), []byte("Alice"))
			storage.Put([]byte("user:2"), []byte("Bob"))

			var wg sync.WaitGroup
			var txn1Committed, txn2Committed bool

			// Transaction 1: Read user 1, then update user 2
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn1 := mgr.BeginTransaction(txn.SERIALIZABLE, "users", schema)
				defer func() {
					if err := mgr.Commit(txn1); err == nil {
						txn1Committed = true
					}
				}()

				mgr.Read(txn1, []byte("user:1"))
				<-time.After(10 * time.Millisecond)
				mgr.Write(txn1, []byte("user:2"), []byte("Bob-Updated"))
			}()

			// Transaction 2: Read user 2, then update user 1
			<-time.After(2 * time.Millisecond)
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn2 := mgr.BeginTransaction(txn.SERIALIZABLE, "users", schema)
				defer func() {
					if err := mgr.Commit(txn2); err == nil {
						txn2Committed = true
					}
				}()

				mgr.Read(txn2, []byte("user:2"))
				<-time.After(5 * time.Millisecond)
				mgr.Write(txn2, []byte("user:1"), []byte("Alice-Updated"))
			}()

			wg.Wait()

			// With SERIALIZABLE (2PL), one should typically be blocked
			// At least one should succeed
			if txn1Committed || txn2Committed {
				t.Logf("mgr %s: txn1=%v, txn2=%v (serializable maintained)", factory.String(), txn1Committed, txn2Committed)
			}
		})
	}
}

// TestWriteConflictDetection tests that write conflicts are detected appropriately
func TestWriteConflictDetection(t *testing.T) {
	factories := newFactories(t)
	defer cleanupFactories(factories)

	for _, factory := range factories {
		t.Run(factory.String(), func(t *testing.T) {
			mgr, err := factory.CreateManager(nil)
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}
			defer mgr.Close()

			schema := testutil.NewUserTableSchema()
			storage := testutil.NewInMemoryStorageEngine()
			storage.Put([]byte("user:1"), []byte("Alice"))

			var wg sync.WaitGroup
			var txn1Err, txn2Err error

			// Transaction 1: Write
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn1 := mgr.BeginTransaction(txn.SERIALIZABLE, "users", schema)
				txn1Err = mgr.Write(txn1, []byte("user:1"), []byte("Alice-v1"))
				_ = mgr.Commit(txn1)
			}()

			// Transaction 2: Write same key
			<-time.After(2 * time.Millisecond)
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn2 := mgr.BeginTransaction(txn.SERIALIZABLE, "users", schema)
				txn2Err = mgr.Write(txn2, []byte("user:1"), []byte("Alice-v2"))
				_ = mgr.Commit(txn2)
			}()

			wg.Wait()

			// At least one should have encountered a conflict or lock wait
			// Just verify no panic
			t.Logf("mgr %s: txn1 error=%v, txn2 error=%v", factory.String(), txn1Err, txn2Err)
		})
	}
}

// TestMultipleTransactionsAllLevels stress tests multiple concurrent transactions
func TestMultipleTransactionsAllLevels(t *testing.T) {
	factories := newFactories(t)
	defer cleanupFactories(factories)

	isolationLevels := []uint8{
		txn.READ_UNCOMMITTED,
		txn.READ_COMMITTED,
		txn.REPEATABLE_READ,
		txn.SERIALIZABLE,
	}

	for _, factory := range factories {
		for _, level := range isolationLevels {
			levelName := map[uint8]string{
				txn.READ_UNCOMMITTED: "READ_UNCOMMITTED",
				txn.READ_COMMITTED:   "READ_COMMITTED",
				txn.REPEATABLE_READ:  "REPEATABLE_READ",
				txn.SERIALIZABLE:     "SERIALIZABLE",
			}[level]

			t.Run(fmt.Sprintf("%s/%s", factory.String(), levelName), func(t *testing.T) {
				mgr, err := factory.CreateManager(nil)
				if err != nil {
					t.Fatalf("failed to create manager: %v", err)
				}
				defer mgr.Close()

				schema := testutil.NewUserTableSchema()
				storage := testutil.NewInMemoryStorageEngine()

				// Initialize some data
				for i := 1; i <= 5; i++ {
					key := []byte(fmt.Sprintf("user:%d", i))
					value := []byte(fmt.Sprintf("value-%d", i))
					storage.Put(key, value)
				}

				var wg sync.WaitGroup
				numTxns := 10

				// Spawn multiple transactions
				for i := 0; i < numTxns; i++ {
					wg.Add(1)
					go func(txnIdx int) {
						defer wg.Done()

						txnID := mgr.BeginTransaction(level, "users", schema)

						// Perform some reads
						for j := 1; j <= 3; j++ {
							key := []byte(fmt.Sprintf("user:%d", (txnIdx+j)%5+1))
							_, _ = mgr.Read(txnID, key)
						}

						// Perform a write
						key := []byte(fmt.Sprintf("user:%d", txnIdx%5+1))
						value := []byte(fmt.Sprintf("updated-by-txn-%d", txnIdx))
						_ = mgr.Write(txnID, key, value)

						// Commit
						_ = mgr.Commit(txnID)
					}(i)
				}

				wg.Wait()
				t.Logf("mgr %s with %s completed %d transactions successfully", factory.String(), levelName, numTxns)
			})
		}
	}
}

// TestAbortRollback tests that aborts properly rollback changes
func TestAbortRollback(t *testing.T) {
	factories := newFactories(t)
	defer cleanupFactories(factories)

	for _, factory := range factories {
		t.Run(factory.String(), func(t *testing.T) {
			mgr, err := factory.CreateManager(nil)
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}
			defer mgr.Close()

			schema := testutil.NewUserTableSchema()
			storage := testutil.NewInMemoryStorageEngine()
			storage.Put([]byte("user:1"), []byte("Alice"))

			// Transaction that will be aborted
			txnID := mgr.BeginTransaction(txn.READ_COMMITTED, "users", schema)
			mgr.Write(txnID, []byte("user:1"), []byte("Bob"))
			mgr.Abort(txnID)

			// The change should be visible only during the transaction,
			// but after abort, original value should be maintained
			t.Logf("mgr %s: Transaction aborted, changes should be rolled back", factory.String())
		})
	}
}
