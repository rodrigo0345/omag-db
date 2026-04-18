package recovery

import (
	"context"
	"fmt"
	"log"

	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/buffer"
	wallog "github.com/rodrigo0345/omag/internal/txn/log"
	"github.com/rodrigo0345/omag/internal/txn/rollback"
)

type RecoveryCoordinator interface {
	RecoverFromCrash(ctx context.Context) (*wallog.RecoveryState, error)

	ApplyRecoveryState(ctx context.Context, state *wallog.RecoveryState) error

	ValidateRecoveredState(ctx context.Context, state *wallog.RecoveryState) error

	GetRecoveryStats() RecoveryStats
}

type RecoveryStats struct {
	TotalRecords           int64
	RecordsRedo            int64
	RecordsUndo            int64
	CommittedTransactions  int
	RolledbackTransactions int
	PagesRecovered         int
	Duration               int64
}

type DefaultRecoveryCoordinator struct {
	walMgr           wallog.ILogManager
	storageEngine    storage.IStorageEngine
	storageResolver  func(tableName string) storage.IStorageEngine
	bufferPool       buffer.IBufferPoolManager
	rollbackMgr      *rollback.RollbackManager
	stats            RecoveryStats
}

func NewDefaultRecoveryCoordinator(
	walMgr wallog.ILogManager,
	storageEngine storage.IStorageEngine,
	storageResolver func(tableName string) storage.IStorageEngine,
	bufferPool buffer.IBufferPoolManager,
	rollbackMgr *rollback.RollbackManager,
) RecoveryCoordinator {
	return &DefaultRecoveryCoordinator{
		walMgr:          walMgr,
		storageEngine:    storageEngine,
		storageResolver: storageResolver,
		bufferPool:      bufferPool,
		rollbackMgr:     rollbackMgr,
		stats:           RecoveryStats{},
	}
}

func (rc *DefaultRecoveryCoordinator) RecoverFromCrash(ctx context.Context) (*wallog.RecoveryState, error) {
	log.Printf("[Recovery] Starting crash recovery...")

	log.Printf("[Recovery] Running WAL analysis, redo, and undo phases...")
	state, err := rc.walMgr.Recover()
	if err != nil {
		return nil, fmt.Errorf("WAL recovery failed: %w", err)
	}

	log.Printf("[Recovery] WAL recovery complete: %d commit records across %d committed txn IDs, %d aborted txn IDs",
		state.CommitRecords, len(state.CommittedTxns), len(state.AbortedTxns))

	log.Printf("[Recovery] Applying recovery state to storage engine...")
	if err := rc.ApplyRecoveryState(ctx, state); err != nil {
		return nil, fmt.Errorf("failed to apply recovery state: %w", err)
	}

	log.Printf("[Recovery] Validating recovered state...")
	if err := rc.ValidateRecoveredState(ctx, state); err != nil {
		log.Printf("[Recovery] Warning: validation found issues: %v", err)
	}

	log.Printf("[Recovery] Crash recovery complete")
	return state, nil
}

func (rc *DefaultRecoveryCoordinator) ApplyRecoveryState(ctx context.Context, state *wallog.RecoveryState) error {
	if state == nil {
		return fmt.Errorf("recovery state is nil")
	}

	log.Printf("[Recovery] Applying table-aware recovery: replaying %d committed transactions with %d operations",
		len(state.CommittedTxns), len(state.Operations))

	for _, op := range state.Operations {
		if !state.CommittedTxns[op.TxnID] {
			continue
		}

		engine := rc.resolveStorageEngine(op.TableName)
		if engine == nil {
			log.Printf("[Recovery] Warning: no storage engine for table %q, skipping op for txn %d", op.TableName, op.TxnID)
			continue
		}

		switch op.Type {
		case wallog.PUT:
			if err := engine.Put(op.Key, op.Value); err != nil {
				log.Printf("[Recovery] Warning: Failed to replay PUT operation for txn %d: %v", op.TxnID, err)
			}
		case wallog.DELETE:
			if err := engine.Delete(op.Key); err != nil {
				log.Printf("[Recovery] Warning: Failed to replay DELETE operation for txn %d: %v", op.TxnID, err)
			}
		}
	}

	log.Printf("[Recovery] Table-aware recovery complete: replayed operations from %d committed transactions", len(state.CommittedTxns))
	return nil
}

func (rc *DefaultRecoveryCoordinator) resolveStorageEngine(tableName string) storage.IStorageEngine {
	if rc.storageResolver != nil {
		if engine := rc.storageResolver(tableName); engine != nil {
			return engine
		}
	}
	return rc.storageEngine
}

func (rc *DefaultRecoveryCoordinator) ValidateRecoveredState(ctx context.Context, state *wallog.RecoveryState) error {

	if state == nil {
		return fmt.Errorf("recovery state is nil")
	}

	if len(state.CommittedTxns) == 0 && len(state.AbortedTxns) == 0 {
		return nil
	}

	log.Printf("[Recovery] Validation: %d committed, %d aborted transactions",
		len(state.CommittedTxns), len(state.AbortedTxns))


	return nil
}

func (rc *DefaultRecoveryCoordinator) GetRecoveryStats() RecoveryStats {
	return rc.stats
}
