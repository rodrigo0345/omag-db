package synchronization

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

var ErrStaleRaftTerm = fmt.Errorf("stale raft term")

// RaftReplicationCoordinator enforces leader-directed writes and quorum-style replication.
type RaftReplicationCoordinator struct {
	base *TransportReplicationCoordinator

	mu         sync.RWMutex
	term       atomic.Uint64
	localNode  string
	leaderNode string
}

func NewRaftReplicationCoordinator(config ReplicationConfig, communicator NodeCommunicator) *RaftReplicationCoordinator {
	config.Strategy = SyncStrategyRaft
	if config.ReadPolicy == SyncPolicyLocal {
		config.ReadPolicy = SyncPolicySynchronous
	}
	if config.WritePolicy == SyncPolicyLocal {
		config.WritePolicy = SyncPolicyQuorum
	}
	if config.MinWriteAcks <= 0 {
		config.MinWriteAcks = 1
	}
	r := &RaftReplicationCoordinator{base: NewTransportReplicationCoordinator(config, communicator)}
	r.SetLeadership(config.LocalNodeID, config.LeaderNodeID, config.CurrentTerm)
	return r
}

func (r *RaftReplicationCoordinator) Strategy() SyncStrategy { return SyncStrategyRaft }

func (r *RaftReplicationCoordinator) Configure(config ReplicationConfig) error {
	config.Strategy = SyncStrategyRaft
	if config.ReadPolicy == SyncPolicyLocal {
		config.ReadPolicy = SyncPolicySynchronous
	}
	if config.WritePolicy == SyncPolicyLocal {
		config.WritePolicy = SyncPolicyQuorum
	}
	if config.MinWriteAcks <= 0 {
		config.MinWriteAcks = 1
	}
	if err := r.base.Configure(config); err != nil {
		return err
	}
	r.SetLeadership(config.LocalNodeID, config.LeaderNodeID, config.CurrentTerm)
	return nil
}

func (r *RaftReplicationCoordinator) ConnectNode(ctx context.Context, endpoint NodeEndpoint) error {
	return r.base.ConnectNode(ctx, endpoint)
}

func (r *RaftReplicationCoordinator) DisconnectNode(ctx context.Context, nodeID string) error {
	return r.base.DisconnectNode(ctx, nodeID)
}

func (r *RaftReplicationCoordinator) ConnectedNodes() []NodeEndpoint { return r.base.ConnectedNodes() }

func (r *RaftReplicationCoordinator) SetLeadership(localNodeID string, leaderNodeID string, term uint64) {
	_ = r.UpdateLeadership(localNodeID, leaderNodeID, term)
}

// UpdateLeadership applies runtime leadership changes while enforcing Raft term monotonicity.
func (r *RaftReplicationCoordinator) UpdateLeadership(localNodeID string, leaderNodeID string, term uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	currentTerm := r.term.Load()
	if currentTerm > 0 && term > 0 && term < currentTerm {
		return fmt.Errorf("%w: current=%d update=%d", ErrStaleRaftTerm, currentTerm, term)
	}

	if term > 0 {
		r.term.Store(term)
	}
	if localNodeID != "" {
		r.localNode = localNodeID
	}
	// Empty leader is allowed (election in progress / leader crash observed).
	r.leaderNode = leaderNodeID
	return nil
}

func (r *RaftReplicationCoordinator) Leadership() (localNodeID string, leaderNodeID string, term uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.localNode, r.leaderNode, r.term.Load()
}

func (r *RaftReplicationCoordinator) assertWriteLeader() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.localNode == "" || r.leaderNode == "" {
		return fmt.Errorf("raft write rejected: leadership is not configured")
	}
	if r.localNode != r.leaderNode {
		return fmt.Errorf("raft write rejected: local node %q is not leader %q", r.localNode, r.leaderNode)
	}
	return nil
}

func (r *RaftReplicationCoordinator) assertReadLeader() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.localNode == "" || r.leaderNode == "" {
		return fmt.Errorf("raft read rejected: leadership is not configured")
	}
	if r.localNode != r.leaderNode {
		return fmt.Errorf("raft read rejected: local node %q is not leader %q", r.localNode, r.leaderNode)
	}
	return nil
}

func (r *RaftReplicationCoordinator) SynchronizeRead(ctx context.Context, txnID int64, key []byte) error {
	if err := r.assertReadLeader(); err != nil {
		return err
	}
	return r.base.SynchronizeRead(ctx, txnID, key)
}

func (r *RaftReplicationCoordinator) ReplicateWrite(ctx context.Context, txnID int64, key []byte, value []byte) error {
	if err := r.assertWriteLeader(); err != nil {
		return err
	}
	return r.base.ReplicateWrite(ctx, txnID, key, value)
}

func (r *RaftReplicationCoordinator) ReplicateDelete(ctx context.Context, txnID int64, key []byte) error {
	if err := r.assertWriteLeader(); err != nil {
		return err
	}
	return r.base.ReplicateDelete(ctx, txnID, key)
}

func (r *RaftReplicationCoordinator) Commit(ctx context.Context, txnID int64) error {
	if err := r.assertWriteLeader(); err != nil {
		return err
	}
	return r.base.Commit(ctx, txnID)
}

func (r *RaftReplicationCoordinator) Abort(ctx context.Context, txnID int64) error {
	return r.base.Abort(ctx, txnID)
}

func (r *RaftReplicationCoordinator) Close() error { return r.base.Close() }
