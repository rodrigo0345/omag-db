package synchronization

import "context"

// StrongConsistencyReplicationCoordinator enforces synchronous read/write replication.
type StrongConsistencyReplicationCoordinator struct {
	base *TransportReplicationCoordinator
}

func NewStrongConsistencyReplicationCoordinator(config ReplicationConfig, communicator NodeCommunicator) *StrongConsistencyReplicationCoordinator {
	config.Strategy = SyncStrategyStrongConsistency
	config.ReadPolicy = SyncPolicySynchronous
	config.WritePolicy = SyncPolicySynchronous
	if config.MinWriteAcks <= 0 {
		config.MinWriteAcks = 1
	}
	return &StrongConsistencyReplicationCoordinator{base: NewTransportReplicationCoordinator(config, communicator)}
}

func (s *StrongConsistencyReplicationCoordinator) Strategy() SyncStrategy {
	return SyncStrategyStrongConsistency
}

func (s *StrongConsistencyReplicationCoordinator) Configure(config ReplicationConfig) error {
	config.Strategy = SyncStrategyStrongConsistency
	config.ReadPolicy = SyncPolicySynchronous
	config.WritePolicy = SyncPolicySynchronous
	if config.MinWriteAcks <= 0 {
		config.MinWriteAcks = 1
	}
	return s.base.Configure(config)
}

func (s *StrongConsistencyReplicationCoordinator) ConnectNode(ctx context.Context, endpoint NodeEndpoint) error {
	return s.base.ConnectNode(ctx, endpoint)
}

func (s *StrongConsistencyReplicationCoordinator) DisconnectNode(ctx context.Context, nodeID string) error {
	return s.base.DisconnectNode(ctx, nodeID)
}

func (s *StrongConsistencyReplicationCoordinator) ConnectedNodes() []NodeEndpoint {
	return s.base.ConnectedNodes()
}

func (s *StrongConsistencyReplicationCoordinator) SynchronizeRead(ctx context.Context, txnID int64, key []byte) error {
	return s.base.SynchronizeRead(ctx, txnID, key)
}

func (s *StrongConsistencyReplicationCoordinator) ReplicateWrite(ctx context.Context, txnID int64, key []byte, value []byte) error {
	return s.base.ReplicateWrite(ctx, txnID, key, value)
}

func (s *StrongConsistencyReplicationCoordinator) ReplicateDelete(ctx context.Context, txnID int64, key []byte) error {
	return s.base.ReplicateDelete(ctx, txnID, key)
}

func (s *StrongConsistencyReplicationCoordinator) Commit(ctx context.Context, txnID int64) error {
	return s.base.Commit(ctx, txnID)
}

func (s *StrongConsistencyReplicationCoordinator) Abort(ctx context.Context, txnID int64) error {
	return s.base.Abort(ctx, txnID)
}

func (s *StrongConsistencyReplicationCoordinator) Close() error { return s.base.Close() }
