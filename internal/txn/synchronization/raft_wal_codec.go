package synchronization

import (
	"encoding/json"
	"fmt"

	txnlog "github.com/rodrigo0345/omag/internal/txn/log"
)

const raftWALTableName = "__raft__"

const (
	raftWALKindState byte = iota + 1
	raftWALKindLogAppend
	raftWALKindCommit
)

type raftPersistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

type raftWALLogAppend struct {
	Index    uint64              `json:"index"`
	Term     uint64              `json:"term"`
	Envelope ReplicationEnvelope `json:"envelope"`
}

type raftWALCommit struct {
	CommitIndex uint64 `json:"commit_index"`
}

type raftWALEvent struct {
	Kind      byte
	State     *raftPersistentState
	LogAppend *raftWALLogAppend
	Commit    *raftWALCommit
}

func buildRaftStateWALRecord(txnID uint64, state raftPersistentState) (txnlog.WALRecord, error) {
	payload, err := json.Marshal(state)
	if err != nil {
		return txnlog.WALRecord{}, err
	}
	return txnlog.WALRecord{
		TxnID:     txnID,
		TableName: raftWALTableName,
		Type:      txnlog.REPLICATION_INTENT,
		Before:    []byte{raftWALKindState},
		After:     payload,
	}, nil
}

func buildRaftAppendWALRecord(txnID uint64, appendEntry raftWALLogAppend) (txnlog.WALRecord, error) {
	payload, err := json.Marshal(appendEntry)
	if err != nil {
		return txnlog.WALRecord{}, err
	}
	return txnlog.WALRecord{
		TxnID:     txnID,
		TableName: raftWALTableName,
		Type:      txnlog.REPLICATION_INTENT,
		Before:    []byte{raftWALKindLogAppend},
		After:     payload,
	}, nil
}

func buildRaftCommitWALRecord(txnID uint64, commit raftWALCommit) (txnlog.WALRecord, error) {
	payload, err := json.Marshal(commit)
	if err != nil {
		return txnlog.WALRecord{}, err
	}
	return txnlog.WALRecord{
		TxnID:     txnID,
		TableName: raftWALTableName,
		Type:      txnlog.REPLICATION_INTENT,
		Before:    []byte{raftWALKindCommit},
		After:     payload,
	}, nil
}

func decodeRaftWALEvent(record txnlog.WALRecord) (raftWALEvent, bool, error) {
	if record.TableName != raftWALTableName || record.Type != txnlog.REPLICATION_INTENT {
		return raftWALEvent{}, false, nil
	}
	if len(record.Before) == 0 {
		return raftWALEvent{}, true, fmt.Errorf("raft wal record missing kind")
	}

	kind := record.Before[0]
	event := raftWALEvent{Kind: kind}
	switch kind {
	case raftWALKindState:
		var state raftPersistentState
		if err := json.Unmarshal(record.After, &state); err != nil {
			return raftWALEvent{}, true, err
		}
		event.State = &state
	case raftWALKindLogAppend:
		var appendEntry raftWALLogAppend
		if err := json.Unmarshal(record.After, &appendEntry); err != nil {
			return raftWALEvent{}, true, err
		}
		event.LogAppend = &appendEntry
	case raftWALKindCommit:
		var commit raftWALCommit
		if err := json.Unmarshal(record.After, &commit); err != nil {
			return raftWALEvent{}, true, err
		}
		event.Commit = &commit
	default:
		return raftWALEvent{}, true, fmt.Errorf("unknown raft wal event kind %d", kind)
	}
	return event, true, nil
}
