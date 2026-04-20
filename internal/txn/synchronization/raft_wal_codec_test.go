package synchronization

import (
	"testing"
)

func TestDecodeRaftWALEvent_StateRoundTrip(t *testing.T) {
	rec, err := buildRaftStateWALRecord(1, raftPersistentState{CurrentTerm: 4, VotedFor: "n2"})
	if err != nil {
		t.Fatalf("buildRaftStateWALRecord() error = %v", err)
	}

	event, ok, err := decodeRaftWALEvent(rec)
	if err != nil {
		t.Fatalf("decodeRaftWALEvent() error = %v", err)
	}
	if !ok {
		t.Fatal("expected raft wal record to be recognized")
	}
	if event.State == nil || event.State.CurrentTerm != 4 || event.State.VotedFor != "n2" {
		t.Fatalf("decoded state = %+v, want term=4 voted_for=n2", event.State)
	}
}

func TestDecodeRaftWALEvent_AppendRoundTrip(t *testing.T) {
	rec, err := buildRaftAppendWALRecord(2, raftWALLogAppend{
		Index: 9,
		Term:  5,
		Envelope: ReplicationEnvelope{
			TxnID: 7,
			Op:    ReplicationOpWrite,
			Key:   []byte("k"),
			Value: []byte("v"),
		},
	})
	if err != nil {
		t.Fatalf("buildRaftAppendWALRecord() error = %v", err)
	}
	event, ok, err := decodeRaftWALEvent(rec)
	if err != nil {
		t.Fatalf("decodeRaftWALEvent() error = %v", err)
	}
	if !ok || event.LogAppend == nil {
		t.Fatalf("decodeRaftWALEvent() = (%+v, %v), want log append event", event, ok)
	}
	if event.LogAppend.Index != 9 || event.LogAppend.Term != 5 {
		t.Fatalf("decoded append = %+v, want index=9 term=5", event.LogAppend)
	}
}
