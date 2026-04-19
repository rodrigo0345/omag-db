package synchronization

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type testRaftHandler struct{}

func (testRaftHandler) RequestVote(ctx context.Context, req RaftRequestVoteRequest) (RaftRequestVoteResponse, error) {
	_ = ctx
	if req.Term == 0 {
		return RaftRequestVoteResponse{}, errors.New("term must be greater than zero")
	}
	return RaftRequestVoteResponse{Term: req.Term, VoteGranted: req.CandidateID == "n1"}, nil
}

func (testRaftHandler) AppendEntries(ctx context.Context, req RaftAppendEntriesRequest) (RaftAppendEntriesResponse, error) {
	_ = ctx
	if req.Term == 0 {
		return RaftAppendEntriesResponse{}, errors.New("term must be greater than zero")
	}
	return RaftAppendEntriesResponse{Term: req.Term, Success: req.LeaderID != ""}, nil
}

func TestDefaultGRPCUnaryInvoker_InvokeRoundTrip(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = listener.Close() })

	called := false
	server := grpc.NewServer(grpc.ForceServerCodec(grpcJSONCodec{}))
	RegisterGRPCReplicationService(server, func(ctx context.Context, envelope ReplicationEnvelope) error {
		called = envelope.Op == ReplicationOpWrite && envelope.TxnID == 42 && string(envelope.Key) == "k"
		return nil
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	invoker := NewDefaultGRPCUnaryInvoker(GRPCUnaryInvokerOptions{
		AddressResolver: func(endpoint NodeEndpoint) (string, error) {
			if endpoint.NodeID == "" {
				return "", errors.New("missing node id")
			}
			return "bufnet", nil
		},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(grpcJSONCodec{})),
		},
	})
	t.Cleanup(func() { _ = invoker.Close() })

	comm := NewGRPCCommunicator(invoker)
	if err := comm.Send(context.Background(), NodeEndpoint{NodeID: "n1"}, ReplicationEnvelope{
		TxnID: 42,
		Op:    ReplicationOpWrite,
		Key:   []byte("k"),
		Value: []byte("v"),
	}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if !called {
		t.Fatal("expected grpc handler to be called")
	}
}

func TestListenAndServeGRPCReplication_ShutsDownWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	errCh := make(chan error, 1)

	go func() {
		close(ready)
		errCh <- ListenAndServeGRPCReplication(ctx, "127.0.0.1:0", func(ctx context.Context, envelope ReplicationEnvelope) error {
			return nil
		})
	}()

	<-ready
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServeGRPCReplication() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for grpc replication server shutdown")
	}
}

func TestDefaultGRPCUnaryInvoker_RaftRoundTrip(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = listener.Close() })

	server := grpc.NewServer(grpc.ForceServerCodec(grpcJSONCodec{}))
	RegisterGRPCRaftConsensusService(server, testRaftHandler{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	invoker := NewDefaultGRPCUnaryInvoker(GRPCUnaryInvokerOptions{
		AddressResolver: func(endpoint NodeEndpoint) (string, error) {
			if endpoint.NodeID == "" {
				return "", errors.New("missing node id")
			}
			return "bufnet", nil
		},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
				_ = ctx
				_ = s
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.ForceCodec(grpcJSONCodec{})),
		},
	})
	t.Cleanup(func() { _ = invoker.Close() })

	voteResp, err := invoker.RequestVote(context.Background(), NodeEndpoint{NodeID: "n2"}, RaftRequestVoteRequest{
		Term:        3,
		CandidateID: "n1",
	})
	if err != nil {
		t.Fatalf("RequestVote() error = %v", err)
	}
	if !voteResp.VoteGranted {
		t.Fatal("RequestVote() vote_granted = false, want true")
	}

	appendResp, err := invoker.AppendEntries(context.Background(), NodeEndpoint{NodeID: "n2"}, RaftAppendEntriesRequest{
		Term:     3,
		LeaderID: "n1",
	})
	if err != nil {
		t.Fatalf("AppendEntries() error = %v", err)
	}
	if !appendResp.Success {
		t.Fatal("AppendEntries() success = false, want true")
	}
}
