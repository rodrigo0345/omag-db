package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/pgserver"
	"github.com/rodrigo0345/omag/internal/txn/synchronization"
	applog "github.com/rodrigo0345/omag/pkg/log"
)

const (
	defaultDBPath     = "./ines.db"
	defaultLSMDataDir = "./lsm_data"
	defaultWALPath    = "./ines.wal"
)

func main() {
	listenAddr := flag.String("listen", ":5432", "TCP address to listen on for psql connections")
	dbPath := flag.String("db", defaultDBPath, "path to the database file")
	lsmDataDir := flag.String("lsm-data-dir", defaultLSMDataDir, "directory for LSM table data")
	walPath := flag.String("wal", defaultWALPath, "path to the WAL file")
	replicationStrategy := flag.String("replication-strategy", string(synchronization.SyncStrategyStandalone), "replication strategy (standalone|raft)")
	replicationBackend := flag.String("replication-backend", string(synchronization.ReplicationBackendNoop), "replication backend (noop|maelstrom|grpc)")
	replicationMinWriteAcks := flag.Int("replication-min-write-acks", 1, "minimum acknowledgements required for quorum policies")
	replicationLocalNodeID := flag.String("replication-local-node-id", "", "local node id for raft startup bootstrap")
	replicationLeaderNodeID := flag.String("replication-leader-node-id", "", "leader node id for raft startup bootstrap")
	replicationCurrentTerm := flag.Uint64("replication-current-term", 0, "raft term for startup bootstrap")
	peerNodes := flag.String("replication-peer-nodes", "", "comma-separated peer list: nodeA=host:port,nodeB=host:port")
	grpcListen := flag.String("replication-grpc-listen", "", "address for incoming gRPC replication envelopes, e.g. :7000")
	pprofListen := flag.String("pprof-listen", "", "optional net/http/pprof listen address, e.g. :6060")
	debug := flag.Bool("debug", false, "enable debug logs for pgwire server and engine")
	flag.Parse()

	if *debug {
		applog.SetLevel(applog.LevelDebug)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := synchronization.DefaultReplicationConfig()
	cfg.Strategy = synchronization.SyncStrategy(*replicationStrategy)
	cfg.Backend = synchronization.ReplicationBackend(*replicationBackend)
	cfg.MinWriteAcks = *replicationMinWriteAcks
	cfg.LocalNodeID = strings.TrimSpace(*replicationLocalNodeID)
	cfg.LeaderNodeID = strings.TrimSpace(*replicationLeaderNodeID)
	cfg.CurrentTerm = *replicationCurrentTerm

	if cfg.Strategy != synchronization.SyncStrategyStandalone && cfg.Strategy != synchronization.SyncStrategyRaft {
		_, _ = fmt.Fprintf(os.Stderr, "only standalone and raft replication strategies are available for now\n")
		os.Exit(2)
	}

	// TODO parse peers and provide that to the internal network layer
	/* peers, err := parsePeerNodes(*peerNodes)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "invalid --replication-peer-nodes: %v\n", err)
		os.Exit(1)
	}
	*/

	applog.Info("[PGSERVER] starting OMAG pgwire server listen=%s db=%s lsm=%s wal=%s replication=%s/%s local=%s peers=%d debug=%v", *listenAddr, *dbPath, *lsmDataDir, *walPath, cfg.Strategy, cfg.Backend, cfg.LocalNodeID, 1, *debug)

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:                 *dbPath,
		LSMDataDir:             *lsmDataDir,
		WALPath:                *walPath,
		ReplicationConfig:      cfg,
		ReplicationCoordinator: replicator,
	})
	if err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, "failed to open database:", err); writeErr != nil {
			_ = writeErr
		}
		os.Exit(1)
	}
	defer func() {
		if closeErr := engine.Close(); closeErr != nil {
			_, _ = fmt.Fprintln(os.Stderr, "close error:", closeErr)
		}
	}()

	if cfg.Strategy == synchronization.SyncStrategyRaft {
		if err := engine.UpdateRaftLeadership(cfg.LocalNodeID, cfg.LeaderNodeID, cfg.CurrentTerm); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to bootstrap raft leadership: %v\n", err)
			os.Exit(1)
		}
	}

	if cfg.Backend == synchronization.ReplicationBackendGRPC && *grpcListen != "" {
		applier := newReplicatedTxnApplier(engine)
		go func() {
			err := synchronization.ListenAndServeGRPCReplication(ctx, *grpcListen, applier.ApplyEnvelope)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "replication grpc server error: %v\n", err)
				stop()
			}
		}()
	}

	if strings.TrimSpace(*pprofListen) != "" {
		pprofServer := &http.Server{Addr: strings.TrimSpace(*pprofListen), Handler: http.DefaultServeMux}
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = pprofServer.Shutdown(shutdownCtx)
		}()
		go func() {
			if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				_, _ = fmt.Fprintf(os.Stderr, "pprof server error: %v\n", err)
				stop()
			}
		}()
	}

	srv := pgserver.New(engine)

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, "failed to listen:", err); writeErr != nil {
			_ = writeErr
		}
		os.Exit(1)
	}

	if err := srv.Serve(ctx, ln); err != nil && err != context.Canceled {
		if _, writeErr := fmt.Fprintln(os.Stderr, "server error:", err); writeErr != nil {
			_ = writeErr
		}
		os.Exit(1)
	}
}
