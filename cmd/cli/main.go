package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/rodrigo0345/omag/internal/database"
	"github.com/rodrigo0345/omag/internal/pgserver"
	applog "github.com/rodrigo0345/omag/pkg/log"
)

const (
	defaultDBPath     = "./test.db"
	defaultLSMDataDir = "./lsm_data"
	defaultWALPath    = "./test.wal"
)

func main() {
	listenAddr := flag.String("listen", ":5432", "TCP address to listen on for psql connections")
	dbPath := flag.String("db", defaultDBPath, "path to the database file")
	lsmDataDir := flag.String("lsm-data-dir", defaultLSMDataDir, "directory for LSM table data")
	walPath := flag.String("wal", defaultWALPath, "path to the WAL file")
	debug := flag.Bool("debug", false, "enable debug logs for pgwire server and engine")
	flag.Parse()

	if *debug {
		applog.SetLevel(applog.LevelDebug)
	}

	applog.Info("[PGSERVER] starting OMAG pgwire server listen=%s db=%s lsm=%s wal=%s debug=%v", *listenAddr, *dbPath, *lsmDataDir, *walPath, *debug)

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:     *dbPath,
		LSMDataDir: *lsmDataDir,
		WALPath:    *walPath,
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

	srv := pgserver.New(engine)
	if err := srv.ListenAndServe(*listenAddr); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, "server error:", err); writeErr != nil {
			_ = writeErr
		}
		os.Exit(1)
	}
}
