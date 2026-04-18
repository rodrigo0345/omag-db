package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/rodrigo0345/omag/internal/database"
)

const (
	defaultDBPath     = "./test.db"
	defaultLSMDataDir = "./lsm_data"
	defaultWALPath    = "./test.wal"
)

func main() {
	mode := flag.String("mode", "cli", "cli or server")
	flag.Parse()

	engine, err := database.OpenMVCCLSM(database.Options{
		DBPath:     defaultDBPath,
		LSMDataDir: defaultLSMDataDir,
		WALPath:    defaultWALPath,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to open database:", err)
		os.Exit(1)
	}
	defer engine.Close()

	switch *mode {
	case "cli":
		shell := &sqlShellNew{db: engine}
		if err := shell.Run(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "server":
		srv := NewPostgresServer(engine)
		if err := srv.ListenAndServe(":5432"); err != nil {
			fmt.Fprintln(os.Stderr, "server error:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}
}
