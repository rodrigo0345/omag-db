package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteBenchmarkPDF(t *testing.T) {
	tempDir := t.TempDir()
	output := filepath.Join(tempDir, "report.pdf")

	report := benchmarkReport{
		GeneratedAt:   time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC),
		HostOS:        "linux",
		HostArch:      "amd64",
		GoVersion:     "go1.25.1",
		EngineImage:   "omag-benchmark-engine:latest",
		PostgresImage: "postgres:16-alpine",
		SeedRows:      8,
		WarmupOps:     2,
		MeasuredOps:   4,
		Results: []workloadResult{
			{Backend: backendEngine, Workload: workloadReads, Throughput: 123.45, Avg: 10 * time.Millisecond, P50: 9 * time.Millisecond, P95: 12 * time.Millisecond, P99: 15 * time.Millisecond, Min: 8 * time.Millisecond, Max: 16 * time.Millisecond},
			{Backend: backendPostgres, Workload: workloadWrites, Throughput: 234.56, Avg: 8 * time.Millisecond, P50: 7 * time.Millisecond, P95: 10 * time.Millisecond, P99: 11 * time.Millisecond, Min: 6 * time.Millisecond, Max: 13 * time.Millisecond},
		},
			CPUProfileSeconds: 10,
			CPUProfilePath:    "/tmp/omag-engine.pprof",
			CPUHotspots: []cpuHotspot{
				{Function: "github.com/rodrigo0345/omag/internal/storage/lsm.(*LSMTreeBackend).Get", FlatPercent: 27.5, CumPercent: 42.1, AmdahlMaxSpeedup: 1.38, Amdahl2xSpeedup: 1.16},
			},
	}

	if err := writeBenchmarkPDF(output, report); err != nil {
		t.Fatalf("writeBenchmarkPDF() error = %v", err)
	}

	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read output = %v", err)
	}
	if !strings.HasPrefix(string(data), "%PDF-") {
		t.Fatalf("expected PDF header, got %q", string(data[:8]))
	}
	if len(data) < 200 {
		t.Fatalf("pdf output too small: %d bytes", len(data))
	}
	if !strings.Contains(string(data), "ENGINE CPU HOTSPOTS") {
		t.Fatalf("expected cpu hotspot section in rendered report")
	}
}

