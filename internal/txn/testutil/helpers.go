package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TempDir(t *testing.T) string {
	tmpDir, err := os.MkdirTemp("", "omag-test-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})
	return tmpDir
}

func TempFile(t *testing.T, dir, name string) string {
	path := filepath.Join(dir, name)
	return path
}

func CleanupFile(t *testing.T, path string) {
	t.Cleanup(func() {
		os.Remove(path)
	})
}
