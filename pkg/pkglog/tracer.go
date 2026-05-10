package pkglog

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type TraceEvent struct {
	Message   string
	File      string
	Line      int
	Timestamp time.Time
}

type Tracer struct {
	mu     sync.Mutex
	events []TraceEvent
}

func NewTracer() *Tracer {
	return &Tracer{
		events: make([]TraceEvent, 0),
	}
}

func (t *Tracer) Add(msg string) {
	// runtime.Caller(1) skips 1 level of the call stack (this Add function)
	// to get the file and line of whoever called Add.
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		line = 0
	}

	// Optional: Get just the filename instead of the full absolute path
	shortFile := filepath.Base(file)

	t.mu.Lock()
	defer t.mu.Unlock()

	t.events = append(t.events, TraceEvent{
		Message:   msg,
		File:      shortFile,
		Line:      line,
		Timestamp: time.Now(),
	})
}

// Clear wipes the current trace history.
func (t *Tracer) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Re-slice to keep underlying capacity, or use `t.events = nil`
	t.events = t.events[:0]
}

// PrintTrace is a helper to output the trace nicely.
func (t *Tracer) PrintTrace() {
	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Println("=== Execution Trace ===")
	for i, event := range t.events {
		fmt.Printf("[%d] %s:%d | %s | %s\n",
			i+1, event.File, event.Line, event.Timestamp.Format("15:04:05.000"), event.Message)
	}
	fmt.Println("=======================")
}
