// Package debug collects recent application logs and HTTP access entries in a
// bounded ring so they can be inspected at runtime via the /v1/debug/* API.
package debug

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// Entry is a single ring-buffer record.
type Entry struct {
	Ts         time.Time `json:"ts"`
	Kind       string    `json:"kind"` // "app" or "access"
	Line       string    `json:"line,omitempty"`
	Method     string    `json:"method,omitempty"`
	Path       string    `json:"path,omitempty"`
	Remote     string    `json:"remote,omitempty"`
	Status     int       `json:"status,omitempty"`
	DurationMs int64     `json:"duration_ms,omitempty"`
}

type ring struct {
	mu      sync.Mutex
	entries []Entry
	cap     int
}

var buf = &ring{cap: 800}

func (r *ring) add(e Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, e)
	if len(r.entries) > r.cap {
		r.entries = r.entries[len(r.entries)-r.cap:]
	}
}

// Write implements io.Writer: a line-oriented sink for the standard logger.
func (r *ring) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if s := strings.TrimSpace(line); s != "" {
			r.add(Entry{Ts: time.Now(), Kind: "app", Line: s})
		}
	}
	return len(p), nil
}

// AddAccess records one finished HTTP request.
func AddAccess(method, path, remote string, status int, dur time.Duration) {
	buf.add(Entry{Ts: time.Now(), Kind: "access", Method: method, Path: path, Remote: remote, Status: status, DurationMs: dur.Milliseconds()})
}

// Tail returns the newest n entries, oldest first.
func Tail(n int) []Entry {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	if len(buf.entries) < n {
		n = len(buf.entries)
	}
	if n <= 0 {
		return []Entry{}
	}
	out := make([]Entry, n)
	copy(out, buf.entries[len(buf.entries)-n:])
	return out
}

// Len returns how many entries are currently buffered.
func Len() int {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	return len(buf.entries)
}

// Clear empties the buffer (used by debug admin action).
func Clear() {
	buf.mu.Lock()
	defer buf.mu.Unlock()
	buf.entries = nil
}

// Attach routes the standard logger into the ring buffer as well as stderr.
// Must be called before package-internal loggers capture log.Default().Writer().
func Attach() {
	log.SetOutput(&logTee{rw: buf})
}

type logTee struct {
	rw *ring
}

func (t *logTee) Write(p []byte) (int, error) {
	_, _ = os.Stderr.Write(p)
	return t.rw.Write(p)
}