// Package transfer implements put.io-style remote URL fetching: a background
// worker downloads bytes from public URLs straight into the content-addressed
// blob store.
//
// Transfers are writer-local state (like upload sessions): they are created on
// the node that currently holds the write lease, and completed transfers put
// objects normally, so replicas converge through the normal oplog.
package transfer

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/anacrolix/torrent"

	"storaged/internal/config"
	"storaged/internal/meta"
	"storaged/internal/store"
)

// Status values persisted in the transfers table.
const (
	StatusQueued      = "queued"
	StatusDownloading = "downloading"
	StatusDone        = "done"
	StatusFailed      = "failed"
	StatusCancelled   = "cancelled"
)

// MaxConcurrent is how many downloads may run at once (SQLite serializes the
// metadata writes anyway; this bounds outbound bandwidth).
const MaxConcurrent = 2

// StuckRequeueAfter requeues a "downloading" transfer this old — e.g. left
// over from a hard kill or a cancelled HTTP stream.
const StuckRequeueAfter = 15 * time.Minute

// Manager downloads queued transfer jobs on the local node.
type Manager struct {
	cfg      *config.Config
	meta     *meta.Meta
	sto      *store.Store
	canWrite func() bool
	log      *log.Logger

	client *http.Client

	// shared BitTorrent client, created lazily on the first torrent transfer.
	tclient   *torrent.Client
	tclientMu sync.Mutex
	tdir      string

	notify chan struct{}

	mu      sync.Mutex
	running map[string]context.CancelFunc
}

// NewManager builds a transfer manager. canWrite reports whether this node
// currently holds the write lease; downloads refuse to commit when it flips.
func NewManager(cfg *config.Config, m *meta.Meta, st *store.Store, canWrite func() bool) *Manager {
	return &Manager{
		cfg:      cfg,
		meta:     m,
		sto:      st,
		canWrite: canWrite,
		log:      log.New(log.Default().Writer(), "[transfer] ", log.LstdFlags),
		client: &http.Client{
			// No overall timeout: big downloads stream for a long time. The
			// per-transfer context bounds them.
			Timeout: 0,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
		notify:  make(chan struct{}, 1),
		running: map[string]context.CancelFunc{},
	}
}

// Notify wakes the worker up (a transfer was queued).
func (m *Manager) Notify() {
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

// Cancel stops a running download for id (idempotent).
func (m *Manager) Cancel(id string) {
	m.mu.Lock()
	cancel, ok := m.running[id]
	if ok {
		delete(m.running, id)
	}
	m.mu.Unlock()
	if ok {
		cancel()
	}
}

// Start runs the worker loop until ctx is cancelled.
func (m *Manager) Start(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.closeTorrentClient()
			return
		case <-m.notify:
			m.drain(ctx)
		case <-t.C:
			m.drain(ctx)
		}
	}
}

// drain requeues crashed downloads (when we're the writer) and starts queued
// jobs up to the concurrency cap.
func (m *Manager) drain(ctx context.Context) {
	if !m.canWrite() {
		return
	}
	if n, err := m.meta.RequeueStuckTransfers(time.Now().Add(-StuckRequeueAfter)); err == nil && n > 0 {
		m.log.Printf("requeued %d stuck transfers", n)
	}
	queued, err := m.meta.QueuedTransfers(MaxConcurrent)
	if err != nil {
		m.log.Printf("queued pull: %v", err)
		return
	}
	for _, t := range queued {
		m.mu.Lock()
		_, already := m.running[t.ID]
		m.mu.Unlock()
		if already {
			continue
		}
		ctx, cancel := context.WithCancel(ctx)
		m.mu.Lock()
		m.running[t.ID] = cancel
		m.mu.Unlock()
		go func(t meta.Transfer) {
			defer func() {
				m.mu.Lock()
				delete(m.running, t.ID)
				m.mu.Unlock()
			}()
			m.download(ctx, t)
		}(t)
	}
}

// download runs one transfer job, setting terminal status on failure.
func (m *Manager) download(ctx context.Context, t meta.Transfer) {
	if err := m.meta.SetTransferStatus(t.ID, StatusDownloading, ""); err != nil {
		m.log.Printf("transfer %s: %v", t.ID, err)
		return
	}
	var err error
	if t.Kind == meta.KindTorrent {
		err = m.downloadTorrent(ctx, t)
	} else {
		err = m.downloadURL(ctx, t)
	}
	if err != nil {
		if ctx.Err() != nil {
			_ = m.meta.SetTransferStatus(t.ID, StatusCancelled, "")
		} else {
			_ = m.meta.SetTransferStatus(t.ID, StatusFailed, truncate(err.Error(), 500))
		}
		m.log.Printf("transfer %s (%s) failed: %v", t.ID, t.Key, err)
	}
}

// downloadURL performs the HTTP fetch and, on success, commits a new object.
func (m *Manager) downloadURL(ctx context.Context, t meta.Transfer) error {
	_ = m.sto.AbortTmp(t.ID) // discard any partial from a crashed earlier run
	blob, err := m.fetch(ctx, t)
	if err != nil {
		_ = m.sto.AbortTmp(t.ID)
		return err
	}
	// re-check the lease before committing — we may have been demoted mid-fetch
	if !m.canWrite() {
		_ = m.sto.AbortTmp(t.ID)
		return fmt.Errorf("node is no longer the writer; transfer not committed")
	}
	ct := t.ContentType
	if ct == "" {
		ct = blob.contentType
	}
	obj := &meta.Object{
		Bucket:      t.Bucket,
		Key:         t.Key,
		BlobHash:    blob.hash,
		Size:        blob.size,
		ContentType: ct,
		ETag:        `"` + blob.hash + `"`,
		Metadata:    map[string]string{},
	}
	_ = m.sto.AbortTmp(t.ID)
	if err := m.meta.PutObject(obj); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	_ = m.meta.SetTransferStatus(t.ID, StatusDone, "")
	m.log.Printf("transfer %s fetched %s (%s) -> %s/%s", t.ID, t.URL, byteSize(blob.size), t.Bucket, t.Key)
	return nil
}

type fetchedBlob struct {
	hash        string
	size        int64
	contentType string
}

// fetch streams t.URL into a staging blob, returning its content hash.
func (m *Manager) fetch(ctx context.Context, t meta.Transfer) (*fetchedBlob, error) {
	u, err := url.Parse(t.URL)
	if err != nil {
		return nil, fmt.Errorf("bad url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("only http/https URLs are fetchable, got %q", u.Scheme)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "storaged-transfer/1.0")
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("remote returned %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	var total int64 = resp.ContentLength
	if total > m.cfg.MaxBodyBytes {
		return nil, fmt.Errorf("remote file too large (%.3f GiB > %.3f GiB cap)",
			float64(total)/(1<<30), float64(m.cfg.MaxBodyBytes)/(1<<30))
	}

	tmp, err := m.sto.OpenTmp(t.ID)
	if err != nil {
		return nil, err
	}
	body := io.LimitReader(resp.Body, m.cfg.MaxBodyBytes+1)
	pr := &progressReader{r: body, cb: func(n, sz int64) {
		_ = m.meta.UpdateTransferProgress(t.ID, n, sz)
	}}
	n, copyErr := io.CopyBuffer(tmp, pr, make([]byte, 1<<20))
	closeErr := tmp.Close()
	if copyErr != nil {
		_ = m.meta.UpdateTransferProgress(t.ID, n, total)
		return nil, copyErr
	}
	if closeErr != nil {
		_ = m.meta.UpdateTransferProgress(t.ID, n, total)
		return nil, closeErr
	}
	if n > m.cfg.MaxBodyBytes {
		return nil, fmt.Errorf("remote file exceeded %.3f GiB cap", float64(m.cfg.MaxBodyBytes)/(1<<30))
	}
	_ = m.meta.UpdateTransferProgress(t.ID, n, total)
	if n == 0 {
		return nil, fmt.Errorf("remote returned no bytes")
	}
	if total <= 0 {
		total = n
	}
	hash, size, _, err := m.sto.FinalizeTmp(t.ID)
	if err != nil {
		return nil, err
	}
	return &fetchedBlob{hash: hash, size: size, contentType: ct}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func byteSize(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// progressReader reports download progress to the metadata DB (~1 Hz).
type progressReader struct {
	r      io.Reader
	cb     func(done, total int64)
	done   int64
	lastTS time.Time
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.done += int64(n)
	if p.cb != nil && (time.Since(p.lastTS) >= time.Second || err != nil) {
		p.lastTS = time.Now()
		p.cb(p.done, -1)
	}
	return n, err
}
