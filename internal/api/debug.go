package api

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"storaged/internal/debug"
)

// disk stats, computed lazily and cached for a few seconds so /v1/debug/stats
// never walks the data dir on every call.
var (
	diskStatBytes atomic.Int64
	diskStatAt    atomic.Int64
)

// handleDebugLogs returns the most recent application + access log entries.
func (s *Server) handleDebugLogs(w http.ResponseWriter, r *http.Request) {
	n := 300
	if q := r.URL.Query().Get("n"); q != "" {
		if v, err := strconv.Atoi(q); err == nil && v > 0 {
			if v > 2000 {
				v = 2000
			}
			n = v
		}
	}
	writeJSON(w, 200, E{"count": debug.Len(), "logs": debug.Tail(n)})
}

// handleDebugStats returns a runtime snapshot: system, node role, meta counts,
// store usage and transfer activity.
func (s *Server) handleDebugStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	totals, err := s.meta.BucketTotals(ctx)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	var bucketCount, objectCount, totalBytes int64
	for _, t := range totals {
		bucketCount++
		objectCount += t.Objects
		totalBytes += t.Size
	}

	trash, _ := s.meta.ListTrashed(ctx, 100000)
	transfers, _ := s.meta.ListTransfers(ctx, "", "")
	blobs, _ := s.store.CountBlobs()
	lsn, _ := s.meta.LastLSN()
	wm, _ := s.meta.Watermark()
	lease, _ := s.meta.LeaseHolder()
	free, _ := DiskFree(s.cfg.DataDir)

	var tq, td, ton, tfail, tcancel int64
	for _, t := range transfers {
		switch t.Status {
		case "queued":
			tq++
		case "downloading":
			td++
		case "done":
			ton++
		case "failed":
			tfail++
		case "cancelled":
			tcancel++
		}
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	writeJSON(w, 200, E{
		"node": E{
			"node_id":        s.cfg.NodeID,
			"role":           s.role.role,
			"writes":         s.WritesAllowed(),
			"listen":         s.cfg.Listen,
			"public_url":     s.cfg.PublicURL,
			"peer_url":       s.cfg.Peer.URL,
			"uptime_sec":     int64(time.Since(s.startedAt).Seconds()),
			"started_at":     s.startedAt.UTC().Format(time.RFC3339),
			"active_uploads": s.activeUploads.Load(),
		},
		"runtime": E{
			"go_version":  runtime.Version(),
			"goroutines":  runtime.NumGoroutine(),
			"mem_alloc":   ms.Alloc,
			"mem_sys":     ms.Sys,
			"heap_alloc":  ms.HeapAlloc,
			"num_gc":      ms.NumGC,
			"arch":        runtime.GOARCH,
			"os":          runtime.GOOS,
		},
		"meta": E{
			"buckets":      bucketCount,
			"objects":      objectCount,
			"size":         totalBytes,
			"trash":        len(trash),
			"last_lsn":     lsn,
			"repl_wm":      wm,
			"lease_holder": lease,
		},
		"store": E{
			"blobs":      blobs,
			"data_bytes": s.diskUsageBytes(),
			"disk_free":  free,
		},
		"transfers": E{
			"queued":       tq,
			"downloading":  td,
			"done":         ton,
			"failed":       tfail,
			"cancelled":    tcancel,
			"total":        int64(len(transfers)),
		},
	})
}

// handleDebugRoutes lists every registered public API route pattern.
func (s *Server) handleDebugRoutes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, E{"routes": debugRoutes()})
}

// debugRoutes is a literal list kept in sync with Handler() so the dashboard
// can advertise exactly what this build exposes.
func debugRoutes() []string {
	return []string{
		"GET      /v1/health",
		"GET      /v1/buckets",
		"PUT      /v1/buckets/{b}",
		"GET      /v1/buckets/{b}",
		"DELETE   /v1/buckets/{b}",
		"GET      /v1/buckets/{b}/objects",
		"PUT      /v1/buckets/{b}/objects/{key...}",
		"GET      /v1/buckets/{b}/objects/{key...}",
		"HEAD     /v1/buckets/{b}/objects/{key...}",
		"DELETE   /v1/buckets/{b}/objects/{key...}",
		"POST     /v1/buckets/{b}/uploads",
		"POST     /v1/buckets/{b}/uploads/commit",
		"POST     /v1/buckets/{b}/uploads/abort",
		"POST     /v1/buckets/{b}/uploads/upload-url",
		"POST     /v1/buckets/{b}/uploads/download-url",
		"POST     /v1/buckets/{b}/uploads/multipart",
		"POST     /v1/buckets/{b}/uploads/multipart/complete",
		"PUT      /v1/multipart/{upload_id}/parts/{part}",
		"DELETE   /v1/multipart/{upload_id}",
		"GET      /v1/public/{b}/{key...}",
		"POST     /v1/admin/gc",
		"POST     /v1/admin/checkpoint",
		"POST     /v1/admin/promote",
		"POST     /v1/admin/demote",
		"POST     /v1/buckets/{b}/transfers",
		"GET      /v1/buckets/{b}/transfers",
		"GET      /v1/transfers/{id}",
		"DELETE   /v1/buckets/{b}/transfers/{id}",
		"POST     /v1/buckets/{b}/transfers/{id}/retry",
		"GET      /v1/trash",
		"POST     /v1/trash/restore",
		"POST     /v1/trash/purge",
		"DELETE   /v1/trash",
		"GET      /v1/buckets/{b}/zip",
		"GET      /v1/debug/logs",
		"GET      /v1/debug/stats",
		"GET      /v1/debug/routes",
		"POST     /v1/auth/signup",
		"POST     /v1/auth/token",
	}
}

func (s *Server) diskUsageBytes() int64 {
	now := time.Now().Unix()
	if now-diskStatAt.Load() < 15 {
		return diskStatBytes.Load()
	}
	var sz int64
	_ = filepath.Walk(s.cfg.DataDir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			sz += info.Size()
		}
		return nil
	})
	diskStatBytes.Store(sz)
	diskStatAt.Store(now)
	return sz
}