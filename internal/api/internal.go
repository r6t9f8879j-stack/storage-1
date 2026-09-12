package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"storaged/internal/meta"
)

// The /_internal/* endpoints talk only to the sibling node (peer secret).
// They drive oplog replication and blob/material recovery.

func (s *Server) handleInternalHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, E{
		"node_id":     s.cfg.NodeID,
		"role":        s.role.role,
		"writes":      s.WritesAllowed(),
		"lsn":         mustInt64(s.meta.LastLSN()),
		"watermark":   mustInt64(s.meta.Watermark()),
		"disk_free":   mustDiskFree(s.cfg.DataDir),
		"active_uploads": s.activeUploads.Load(),
		"uptime_sec":     int64(time.Since(s.startedAt).Seconds()),
	})
}

func (s *Server) handleInternalOplog(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ops, err := s.meta.OplogSince(since, limit)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if ops == nil {
		ops = []meta.Op{}
	}
	last := int64(0)
	if len(ops) > 0 {
		last = ops[len(ops)-1].LSN
	}
	writeJSON(w, 200, E{"ops": ops, "last_lsn": last, "watermark": mustInt64(s.meta.Watermark())})
}

// applyResult is what the follower needs to proceed.

// inventory is the full reconciliation snapshot.
type inventoryEntry struct {
	Bucket   string `json:"bucket"`
	Key      string `json:"key"`
	BlobHash string `json:"blob_hash"`
	Size     int64  `json:"size"`
}

func (s *Server) handleInternalInventory(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.meta.ListBuckets(r.Context())
	if err != nil {
		s.handleErr(w, err)
		return
	}
	var objects []inventoryEntry
	for _, b := range buckets {
		after := ""
		for {
			entries, next, err := s.meta.ListObjects(r.Context(), b.Name, meta.ListFilter{After: after, Limit: 1000})
			if err != nil {
				s.handleErr(w, err)
				return
			}
			for _, e := range entries {
				objects = append(objects, inventoryEntry{
					Bucket: e.Bucket, Key: e.Key, BlobHash: e.BlobHash, Size: e.Size,
				})
			}
			if next == "" {
				break
			}
			after = next
		}
	}
	if objects == nil {
		objects = []inventoryEntry{}
	}
	writeJSON(w, 200, E{
		"node_id": s.cfg.NodeID,
		"lsn":     mustInt64(s.meta.LastLSN()),
		"watermark": mustInt64(s.meta.Watermark()),
		"buckets": buckets,
		"objects": objects,
	})
}

func (s *Server) handleInternalBlob(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	if len(hash) != 64 {
		writeJSON(w, 400, E{"error": "invalid blob hash"})
		return
	}
	f, err := s.store.OpenBlob(hash)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		s.handleErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(st.Size(), 10))
	if _, err := io.Copy(w, f); err != nil {
		s.logger.Printf("blob stream %s interrupted: %v", hash, err)
	}
}

func (s *Server) handleInternalLease(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		NodeID string `json:"node_id"`
		Force  bool   `json:"force"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.NodeID == "" {
		writeJSON(w, 400, E{"error": "node_id required"})
		return
	}
	holder, _ := s.meta.LeaseHolder()
	if holder != "" && holder != payload.NodeID && !payload.Force {
		writeJSON(w, 409, E{"holder": holder})
		return
	}
	acquired, err := s.meta.TakeLease(payload.NodeID)
	if err != nil {
		writeJSON(w, 500, E{"error": err.Error()})
		return
	}
	if acquired {
		s.Promote()
	}
	writeJSON(w, 200, E{"holder": payload.NodeID, "acquired": acquired, "writes": s.WritesAllowed()})
}

// ---- small helpers ----

func mustInt64(v int64, err error) int64 {
	if err != nil {
		return 0
	}
	return v
}

func mustDiskFree(p string) int64 {
	v, err := DiskFree(p)
	if err != nil {
		return -1
	}
	return v
}