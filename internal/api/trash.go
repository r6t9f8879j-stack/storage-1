package api

import (
	"encoding/json"
	"io"
	"net/http"

	"storaged/internal/meta"
)

// Soft-delete trash: objects are moved to a "trashed" state with a timestamp
// instead of being permanently deleted right away, giving users a window to
// restore accidental deletions.  A periodic GC sweep permanently removes
// objects that have been in the trash longer than gc_grace.
//
// Endpoint design mirrors put.io's trash model:
//   GET    /v1/trash              list trashed objects
//   POST   /v1/trash/restore      restore an object (body {bucket,key})
//   POST   /v1/trash/purge        permanently purge one (body {bucket,key})
//   DELETE /v1/trash              purge ALL trashed objects

func (s *Server) handleListTrash(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if q := r.URL.Query().Get("limit"); q != "" {
		var l int
		for _, c := range q {
			if c >= '0' && c <= '9' {
				l = l*10 + int(c-'0')
			}
		}
		if l > 0 && l <= 2000 {
			limit = l
		}
	}
	entries, err := s.meta.ListTrashed(r.Context(), limit)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"entries": entries})
}

func (s *Server) handleRestoreTrash(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, E{"error": "invalid body"})
		return
	}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &body)
	}
	if body.Bucket == "" || body.Key == "" {
		writeJSON(w, 400, E{"error": "bucket and key required"})
		return
	}
	if err := meta.ValidKey(body.Key); err != nil {
		s.handleErr(w, err)
		return
	}
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}
	ok, err := s.meta.RestoreObject(body.Bucket, body.Key)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if !ok {
		writeJSON(w, 404, E{"error": "object not in trash"})
		return
	}
	writeJSON(w, 200, E{"bucket": body.Bucket, "key": body.Key, "status": "restored"})
}

func (s *Server) handlePurgeTrashOne(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, 400, E{"error": "invalid body"})
		return
	}
	if len(data) > 0 {
		_ = json.Unmarshal(data, &body)
	}
	if body.Bucket == "" || body.Key == "" {
		writeJSON(w, 400, E{"error": "bucket and key required"})
		return
	}
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}
	hash, _, deleted, err := s.meta.PurgeObject(body.Bucket, body.Key)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if !deleted {
		writeJSON(w, 404, E{"error": "object not found"})
		return
	}
	// eagerly remove the blob; GC will also clean it up
	if hash != "" {
		_ = s.store.DeleteBlob(hash)
	}
	w.WriteHeader(204)
}

func (s *Server) handlePurgeTrashAll(w http.ResponseWriter, r *http.Request) {
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}
	total := 0
	for {
		entries, err := s.meta.ListTrashed(r.Context(), 100)
		if err != nil {
			s.handleErr(w, err)
			return
		}
		if len(entries) == 0 {
			break
		}
		for _, e := range entries {
			hash, _, deleted, err := s.meta.PurgeObject(e.Bucket, e.Key)
			if err != nil {
				writeJSON(w, 500, E{"error": err.Error(), "purged": total})
				return
			}
			if deleted {
				if hash != "" {
					_ = s.store.DeleteBlob(hash)
				}
				total++
			}
		}
	}
	writeJSON(w, 200, E{"purged": total})
}
