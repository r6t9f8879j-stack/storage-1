package api

import (
	"net/http"
	"time"
)

// Admin endpoints are for maintenance and failover orchestration (admin scope).

func (s *Server) handleAdminGC(w http.ResponseWriter, r *http.Request) {
	ref, err := s.meta.ReferencedHashes()
	if err != nil {
		s.handleErr(w, err)
		return
	}
	// drop stale upload sessions + their staging files
	cutoff := time.Now().Add(-s.cfg.UploadTTL())
	stale, err := s.meta.StaleUploadIDs(cutoff)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	for _, id := range stale {
		_ = s.store.AbortTmp(id)
		_ = s.store.AbortTmp(id + ".mp")
		parts, perr := s.meta.ListParts(id)
		if perr == nil {
			for _, p := range parts {
				ref[p.BlobHash] = true // keep while session may be resumed elsewhere? no: session is stale
			}
		}
		_ = s.meta.DeleteUpload(id)
	}
	removed, freed, err := s.store.GarbageCollect(ref, s.cfg.GCGrace())
	if err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"blobs_removed": removed, "bytes_freed": freed, "stale_sessions": len(stale)})
}

func (s *Server) handleAdminCheckpoint(w http.ResponseWriter, r *http.Request) {
	if err := s.meta.Checkpoint(); err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"checkpointed": true})
}

func (s *Server) handleAdminPromote(w http.ResponseWriter, r *http.Request) {
	if _, err := s.meta.TakeLease(s.cfg.NodeID); err != nil {
		s.handleErr(w, err)
		return
	}
	s.Promote()
	writeJSON(w, 200, E{"node": s.cfg.NodeID, "writes": true})
}

func (s *Server) handleAdminDemote(w http.ResponseWriter, r *http.Request) {
	_, _ = s.meta.ReleaseLease(s.cfg.NodeID)
	s.Demote()
	writeJSON(w, 200, E{"node": s.cfg.NodeID, "writes": false})
}