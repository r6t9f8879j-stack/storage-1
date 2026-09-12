package api

import (
	"net/http"

	"storaged/internal/meta"
)

// Public object access (no auth). Only served from buckets marked public.
func (s *Server) handlePublicGet(w http.ResponseWriter, r *http.Request) {
	b, key := r.PathValue("b"), r.PathValue("key")
	if err := meta.ValidKey(key); err != nil {
		s.handleErr(w, err)
		return
	}
	bk, err := s.meta.GetBucket(b)
	if err != nil || !bk.IsPublic {
		// do not reveal whether the bucket exists
		writeJSON(w, 404, E{"error": "not found"})
		return
	}
	obj, err := s.meta.GetObject(b, key)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	streamObject(w, r, s, obj, true, false)
}