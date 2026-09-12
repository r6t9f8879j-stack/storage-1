package api

import (
	"net/http"

	"storaged/internal/meta"
)

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := s.meta.ListBuckets(r.Context())
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if buckets == nil {
		buckets = []meta.Bucket{}
	}
	writeJSON(w, 200, E{"buckets": buckets})
}

func (s *Server) handlePutBucket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("b")
	if err := meta.ValidBucket(name); err != nil {
		s.handleErr(w, err)
		return
	}
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}
	public := r.URL.Query().Get("public") == "true"
	if err := s.meta.PutBucket(name, public); err != nil {
		s.handleErr(w, err)
		return
	}
	b, err := s.meta.GetBucket(name)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"bucket": b})
}

func (s *Server) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	b, err := s.meta.GetBucket(r.PathValue("b"))
	if err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"bucket": b})
}

func (s *Server) handleDeleteBucket(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("b")
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}
	n, err := s.meta.DeleteBucket(name)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if n == 0 {
		s.handleErr(w, meta.ErrNotFound)
		return
	}
	w.WriteHeader(204)
}