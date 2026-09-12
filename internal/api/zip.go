package api

import (
	"archive/zip"
	"io"
	"net/http"
	"strings"

	"storaged/internal/meta"
)

// handleZip streams a ZIP archive of every non-trashed object matching
// ?prefix= under the requested bucket.  The archive is built on-the-fly so
// arbitrarily large buckets can be served without buffering.  All objects
// retain their full key as the zip entry name; public buckets work too.
//
// Examples:
//
//	GET /v1/buckets/media/zip?prefix=photos/2026/
//	GET /v1/buckets/media/zip?prefix=docs/&name=documents.zip
func (s *Server) handleZip(w http.ResponseWriter, r *http.Request) {
	b := r.PathValue("b")
	if _, err := s.meta.GetBucket(b); err != nil {
		s.handleErr(w, err)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	name := r.URL.Query().Get("name")
	if name == "" {
		seg := prefix
		if i := strings.LastIndexByte(seg, '/'); i >= 0 {
			seg = seg[i+1:]
		}
		if seg == "" {
			seg = b
		}
		name = strings.TrimSpace(seg) + ".zip"
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+strings.ReplaceAll(name, `"`, `\"`)+`"`)
	w.Header().Set("Cache-Control", "private, no-cache")
	zw := zip.NewWriter(w)

	after := ""
	for {
		entries, next, err := s.meta.ListObjects(r.Context(), b, meta.ListFilter{
			Prefix: prefix,
			After:  after,
			Limit:  500,
		})
		if err != nil {
			_ = zw.Close()
			s.handleErr(w, err)
			return
		}
		for _, e := range entries {
			fh := &zip.FileHeader{
				Name:   e.Key,
				Method: zip.Deflate,
			}
			fw, err := zw.CreateHeader(fh)
			if err != nil {
				_ = zw.Close()
				return // client likely disconnected
			}
			fp, err := s.store.OpenBlob(e.BlobHash)
			if err != nil {
				continue
			}
			_, _ = io.Copy(fw, fp)
			_ = fp.Close()
		}
		if next == "" || len(entries) == 0 {
			break
		}
		after = next
	}
	_ = zw.Close()
}
