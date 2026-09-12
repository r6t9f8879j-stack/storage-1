package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"storaged/internal/meta"
)

// keyFromRequest resolves an object key from the ?key= query param or a small
// JSON body. Used by session/multipart/sign routes (object keys may contain
// slashes, so they cannot be path segments).
func keyFromRequest(r *http.Request) string {
	if k := r.URL.Query().Get("key"); k != "" {
		return k
	}
	if r.Body == nil {
		return ""
	}
	var b struct {
		Key string `json:"key"`
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err == nil && len(data) > 0 {
		_ = json.Unmarshal(data, &b)
	}
	return b.Key
}

// ---- upload capture ----

// beginOrResumeSession resolves the upload session and staging file state for a
// body-carrying request. Returns the upload_id and the current tmp size.
func (s *Server) beginOrResumeSession(w http.ResponseWriter, r *http.Request, b, key, uploadID, ct string, offset int64) (string, string, int64, bool) {
	if uploadID == "" {
		var err error
		uploadID, err = s.meta.BeginUpload(b, key, ct)
		if err != nil {
			s.handleErr(w, err)
			return "", "", 0, false
		}
	} else {
		gotCT, ok, err := s.meta.GetUpload(b, key, uploadID)
		if err != nil {
			s.handleErr(w, err)
			return "", "", 0, false
		}
		if !ok {
			writeJSON(w, 404, E{"error": "upload session not found"})
			return "", "", 0, false
		}
		if ct == "" {
			ct = gotCT
		} else if gotCT == "" {
			_ = s.meta.SetUploadContentType(uploadID, ct)
		}
	}
	if ct == "" {
		ct = "application/octet-stream"
	}

	cur, err := s.store.TmpSize(uploadID)
	if err != nil {
		s.handleErr(w, err)
		return "", "", 0, false
	}
	if offset > 0 {
		if cur < offset {
			writeJSON(w, 409, E{"error": "resume offset beyond received bytes", "received": cur})
			return "", "", 0, false
		}
		if cur > offset {
			if err := s.store.TruncateTmp(uploadID, offset); err != nil {
				s.handleErr(w, err)
				return "", "", 0, false
			}
		}
	}
	return uploadID, ct, offset, true
}

func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request) {
	b, key := r.PathValue("b"), r.PathValue("key")
	if err := meta.ValidBucket(b); err != nil {
		s.handleErr(w, err)
		return
	}
	if err := meta.ValidKey(key); err != nil {
		s.handleErr(w, err)
		return
	}
	if _, err := s.meta.GetBucket(b); err != nil {
		s.handleErr(w, err)
		return
	}
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}

	// preconditions
	ifNoneMatch := strings.TrimSpace(r.Header.Get("If-None-Match"))
	ifMatch := strings.TrimSpace(r.Header.Get("If-Match"))
	if ifNoneMatch == "*" {
		if _, err := s.meta.GetObject(b, key); err == nil {
			writeJSON(w, 412, E{"error": "precondition failed: object exists"})
			return
		} else if !errors.Is(err, meta.ErrNotFound) {
			s.handleErr(w, err)
			return
		}
	}
	if ifMatch != "" && ifMatch != "*" {
		cur, err := s.meta.GetObject(b, key)
		if err != nil && !errors.Is(err, meta.ErrNotFound) {
			s.handleErr(w, err)
			return
		}
		if err == nil && !etagMatches(ifMatch, cur.ETag) {
			writeJSON(w, 412, E{"error": "precondition failed: If-Match mismatch"})
			return
		}
		if errors.Is(err, meta.ErrNotFound) {
			writeJSON(w, 412, E{"error": "precondition failed: If-Match, object missing"})
			return
		}
	}

	q := r.URL.Query()
	uploadID := q.Get("upload_id")
	offset, _ := strconv.ParseInt(q.Get("offset"), 10, 64)
	final := !strings.EqualFold(r.Header.Get("X-Final"), "false")
	ct := r.Header.Get("Content-Type")

	uploadID, ct, prefix, ok := s.beginOrResumeSession(w, r, b, key, uploadID, ct, offset)
	if !ok {
		return
	}

	s.activeUploads.Add(1)
	defer s.activeUploads.Add(-1)

	tmp, err := s.store.OpenTmp(uploadID)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	body := http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	buf := make([]byte, 1<<20)
	n, copyErr := io.CopyBuffer(tmp, body, buf)
	closeErr := tmp.Close()
	if copyErr != nil {
		s.handleErr(w, copyErr)
		return
	}
	if closeErr != nil {
		s.handleErr(w, closeErr)
		return
	}
	received := prefix + n

	if !final {
		writeJSON(w, 202, E{"upload_id": uploadID, "received": received, "final": false})
		return
	}

	hash, size, etag, err := s.store.FinalizeTmp(uploadID)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	obj := &meta.Object{
		Bucket: b, Key: key, BlobHash: hash, Size: size,
		ContentType: ct, ETag: etag, Metadata: map[string]string{},
	}
	if err := s.meta.PutObject(obj); err != nil {
		s.handleErr(w, err)
		return
	}
	_ = s.meta.DeleteUpload(uploadID)
	writeJSON(w, 201, E{"object": obj})
}

func (s *Server) handleCommitUpload(w http.ResponseWriter, r *http.Request) {
	b := r.PathValue("b")
	key := keyFromRequest(r)
	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		writeJSON(w, 400, E{"error": "upload_id required"})
		return
	}
	if key == "" {
		sb, sk, ok, err := s.meta.UploadByID(uploadID)
		if err != nil {
			s.handleErr(w, err)
			return
		}
		if !ok {
			writeJSON(w, 404, E{"error": "upload session not found"})
			return
		}
		b, key = sb, sk
	}
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}
	ct, ok, err := s.meta.GetUpload(b, key, uploadID)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if !ok {
		writeJSON(w, 404, E{"error": "upload session not found"})
		return
	}
	hash, size, etag, err := s.store.FinalizeTmp(uploadID)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	obj := &meta.Object{
		Bucket: b, Key: key, BlobHash: hash, Size: size,
		ContentType: ct, ETag: etag, Metadata: map[string]string{},
	}
	if err := s.meta.PutObject(obj); err != nil {
		s.handleErr(w, err)
		return
	}
	_ = s.meta.DeleteUpload(uploadID)
	writeJSON(w, 200, E{"object": obj})
}

func (s *Server) handleAbortUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		writeJSON(w, 400, E{"error": "upload_id required"})
		return
	}
	_ = s.store.AbortTmp(uploadID)
	_ = s.meta.DeleteUpload(uploadID)
	w.WriteHeader(204)
}

func (s *Server) handleBeginUpload(w http.ResponseWriter, r *http.Request) {
	b := r.PathValue("b")
	key := keyFromRequest(r)
	if err := meta.ValidBucket(b); err != nil {
		s.handleErr(w, err)
		return
	}
	if err := meta.ValidKey(key); err != nil {
		s.handleErr(w, err)
		return
	}
	if _, err := s.meta.GetBucket(b); err != nil {
		s.handleErr(w, err)
		return
	}
	ct := r.Header.Get("Content-Type")
	id, err := s.meta.BeginUpload(b, key, ct)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	received, err := s.store.TmpSize(id)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"upload_id": id, "key": key, "received": received})
}

// ---- read path ----

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request) {
	b, key := r.PathValue("b"), r.PathValue("key")
	obj, err := s.meta.GetObject(b, key)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	bk, _ := s.meta.GetBucket(b)
	streamObject(w, r, s, obj, bk != nil && bk.IsPublic, false)
}

func (s *Server) handleHeadObject(w http.ResponseWriter, r *http.Request) {
	b, key := r.PathValue("b"), r.PathValue("key")
	obj, err := s.meta.GetObject(b, key)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	bk, _ := s.meta.GetBucket(b)
	streamObject(w, r, s, obj, bk != nil && bk.IsPublic, true)
}

// streamObject implements the full conditional GET/Range pipeline for a
// resolved object. It is shared between the authed and public routes.
func streamObject(w http.ResponseWriter, r *http.Request, s *Server, obj *meta.Object, public, headOnly bool) {
	lastMod := obj.UpdatedAt.UTC().Format(http.TimeFormat)
	lastModT, _ := time.Parse(http.TimeFormat, lastMod)
	total := obj.Size

	// RFC 7232 conditionals
	ims, imsErr := httpdate(r.Header.Get("If-Modified-Since"))
	ius, iusErr := httpdate(r.Header.Get("If-Unmodified-Since"))

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etagMatches(inm, obj.ETag) {
			w.WriteHeader(304)
			return
		}
	} else if imsErr == nil && !lastModT.After(ims.Truncate(time.Second)) {
		w.WriteHeader(304)
		return
	}
	if im2 := r.Header.Get("If-Match"); im2 != "" {
		if !etagMatches(im2, obj.ETag) {
			w.WriteHeader(412)
			return
		}
	} else if iusErr == nil && lastModT.After(ius) {
		w.WriteHeader(412)
		return
	}

	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("ETag", obj.ETag)
	w.Header().Set("Last-Modified", lastMod)
	w.Header().Set("Accept-Ranges", "bytes")
	if public {
		w.Header().Set("Cache-Control", "public, max-age=3600")
	} else {
		w.Header().Set("Cache-Control", "private, no-cache")
	}

	forwardRange := true
	if ir := r.Header.Get("If-Range"); ir != "" {
		if t, err := httpdate(ir); err == nil {
			forwardRange = lastModT.Truncate(time.Second).Equal(t.Truncate(time.Second))
		} else {
			forwardRange = etagMatches(ir, obj.ETag)
		}
	}

	offset, length, ranged := int64(0), total, false
	if rg := r.Header.Get("Range"); rg != "" && forwardRange {
		if off, ln, ok := parseRange(rg, total); ok {
			offset, length, ranged = off, ln, true
		} else if strings.HasPrefix(rg, "bytes=") {
			w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(total, 10))
			w.WriteHeader(416)
			return
		}
	}

	if ranged {
		w.Header().Set("Content-Range", bytesRangeHeader(offset, length, total))
		w.WriteHeader(206)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(total, 10))
		w.WriteHeader(200)
	}
	if headOnly {
		return
	}
	if _, err := s.store.CopyBlobTo(w, obj.BlobHash, offset, length); err != nil {
		if isNotExist(err) {
			return // headers already written
		}
	}
}

func isNotExist(err error) bool {
	var ne interface{ IsNotExist() bool }
	return errors.As(err, &ne) && ne.IsNotExist()
}

// ---- listing ----

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request) {
	b := r.PathValue("b")
	q := r.URL.Query()
	prefix := q.Get("prefix")
	after := q.Get("after")
	search := q.Get("q")
	delimiter := q.Get("delimiter")
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 2000 {
		limit = 2000
	}
	if _, err := s.meta.GetBucket(b); err != nil {
		s.handleErr(w, err)
		return
	}
	if search == "" && delimiter != "" {
		// S3-style directory listing (folders = common prefixes)
		files, folders, next, err := s.meta.ListFolder(r.Context(), b, prefix, delimiter, limit, after)
		if err != nil {
			s.handleErr(w, err)
			return
		}
		writeJSON(w, 200, E{"entries": files, "folders": folders, "next": next, "prefix": prefix, "delimiter": delimiter, "truncated": next != ""})
		return
	}
	entries, next, err := s.meta.ListObjects(r.Context(), b, meta.ListFilter{
		Prefix: prefix,
		After:  after,
		Search: search,
		Limit:  limit,
	})
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if entries == nil {
		entries = []meta.ListEntry{}
	}
	writeJSON(w, 200, E{"entries": entries, "next": next, "prefix": prefix, "q": search, "truncated": next != ""})
}

// ---- presigned URLs ----

func (s *Server) handleSignUploadURL(w http.ResponseWriter, r *http.Request) {
	b, key := r.PathValue("b"), keyFromRequest(r)
	if err := meta.ValidBucket(b); err != nil || meta.ValidKey(key) != nil {
		writeJSON(w, 400, E{"error": "invalid bucket or key"})
		return
	}
	ttl := s.cfg.SignedURLTTL()
	url := s.auth.SignURL("PUT", b, key, ttl, s.cfg.PublicURL)
	writeJSON(w, 200, E{"method": "PUT", "url": url, "expires_at": time.Now().Add(ttl).UTC().Format(time.RFC3339)})
}

func (s *Server) handleSignDownloadURL(w http.ResponseWriter, r *http.Request) {
	b, key := r.PathValue("b"), keyFromRequest(r)
	ttl := s.cfg.SignedURLTTL()
	url := s.auth.SignURL("GET", b, key, ttl, s.cfg.PublicURL)
	writeJSON(w, 200, E{"method": "GET", "url": url, "expires_at": time.Now().Add(ttl).UTC().Format(time.RFC3339)})
}

// ---- multipart upload ----

func (s *Server) handleMultipartBegin(w http.ResponseWriter, r *http.Request) {
	b := r.PathValue("b")
	key := keyFromRequest(r)
	if _, err := s.meta.GetBucket(b); err != nil {
		s.handleErr(w, err)
		return
	}
	if err := meta.ValidKey(key); err != nil {
		s.handleErr(w, err)
		return
	}
	ct := r.Header.Get("Content-Type")
	id, err := s.meta.BeginUpload(b, key, ct)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	parts, err := s.meta.ListParts(id)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if parts == nil {
		parts = []meta.Part{}
	}
	writeJSON(w, 200, E{"upload_id": id, "parts": parts})
}

func (s *Server) handleMultipartPutPart(w http.ResponseWriter, r *http.Request) {
	uploadID := r.PathValue("upload_id")
	part, err := strconv.Atoi(r.PathValue("part"))
	if err != nil || part < 1 || part > 10000 {
		writeJSON(w, 400, E{"error": "invalid part number"})
		return
	}
_, _, ok, err := s.meta.UploadByID(uploadID)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if !ok {
		writeJSON(w, 404, E{"error": "upload session not found"})
		return
	}
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}

	partTmp := uploadID + ".p" + strconv.Itoa(part)
	tmp, err := s.store.OpenTmp(partTmp)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	body := http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	if _, err := io.CopyBuffer(tmp, body, make([]byte, 1<<20)); err != nil {
		tmp.Close()
		s.handleErr(w, err)
		return
	}
	if err := tmp.Close(); err != nil {
		s.handleErr(w, err)
		return
	}
	hash, size, _, err := s.store.FinalizeTmp(partTmp)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if err := s.meta.AddPart(uploadID, part, hash, size); err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"part": part, "etag": hash, "size": size})
}

func (s *Server) handleMultipartComplete(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("upload_id")
	if uploadID == "" {
		writeJSON(w, 400, E{"error": "upload_id required"})
		return
	}
	b := r.PathValue("b")
	key := keyFromRequest(r)
	if key == "" {
		sb, sk, ok, err := s.meta.UploadByID(uploadID)
		if err != nil {
			s.handleErr(w, err)
			return
		}
		if !ok {
			writeJSON(w, 404, E{"error": "upload session not found"})
			return
		}
		b, key = sb, sk
	}
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}
	ct, ok, err := s.meta.GetUpload(b, key, uploadID)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if !ok {
		writeJSON(w, 404, E{"error": "upload session not found"})
		return
	}
	parts, err := s.meta.ListParts(uploadID)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if len(parts) == 0 {
		writeJSON(w, 400, E{"error": "no parts uploaded"})
		return
	}

	// concatenate part blobs into a final staging file, then hash once
	finalTmp := uploadID + ".mp"
	f, err := s.store.OpenTmp(finalTmp)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	var total int64
	for _, p := range parts {
		fp, err := s.store.OpenBlob(p.BlobHash)
		if err != nil {
			f.Close()
			s.handleErr(w, err)
			return
		}
		n, cerr := io.Copy(f, fp)
		fp.Close()
		if cerr != nil {
			f.Close()
			s.handleErr(w, cerr)
			return
		}
		total += n
	}
	if err := f.Close(); err != nil {
		s.handleErr(w, err)
		return
	}
	hash, size, etag, err := s.store.FinalizeTmp(finalTmp)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	obj := &meta.Object{
		Bucket: b, Key: key, BlobHash: hash, Size: size,
		ContentType: ct, ETag: etag, Metadata: map[string]string{},
	}
	if err := s.meta.PutObject(obj); err != nil {
		s.handleErr(w, err)
		return
	}
	_ = s.meta.DeleteUpload(uploadID)
	writeJSON(w, 200, E{"object": obj})
}

func (s *Server) handleMultipartAbort(w http.ResponseWriter, r *http.Request) {
	uploadID := r.PathValue("upload_id")
	parts, err := s.meta.ListParts(uploadID)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	for _, p := range parts {
		_ = s.store.DeleteBlob(p.BlobHash)
	}
	_ = s.meta.DeleteUpload(uploadID)
	w.WriteHeader(204)
}

// ---- delete (soft-delete: moves to trash) ----

func (s *Server) handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	b, key := r.PathValue("b"), r.PathValue("key")
	if ce := s.checkWrite(); ce != nil {
		s.handleErr(w, ce)
		return
	}
	ok, err := s.meta.TrashObject(b, key)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if !ok {
		s.handleErr(w, meta.ErrNotFound)
		return
	}
	writeJSON(w, 200, E{"bucket": b, "key": key, "status": "trashed"})
}