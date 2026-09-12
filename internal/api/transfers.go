package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/anacrolix/torrent/metainfo"

	"storaged/internal/meta"
	"storaged/internal/transfer"
)

// Transfers mirror put.io's "paste a link, we fetch it" flow. A transfer job
// is queued locally; the background worker (internal/transfer) downloads the
// URL into the blob store and commits it as a normal object (replicated via
// the oplog). Transfer records themselves are writer-local.

func (s *Server) handleCreateTransfer(w http.ResponseWriter, r *http.Request) {
	b := r.PathValue("b")
	if err := meta.ValidBucket(b); err != nil {
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
	data, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeJSON(w, 400, E{"error": "invalid body"})
		return
	}

	var id string
	switch {
	case len(data) > 0 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-bittorrent"):
		// raw .torrent upload
		id, err = s.createRawTorrent(r, b, data)
	case len(data) > 0:
		id, err = s.createJSONTransfer(b, data)
	default:
		writeJSON(w, 400, E{"error": "empty body"})
		return
	}
	if err != nil {
		s.handleErr(w, err)
		return
	}
	t, err := s.meta.GetTransfer(id)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if s.tf != nil {
		s.tf.Notify()
	}
	writeJSON(w, 201, E{"transfer": t})
}

// createRawTorrent queues a download from uploaded .torrent bytes.
func (s *Server) createRawTorrent(r *http.Request, bucket string, data []byte) (string, error) {
	mi, err := metainfo.Load(bytes.NewReader(data))
	if err != nil {
		return "", meta.ErrInvalidF("bad .torrent file: %v", err)
	}
	name := ""
	if info, err := mi.UnmarshalInfo(); err == nil {
		name = info.BestName()
	}
	q := r.URL.Query()
	key := q.Get("key")
	if key == "" {
		key = torrentKey(name, "torrent")
	}
	if err := meta.ValidKey(key); err != nil {
		return "", err
	}
	return s.meta.CreateTorrentTransfer(bucket, key, "", base64.StdEncoding.EncodeToString(data))
}

// createJSONTransfer handles {"url":...} (put.io URL fetch) and
// {"magnet":...} (BitTorrent download) payloads.
func (s *Server) createJSONTransfer(bucket string, data []byte) (string, error) {
	var body struct {
		URL         string `json:"url"`
		Magnet      string `json:"magnet"`
		Key         string `json:"key"`
		ContentType string `json:"content_type"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return "", meta.ErrInvalidF("invalid JSON body: %v", err)
	}
	if m := strings.TrimSpace(body.Magnet); m != "" && strings.HasPrefix(m, "magnet:?") {
		key := body.Key
		if key == "" {
			key = magnetKey(m)
		}
		if err := meta.ValidKey(key); err != nil {
			return "", err
		}
		return s.meta.CreateTorrentTransfer(bucket, key, m, "")
	}
	src := strings.TrimSpace(body.URL)
	if src == "" {
		return "", meta.ErrInvalidF("url or magnet required")
	}
	u, err := url.Parse(src)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", meta.ErrInvalidF("url must be an http(s) URL")
	}
	key := body.Key
	if key == "" {
		key = keyFromURL(u)
	}
	if err := meta.ValidKey(key); err != nil {
		return "", err
	}
	return s.meta.CreateTransfer(bucket, key, src, body.ContentType)
}

func (s *Server) handleListTransfers(w http.ResponseWriter, r *http.Request) {
	b := r.PathValue("b")
	if _, err := s.meta.GetBucket(b); err != nil {
		s.handleErr(w, err)
		return
	}
	transfers, err := s.meta.ListTransfers(r.Context(), b, r.URL.Query().Get("status"))
	if err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"transfers": transfers})
}

func (s *Server) handleGetTransfer(w http.ResponseWriter, r *http.Request) {
	t, err := s.meta.GetTransfer(r.PathValue("id"))
	if err != nil {
		s.handleErr(w, err)
		return
	}
	writeJSON(w, 200, E{"transfer": t})
}

func (s *Server) handleCancelTransfer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, err := s.meta.GetTransfer(id)
	if err != nil {
		s.handleErr(w, err)
		return
	}
	if t.Bucket != r.PathValue("b") || t.Bucket == "" {
		writeJSON(w, 404, E{"error": "transfer not found"})
		return
	}
	if t.Status == transfer.StatusQueued || t.Status == transfer.StatusDownloading {
		if s.tf != nil {
			s.tf.Cancel(id)
		}
		_ = s.meta.SetTransferStatus(id, transfer.StatusCancelled, "cancelled by user")
	}
	w.WriteHeader(204)
}

// keyFromURL derives an object key from a URL's final path segment.
func keyFromURL(u *url.URL) string {
	seg := strings.Trim(u.Path, "/")
	if seg == "" {
		return "download"
	}
	// keep only the final non-empty segment, else the full path (it may be
	// garbage for some CDNs, so stay conservative)
	if i := strings.LastIndex(seg, "/"); i >= 0 {
		seg = seg[i+1:]
	}
	seg = path.Base(strings.ReplaceAll(seg, "\\", "/"))
	seg = strings.Trim(seg, " /")
	if seg == "" {
		seg = "download"
	}
	return seg
}

// magnetKey derives a stable default object key from a magnet link's info hash
// (magnet_<12 hex chars>); users override with an explicit key.
func magnetKey(m string) string {
	m = strings.TrimPrefix(m, "magnet:?")
	var frag string
	for _, p := range strings.Split(m, "&") {
		p = strings.TrimSpace(p)
		if strings.HasPrefix(p, "xt=urn:bt") && strings.Contains(p, ":") {
			frag = p[strings.LastIndex(p, ":")+1:]
			break
		}
	}
	frag = strings.ToLower(strings.TrimSpace(frag))
	if frag == "" {
		return "magnet"
	}
	if len(frag) > 12 {
		frag = frag[:12]
	}
	return "magnet_" + frag
}

// torrentKey sanitizes a torrent name into a usable object key for the
// transfer's default base path.
func torrentKey(name, fallback string) string {
	if name != "" {
		// keep letters, digits and common key punctuation; map the rest to '_'.
		// spaces are preserved, but leading/trailing ones are trimmed because
		// validKey rejects keys that start or end with a space.
		k := strings.Map(func(r rune) rune {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
				return r
			case r == '-' || r == '_' || r == '.' || r == '/' || r == ' ':
				return r
			}
			return '_'
		}, name)
		k = strings.Trim(k, "/._ ")
		if k != "" {
			return k
		}
	}
	return fallback
}
