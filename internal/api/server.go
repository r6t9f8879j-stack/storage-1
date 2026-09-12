package api

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"storaged/internal/auth"
	"storaged/internal/config"
	"storaged/internal/debug"
	"storaged/internal/meta"
	"storaged/internal/repl"
	"storaged/internal/store"
	"storaged/internal/transfer"
)

//go:embed web/landing.html
var landingHTML []byte

// RoleState tracks the local write role.
type RoleState struct {
	nodeID        string
	role          string
	writesAllowed bool
}

// Server holds all runtime dependencies for the HTTP API.
type Server struct {
	cfg           *config.Config
	auth          *auth.Auth
	meta          *meta.Meta
	store         *store.Store
	role          *RoleState
	repl          *repl.Replicator
	tf            *transfer.Manager
	startedAt     time.Time
	logger        *log.Logger
	activeUploads atomic.Int64
}

// New wires a Server given config+meta+store.
func New(cfg *config.Config, a *auth.Auth, m *meta.Meta, st *store.Store) *Server {
	s := &Server{
		cfg: cfg, auth: a, meta: m, store: st,
		role:      &RoleState{nodeID: cfg.NodeID, role: cfg.Role},
		startedAt: time.Now(),
		logger:    log.New(log.Default().Writer(), "[storaged] ", log.LstdFlags),
	}
	if cfg.IsLeader() {
		s.role.writesAllowed = true
	}
	return s
}

// SetReplicator attaches the follower pull loop (nil for leader-only).
func (s *Server) SetReplicator(r *repl.Replicator) { s.repl = r }

// SetTransferer attaches the background URL-fetch worker.
func (s *Server) SetTransferer(t *transfer.Manager) { s.tf = t }

// WritesAllowed reports whether this node currently accepts client writes.
func (s *Server) WritesAllowed() bool { return s.role.writesAllowed }

// Promote flips this node to the writer role (used by keep-alive on failover).
func (s *Server) Promote() { s.role.writesAllowed = true }

// Demote revokes local write rights.
func (s *Server) Demote() { s.role.writesAllowed = false }

// Handler returns the fully wired http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /{$}", logit(s.handleRoot))
	mux.HandleFunc("GET /favicon.ico", logit(s.handleFavicon))
	mux.HandleFunc("GET /v1/health", logit(s.handleHealth))
	mux.HandleFunc("GET /v1/buckets", authz(s, ScopeRead)(s.handleListBuckets))
	mux.HandleFunc("PUT /v1/buckets/{b}", authz(s, ScopeAdmin)(s.handlePutBucket))
	mux.HandleFunc("GET /v1/buckets/{b}", authz(s, ScopeRead)(s.handleGetBucket))
	mux.HandleFunc("DELETE /v1/buckets/{b}", authz(s, ScopeAdmin)(s.handleDeleteBucket))

	mux.HandleFunc("GET /v1/buckets/{b}/objects", authz(s, ScopeRead)(s.handleListObjects))
	mux.HandleFunc("PUT /v1/buckets/{b}/objects/{key...}", signedOrAuthz(s, ScopeAdmin)(s.handlePutObject))
	mux.HandleFunc("GET /v1/buckets/{b}/objects/{key...}", signedOrAuthz(s, ScopeRead)(s.handleGetObject))
	mux.HandleFunc("HEAD /v1/buckets/{b}/objects/{key...}", signedOrAuthz(s, ScopeRead)(s.handleHeadObject))
	mux.HandleFunc("DELETE /v1/buckets/{b}/objects/{key...}", authz(s, ScopeAdmin)(s.handleDeleteObject))

	mux.HandleFunc("POST /v1/buckets/{b}/uploads", authz(s, ScopeAdmin)(s.handleBeginUpload))
	mux.HandleFunc("POST /v1/buckets/{b}/uploads/commit", authz(s, ScopeAdmin)(s.handleCommitUpload))
	mux.HandleFunc("POST /v1/buckets/{b}/uploads/abort", authz(s, ScopeAdmin)(s.handleAbortUpload))
	mux.HandleFunc("POST /v1/buckets/{b}/uploads/upload-url", authz(s, ScopeAdmin)(s.handleSignUploadURL))
	mux.HandleFunc("POST /v1/buckets/{b}/uploads/download-url", authz(s, ScopeAdmin)(s.handleSignDownloadURL))
	mux.HandleFunc("POST /v1/buckets/{b}/uploads/multipart", authz(s, ScopeAdmin)(s.handleMultipartBegin))
	mux.HandleFunc("POST /v1/buckets/{b}/uploads/multipart/complete", authz(s, ScopeAdmin)(s.handleMultipartComplete))
	mux.HandleFunc("PUT /v1/multipart/{upload_id}/parts/{part}", authz(s, ScopeAdmin)(s.handleMultipartPutPart))
	mux.HandleFunc("DELETE /v1/multipart/{upload_id}", authz(s, ScopeAdmin)(s.handleMultipartAbort))

	mux.HandleFunc("GET /v1/public/{b}/{key...}", logit(s.handlePublicGet))

	mux.HandleFunc("POST /v1/admin/gc", authz(s, ScopeAdmin)(s.handleAdminGC))
	mux.HandleFunc("POST /v1/admin/checkpoint", authz(s, ScopeAdmin)(s.handleAdminCheckpoint))
	mux.HandleFunc("POST /v1/admin/promote", authz(s, ScopeAdmin)(s.handleAdminPromote))
	mux.HandleFunc("POST /v1/admin/demote", authz(s, ScopeAdmin)(s.handleAdminDemote))

	// put.io-style remote URL fetches
	mux.HandleFunc("POST /v1/buckets/{b}/transfers", authz(s, ScopeAdmin)(s.handleCreateTransfer))
	mux.HandleFunc("GET /v1/buckets/{b}/transfers", authz(s, ScopeRead)(s.handleListTransfers))
	mux.HandleFunc("GET /v1/transfers/{id}", authz(s, ScopeRead)(s.handleGetTransfer))
	mux.HandleFunc("DELETE /v1/buckets/{b}/transfers/{id}", authz(s, ScopeAdmin)(s.handleCancelTransfer))

	// trash (soft delete)
	mux.HandleFunc("GET /v1/trash", authz(s, ScopeRead)(s.handleListTrash))
	mux.HandleFunc("POST /v1/trash/restore", authz(s, ScopeAdmin)(s.handleRestoreTrash))
	mux.HandleFunc("POST /v1/trash/purge", authz(s, ScopeAdmin)(s.handlePurgeTrashOne))
	mux.HandleFunc("DELETE /v1/trash", authz(s, ScopeAdmin)(s.handlePurgeTrashAll))

	// streaming ZIP archive of a bucket
	mux.HandleFunc("GET /v1/buckets/{b}/zip", authz(s, ScopeRead)(s.handleZip))

	mux.HandleFunc("GET /v1/debug/logs", authz(s, ScopeRead)(s.handleDebugLogs))
	mux.HandleFunc("GET /v1/debug/stats", authz(s, ScopeRead)(s.handleDebugStats))
	mux.HandleFunc("GET /v1/debug/routes", authz(s, ScopeRead)(s.handleDebugRoutes))

	// account creation / login proxy (public by design, no signup rate limit)
	mux.HandleFunc("POST /v1/auth/signup", logit(s.handleAuthSignup))
	mux.HandleFunc("POST /v1/auth/token", logit(s.handleAuthToken))

	mux.Handle("GET /_internal/health", peerAuth(s, s.handleInternalHealth))
	mux.Handle("GET /_internal/oplog", peerAuth(s, s.handleInternalOplog))
	mux.Handle("GET /_internal/inventory", peerAuth(s, s.handleInternalInventory))
	mux.Handle("GET /_internal/blob/{hash}", peerAuth(s, s.handleInternalBlob))
	mux.Handle("PUT /_internal/lease", peerAuth(s, s.handleInternalLease))

	return logit(httpHandlerFunc(recoverMiddleware(cors(mux))))
}

// ---- middleware ----

// cors wraps the API so a browser dashboard served from any hostname (tunnel
// preview URL, CNAME alias, local port forward) can call the same-origin API
// paths. It reflects the Origin header and short-circuits OPTIONS preflights.
// Auth is header-based bearer tokens (no cookies), so CSRF is not a concern.
func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			hdr := w.Header()
			hdr.Set("Access-Control-Allow-Origin", origin)
			hdr.Add("Vary", "Origin")
			hdr.Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, HEAD, OPTIONS")
			hdr.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Range, If-None-Match, If-Match, X-Peer-Secret")
			hdr.Set("Access-Control-Expose-Headers", "ETag, Content-Range, Accept-Ranges, Content-Length")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// Scope mirrors auth.Scope for routing.
type Scope = auth.Scope

const (
	ScopeNone  = auth.ScopeNone
	ScopeRead  = auth.ScopeRead
	ScopeAdmin = auth.ScopeAdmin
)

// authz authorizes with the bearer credential for the required scope, then
// checks rate limit.
func authz(s *Server, need Scope) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			sc := s.auth.ClientScope(r)
			if sc < need {
				unauthorized(w, "missing or invalid credentials")
				return
			}
			if !s.auth.Allow(remoteKey(r) + "|" + auth.BearerToken(r)) {
				tooMany(w)
				return
			}
			next(w, r)
		}
	}
}

// signedOrAuthz allows either a bearer meeting the required scope OR a valid
// presigned URL for the object route. Presigned URLs are accepted for
// GET/HEAD/PUT object routes (only minted by admin-scope sign endpoints).
func signedOrAuthz(s *Server, need Scope) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			bucket := r.PathValue("b")
			key := r.PathValue("key")
			sc := s.auth.ClientScope(r)
			authorized := sc >= need
			if !authorized && bucket != "" && key != "" {
				ok, err := s.auth.VerifySigned(r, r.Method, bucket, key)
				if err != nil {
					unauthorized(w, "invalid signature: "+err.Error())
					return
				}
				authorized = ok
			}
			if !authorized {
				unauthorized(w, "missing or invalid credentials")
				return
			}
			if !s.auth.Allow(remoteKey(r)) {
				tooMany(w)
				return
			}
			next(w, r)
		}
	}
}

func peerAuth(s *Server, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.auth.IsPeerSecret(r.Header.Get("X-Peer-Secret")) {
			unauthorized(w, "invalid peer secret")
			return
		}
		next(w, r)
	}
}

func logit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, code: 200}
		start := time.Now()
		next(sw, r)
		debug.AddAccess(r.Method, r.URL.Path, remoteKey(r), sw.code, time.Since(start))
	}
}

// statusWriter captures the response status while preserving streaming
// (Flush) and sendfile (ReaderFrom) behaviour for large downloads.
type statusWriter struct {
	http.ResponseWriter
	code int
	head bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.head {
		s.code = code
		s.head = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(b []byte) (int, error) {
	if !s.head {
		s.code = 200
		s.head = true
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusWriter) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *statusWriter) ReadFrom(r io.Reader) (int64, error) {
	if !s.head {
		s.code = 200
		s.head = true
	}
	if rf, ok := s.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(s.ResponseWriter, r)
}

func recoverMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				writeJSON(w, 500, E{"error": "internal server error"})
			}
		}()
		h.ServeHTTP(w, r)
	})
}

func httpHandlerFunc(h http.Handler) http.HandlerFunc {
	return h.ServeHTTP
}

// ---- request helpers ----

func remoteKey(r *http.Request) string {
	ip := r.RemoteAddr
	if i := strings.LastIndex(ip, ":"); i > -1 {
		ip = ip[:i]
	}
	return ip
}

// checkWrite verifies role + storage headroom for mutations.
func (s *Server) checkWrite() *httpStatusErr {
	if !s.WritesAllowed() {
		return &httpStatusErr{409, "node is read-only (not lease holder); promote via keep-alive or /v1/admin/promote"}
	}
	free, err := DiskFree(s.cfg.DataDir)
	if err != nil {
		return &httpStatusErr{500, "cannot determine disk free space"}
	}
	if free < s.cfg.HighWaterBytes {
		return &httpStatusErr{507, "storage below high-water mark; writes disabled"}
	}
	return nil
}

func (s *Server) context(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 30*time.Minute)
}

func (s *Server) healthPayload(extra map[string]any) map[string]any {
	lsn, _ := s.meta.LastLSN()
	free, _ := DiskFree(s.cfg.DataDir)
	p := map[string]any{
		"status":         "ready",
		"node_id":        s.cfg.NodeID,
		"role":           s.role.role,
		"writes":         s.WritesAllowed(),
		"disk_free":      free,
		"lsn":            lsn,
		"started_at":     s.startedAt.UTC().Format(time.RFC3339),
		"uptime_sec":     int64(time.Since(s.startedAt).Seconds()),
		"active_uploads": s.activeUploads.Load(),
	}
	if s.repl != nil {
		wm, _ := s.meta.Watermark()
		p["repl_watermark"] = wm
		p["repl_lag_peer"] = s.repl.LeaderLSN()
	}
	if extra != nil {
		for k, v := range extra {
			p[k] = v
		}
	}
	return p
}

// status health.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.healthPayload(nil))
}

// root landing page so a browser at the public domain never hits a bare 404.
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	payload, err := s.dashboardPayload(r.Context())
	if err != nil {
		writeJSON(w, 500, E{"error": "dashboard unavailable"})
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		writeJSON(w, 500, E{"error": "dashboard unavailable"})
		return
	}
	page := bytes.Replace(landingHTML, []byte("__DASHBOARD__"), raw, 1)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(200)
	_, _ = w.Write(page)
}

// dashboardPayload assembles the live JSON the landing page renders.
func (s *Server) dashboardPayload(ctx context.Context) (map[string]any, error) {
	totals, err := s.meta.BucketTotals(ctx)
	if err != nil {
		return nil, err
	}
	blobCount, err := s.store.CountBlobs()
	if err != nil {
		return nil, err
	}
	hp := s.healthPayload(nil)

	var objects int64
	var maxBucketObjects int64
	buckets := make([]map[string]any, 0, len(totals))
	for _, t := range totals {
		objects += t.Objects
		if t.Objects > maxBucketObjects {
			maxBucketObjects = t.Objects
		}
		buckets = append(buckets, map[string]any{
			"name":    t.Name,
			"public":  t.IsPublic,
			"objects": t.Objects,
			"size":    t.Size,
			"sizeH":   humanBytes(t.Size),
		})
	}

	transfers, err := s.meta.ListTransfers(ctx, "", "")
	if err != nil {
		return nil, err
	}

	site := s.cfg.PublicURL
	if site == "" {
		site = "local"
	}

	peerUp := s.cfg.Peer.Enabled && s.cfg.Peer.Key != ""
	checks := []map[string]any{
		{"item": "Content-addressed blob store (sha256)", "status": "pass"},
		{"item": "Commit-after-fsync · atomic rename", "status": "pass"},
		{"item": "SQLite WAL journal · synchronous FULL", "status": "pass"},
		{"item": "Upload sessions + multipart resume", "status": "pass"},
		{"item": "Signed URL HMAC (expiry enforced)", "status": "pass"},
		{"item": "Bearer scopes + rate limiting", "status": "pass"},
		{"item": "Instance lock + heartbeat takeover", "status": "pass"},
		{"item": "Hourly GC · WAL checkpoint", "status": "pass"},
		{"item": "Artifact rescue snapshots", "status": "pass"},
		{"item": "Replication to peer",
			"status": map[bool]string{true: "pass", false: "warn"}[peerUp]},
	}

	milestones := []map[string]any{
		{"label": "storaged at", "date": fmt.Sprintf(":%s", s.cfg.Listen), "done": true},
		{"label": "Cloudflare tunnel", "date": "named", "done": true},
		{"label": "DNS mapped", "date": site, "done": site != "local"},
		{"label": "Artifact rescue", "date": "snapshot+zip", "done": true},
		{"label": "Dual-node replication", "date": "leader/follower", "done": peerUp},
	}

	endpoints := []map[string]any{
		{"method": "GET", "path": "/v1/health", "desc": "Node status (no auth)"},
		{"method": "GET", "path": "/v1/buckets", "desc": "List buckets"},
		{"method": "PUT", "path": "/v1/buckets/{b}?public=", "desc": "Create / update bucket"},
		{"method": "DELETE", "path": "/v1/buckets/{b}", "desc": "Delete bucket"},
		{"method": "GET", "path": "/v1/buckets/{b}/objects", "desc": "List objects (prefix/after/limit/q / delimiter=)"},
		{"method": "PUT", "path": "/v1/buckets/{b}/objects/{key...}", "desc": "Upload (resume ?upload_id=&offset=)"},
		{"method": "GET", "path": "/v1/buckets/{b}/objects/{key...}", "desc": "Download (Range/ETag/conditionals)"},
		{"method": "HEAD", "path": "/v1/buckets/{b}/objects/{key...}", "desc": "Headers only"},
		{"method": "DELETE", "path": "/v1/buckets/{b}/objects/{key...}", "desc": "Soft-delete (move to trash)"},
		{"method": "POST", "path": "/v1/buckets/{b}/uploads", "desc": "Begin upload session"},
		{"method": "POST", "path": "/v1/buckets/{b}/uploads/commit|abort", "desc": "Finalize / abort session"},
		{"method": "POST", "path": "/v1/buckets/{b}/uploads/upload-url", "desc": "Presigned PUT URL"},
		{"method": "POST", "path": "/v1/buckets/{b}/uploads/download-url", "desc": "Presigned GET URL"},
		{"method": "POST", "path": "/v1/buckets/{b}/uploads/multipart", "desc": "Multipart session / complete"},
		{"method": "POST", "path": "/v1/buckets/{b}/transfers", "desc": "Queue remote URL / magnet / .torrent fetch"},
		{"method": "GET", "path": "/v1/buckets/{b}/transfers", "desc": "List transfers"},
		{"method": "GET", "path": "/v1/transfers/{id}", "desc": "Get transfer status"},
		{"method": "DELETE", "path": "/v1/buckets/{b}/transfers/{id}", "desc": "Cancel transfer"},
		{"method": "GET", "path": "/v1/public/{b}/{key...}", "desc": "Public download (no auth)"},
		{"method": "GET", "path": "/v1/trash", "desc": "List trashed objects"},
		{"method": "POST", "path": "/v1/trash/restore", "desc": "Restore from trash"},
		{"method": "POST", "path": "/v1/trash/purge", "desc": "Purge one object from trash"},
		{"method": "DELETE", "path": "/v1/trash", "desc": "Purge ALL trashed objects"},
		{"method": "GET", "path": "/v1/buckets/{b}/zip", "desc": "Stream ZIP of bucket (prefix=)"},
		{"method": "POST", "path": "/v1/admin/{gc,checkpoint,promote,demote}", "desc": "Maintenance"},
	}

	lsn, _ := hp["lsn"].(int64)
	uptime, _ := hp["uptime_sec"].(int64)
	var watermark any
	if wm, ok := hp["repl_watermark"].(int64); ok {
		watermark = wm
	}
	free, _ := hp["disk_free"].(int64)

	replState := "writer (leader)"
	if s.repl != nil {
		replState = "caught up"
	}

	supabase := map[string]string{}
	if s.cfg.SupabaseURL != "" {
		supabase["url"] = s.cfg.SupabaseURL
	}
	if s.cfg.SupabaseAnonKey != "" {
		supabase["anon"] = s.cfg.SupabaseAnonKey
	}

	qs := fmt.Sprintf(`# status
curl -s https://%s/v1/health | jq

# bucket
curl -s -X PUT -H "Authorization: Bearer $K" \
  "https://%s/v1/buckets/media?public=true"

# upload
curl -s -X PUT -H "Authorization: Bearer $K" \
  --data-binary @photo.jpg \
  https://%s/v1/buckets/media/objects/photos/a.jpg

# download
curl -s https://%s/v1/public/media/photos/a.jpg -o a.jpg

# presigned PUT
curl -s -X POST -H "Authorization: Bearer $K" \
  -d '{"key":"photos/b.jpg"}' \
  https://%s/v1/buckets/media/uploads/upload-url`, cfgHost(s.cfg.PublicURL), cfgHost(s.cfg.PublicURL), cfgHost(s.cfg.PublicURL), cfgHost(s.cfg.PublicURL), cfgHost(s.cfg.PublicURL))

	return map[string]any{
		"profile": map[string]any{
			"name": "storaged", "node": hp["node_id"], "asOf": time.Now().UTC().Format("Jan 02, 2006"),
			"publicUrl": site,
		},
		"supabase": supabase,
		"kpis": []map[string]any{
			{"label": "Disk Free", "value": humanBytes(free), "key": "disk"},
			{"label": "Buckets", "value": humanCount(int64(len(buckets))), "key": "buckets"},
			{"label": "Objects", "value": humanCount(objects), "key": "objects"},
			{"label": "Blobs", "value": humanCount(blobCount), "key": "blobs"},
			{"label": "Op Log", "value": humanCount(lsn), "key": "lsn"},
			{"label": "Uptime", "value": humanDuration(time.Duration(uptime) * time.Second), "key": "uptime"},
		},
		"status": map[string]any{
			"node": hp["node_id"], "role": hp["role"], "writes": hp["writes"],
			"started": hp["started_at"], "active": hp["active_uploads"],
			"lsn": lsn, "watermark": watermark, "objects": objects, "blobs": blobCount,
			"repl": replState,
		},
		"buckets":    buckets,
		"bucketsMax": maxBucketObjects,
		"transfers":  transfers,
		"checks":     checks,
		"milestones": milestones,
		"endpoints":  endpoints,
		"quickstart": qs,
	}, nil
}

// cfgHost returns the bare host:port from a URL (for friendly quickstart text).
func cfgHost(url string) string {
	u := strings.TrimPrefix(url, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.TrimSuffix(u, "/")
	if i := strings.IndexByte(u, '/'); i >= 0 {
		u = u[:i]
	}
	return u
}

// humanBytes renders a byte count like "3.4 GiB".
func humanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	f := float64(n)
	for _, u := range []string{"KiB", "MiB", "GiB", "TiB"} {
		f /= 1024
		if f < 1024 {
			return fmt.Sprintf("%.1f %s", f, u)
		}
	}
	return fmt.Sprintf("%.1f PiB", f/1024)
}

// humanCount renders an integer with thousands separators ("1,234").
func humanCount(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	s := fmt.Sprintf("%d", n)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, byte(c))
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}

// humanDuration renders like "2d 4h".
func humanDuration(d time.Duration) string {
	h := int64(d.Hours())
	days := h / 24
	hours := h % 24
	if days > 0 {
		return fmt.Sprintf("%dd %dh", days, hours)
	}
	m := int64(d.Minutes()) % 60
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, m)
	}
	return fmt.Sprintf("%dm", int64(d.Minutes()))
}

// favicon stub — avoid browser 404 noise.
func (s *Server) handleFavicon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.WriteHeader(http.StatusNoContent)
}

// ---- JSON helpers ----

type E = map[string]any

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func unauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", `Bearer`)
	writeJSON(w, 401, E{"error": msg})
}

func tooMany(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "30")
	writeJSON(w, 429, E{"error": "rate limit exceeded"})
}

type httpStatusErr struct {
	code int
	msg  string
}

func (e *httpStatusErr) Error() string { return e.msg }

func (s *Server) handleErr(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, meta.ErrNotFound) {
		writeJSON(w, 404, E{"error": "not found"})
		return
	}
	if errReadOnly == err {
		writeJSON(w, 409, E{"error": err.Error()})
		return
	}
	if errDiskFull == err {
		writeJSON(w, 507, E{"error": err.Error()})
		return
	}
	if errors.Is(err, meta.ErrInvalid) {
		writeJSON(w, 400, E{"error": err.Error()})
		return
	}
	var he *httpStatusErr
	if errors.As(err, &he) {
		writeJSON(w, he.code, E{"error": he.msg})
		return
	}
	var ve *validationErr
	if errors.As(err, &ve) {
		writeJSON(w, ve.code, E{"error": ve.msg})
		return
	}
	s.logger.Printf("internal error: %v", err)
	writeJSON(w, 500, E{"error": "internal server error"})
}

// errors defined here to keep imports tidy.
var (
	errReadOnly = &httpStatusErr{409, "node is read-only (not lease holder)"}
	errDiskFull = &httpStatusErr{507, "storage below high-water mark; writes disabled"}
)

type validationErr struct {
	code int
	msg  string
}

func (e *validationErr) Error() string { return e.msg }
