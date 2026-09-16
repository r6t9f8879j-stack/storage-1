package api

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
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

// netProbeResult is one transport reachability probe against a tracker URL.
type netProbeResult struct {
	URL     string `json:"url"`
	Proto   string `json:"proto"`   // "tcp(HTTP)" or "udp"
	OK      bool   `json:"ok"`      // did the transport reach the tracker?
	Detail  string `json:"detail"`  // human-readable outcome
	Elapsed string `json:"elapsed"` // round-trip time, e.g. "120ms"
}

// handleDebugNet probes each configured tracker URL over the transport it uses
// (HTTP/S announce URLs go out over TCP, udp:// URLs over raw UDP). GitHub-
// hosted runners sit behind an Azure NAT that commonly filters UDP while
// allowing outbound TCP, which is exactly why torrents stall at 0% there (uTP,
// DHT and udp:// trackers all depend on UDP). This endpoint shows, from the
// running node, which transports can actually reach the public tracker network
// so a "no metadata / stalls at 0%" failure can be attributed to the network
// rather than the swarm.
func (s *Server) handleDebugNet(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 40*time.Second)
	defer cancel()

	probeTimeout := 6 * time.Second
	results := make([]netProbeResult, 0, len(s.cfg.TorrentTrackers))
	for _, raw := range s.cfg.TorrentTrackers {
		u, err := url.Parse(raw)
		if err != nil {
			results = append(results, netProbeResult{URL: raw, Proto: "other", Detail: "unparsable: " + err.Error()})
			continue
		}
		res := netProbeResult{URL: raw, Proto: "other"}
		start := time.Now()
		switch {
		case u.Scheme == "http" || u.Scheme == "https":
			res.Proto = "tcp(HTTP)"
			res.Detail, res.OK = probeHTTPTracker(ctx, u, probeTimeout)
		case u.Scheme == "udp":
			res.Proto = "udp"
			res.Detail, res.OK = probeUDPTracker(ctx, u, probeTimeout)
		default:
			res.Detail = "unsupported scheme " + u.Scheme
		}
		res.Elapsed = time.Since(start).Round(time.Millisecond).String()
		results = append(results, res)
	}

	writeJSON(w, 200, E{
		"note": "GitHub-hosted runners: outbound TCP generally works; outbound UDP and inbound connections are typically unavailable",
		"results": results,
		"transport": E{
			"udp_all_blocked": allProtoBlocked(results, "udp"),
			"tcp_all_ok":      allProtoOK(results, "tcp(HTTP)"),
		},
	})
}

// probeHTTPTracker dials a tracker announce host over TCP and performs a minimal
// HTTP request. Any HTTP response (even 4xx from a missing announce params)
// proves TCP+HTTP egress to that tracker.
func probeHTTPTracker(ctx context.Context, u *url.URL, timeout time.Duration) (string, bool) {
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	addr := net.JoinHostPort(u.Hostname(), port)
	d := net.Dialer{}
	conn, err := d.DialContext(dctx, "tcp", addr)
	if err != nil {
		return "no TCP connection: " + err.Error(), false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: storaged-net-probe/1.0\r\nConnection: close\r\n\r\n",
		u.RequestURI(), u.Host)
	if _, err := conn.Write([]byte(req)); err != nil {
		return "write failed: " + err.Error(), false
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return "no HTTP reply: " + err.Error(), false
	}
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "HTTP/") {
		return "tracker did not answer HTTP: " + line, false
	}
	return "HTTP reachable: " + line, true
}

// probeUDPTracker performs the BEP 15 UDP-tracker connect handshake (a 16-byte
// connect request and a matching 16-byte connect response). A response proves
// outbound UDP to the tracker's port is not filtered.
func probeUDPTracker(ctx context.Context, u *url.URL, timeout time.Duration) (string, bool) {
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	host := u.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, "80") // BEP 15 default tracker port
	}
	pc, err := net.Dial("udp", host)
	if err != nil {
		return "no UDP socket: " + err.Error(), false
	}
	defer pc.Close()
	_ = pc.SetDeadline(time.Now().Add(timeout))

	tx := uint32(time.Now().UnixNano() & 0xffffffff)
	req := make([]byte, 16)
	binary.BigEndian.PutUint64(req[0:], 0x41727101980) // BEP 15 magic
	binary.BigEndian.PutUint32(req[8:], 0)             // action: connect
	binary.BigEndian.PutUint32(req[12:], tx)
	if _, err := pc.Write(req); err != nil {
		return "send failed: " + err.Error(), false
	}

	buf := make([]byte, 64)
	for {
		if dctx.Err() != nil {
			return "no UDP response within " + timeout.String() + " (UDP egress likely filtered)", false
		}
		deadline := time.Now().Add(3 * time.Second)
		_ = pc.SetDeadline(deadline)
		n, err := pc.Read(buf)
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue
			}
			return "read failed: " + err.Error(), false
		}
		if n >= 16 && binary.BigEndian.Uint32(buf[8:]) == 0 && binary.BigEndian.Uint32(buf[12:]) == tx {
			connID := binary.BigEndian.Uint64(buf[0:])
			return fmt.Sprintf("UDP reachable (connect ack conn_id=%d)", connID), true
		}
	}
}

// allProtoBlocked reports whether every result of a proto failed.
func allProtoBlocked(results []netProbeResult, proto string) bool {
	matched := false
	for _, rs := range results {
		if rs.Proto != proto {
			continue
		}
		matched = true
		if rs.OK {
			return false
		}
	}
	return matched
}

// allProtoOK reports whether every result of a proto succeeded (false when the
// proto has no results).
func allProtoOK(results []netProbeResult, proto string) bool {
	matched := false
	for _, rs := range results {
		if rs.Proto != proto {
			continue
		}
		matched = true
		if !rs.OK {
			return false
		}
	}
	return matched
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
		"GET      /v1/debug/net",
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