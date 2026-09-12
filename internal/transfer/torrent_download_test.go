package transfer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"

	"storaged/internal/config"
	"storaged/internal/meta"
	"storaged/internal/store"
)

// tempDir is a temp directory whose cleanup tolerates Windows file locks:
// anacrolix's default (mmap) storage IO keeps torrent staging files open for the
// life of the process, so TestMain-level cleanup paths must not fail the test.
// The daemon reclaims that staging data when it next starts.
func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "storaged-xfer-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("leaving %s behind (locked): %v", dir, err)
		}
	})
	return dir
}

// newTestManager wires a real meta/store/manager against temp dirs.
func newTestManager(t *testing.T) (*Manager, *meta.Meta, *store.Store, string) {
	t.Helper()
	dataDir := tempDir(t)
	m, err := meta.New(filepath.Join(dataDir, "db", "meta.db"))
	if err != nil {
		t.Fatalf("meta.New: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	st := store.New(dataDir)
	if err := st.Init(); err != nil {
		t.Fatalf("store.Init: %v", err)
	}
	cfg := &config.Config{NodeID: "testnode", Role: config.RoleLeader, DataDir: dataDir, MaxBodyBytes: 1 << 30}
	mgr := NewManager(cfg, m, st, func() bool { return true })
	t.Cleanup(mgr.closeTorrentClient)
	bucket := "torrents"
	if err := m.PutBucket(bucket, true); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}
	return mgr, m, st, bucket
}

// makeWebseedTorrent builds a single-file torrent whose payload is served over
// plain HTTP (BEP 19 url-list), so tests need no peers or trackers.
func makeWebseedTorrent(t *testing.T, name string, payload []byte) []byte {
	t.Helper()
	dir := tempDir(t)
	if err := os.WriteFile(filepath.Join(dir, name), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	t.Cleanup(srv.Close)
	return buildSingleFileTorrent(t, filepath.Join(dir, name), []string{srv.URL + "/"})
}

// buildSingleFileTorrent builds a one-file torrent for srcFile with the given
// BEP 19 webseed URLs.
func buildSingleFileTorrent(t *testing.T, srcFile string, urlList []string) []byte {
	t.Helper()
	info := metainfo.Info{PieceLength: 32 << 10}
	if err := info.BuildFromFilePath(srcFile); err != nil {
		t.Fatalf("BuildFromFilePath: %v", err)
	}
	ib, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("bencode: %v", err)
	}
	mi := metainfo.MetaInfo{InfoBytes: bencode.Bytes(ib), UrlList: metainfo.UrlList(urlList)}
	return marshalTorrent(t, mi)
}

// throttleWebseed serves fixed bytes over HTTP, supporting Range requests the
// way a real webseed does. A non-zero rate limits the *total* served rate (the
// client opens several parallel requests, so per-request sleeps do not slow the
// download down), which lets a test cancel mid-download and later resume.
// total receives the number of payload bytes served over the server's life,
// which lets a test prove a retry resumed instead of re-fetching everything.
func throttleWebseed(data []byte, rate, total *atomic.Int64) http.HandlerFunc {
	var mu sync.Mutex
	var served int64
	var start time.Time
	wait := func(n int64) {
		total.Add(n)
		r := rate.Load()
		if r <= 0 {
			return
		}
		mu.Lock()
		if start.IsZero() {
			start = time.Now()
		}
		served += n
		behind := time.Duration(float64(served)/float64(r)*float64(time.Second)) - time.Since(start)
		mu.Unlock()
		if behind > 0 {
			time.Sleep(behind)
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		start, end := int64(0), int64(len(data))-1
		if rh := r.Header.Get("Range"); strings.HasPrefix(rh, "bytes=") {
			spec := strings.TrimPrefix(rh, "bytes=")
			segs := strings.SplitN(spec, "-", 2)
			if v, err := strconv.ParseInt(segs[0], 10, 64); err == nil {
				start = v
			}
			if len(segs) > 1 && segs[1] != "" {
				if v, err := strconv.ParseInt(segs[1], 10, 64); err == nil {
					end = v
				}
			}
		}
		if start < 0 {
			start = 0
		}
		if end > int64(len(data))-1 {
			end = int64(len(data)) - 1
		}
		if start > end {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		partial := start != 0 || end != int64(len(data))-1 || r.Header.Get("Range") != ""
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("Accept-Ranges", "bytes")
		code := http.StatusOK
		if partial {
			code = http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		}
		w.WriteHeader(code)
		for off := start; off <= end; {
			n := int64(16 << 10)
			if off+n-1 > end {
				n = end - off + 1
			}
			wait(n)
			if _, err := w.Write(data[off : off+n]); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			off += n
		}
	}
}

// makeWebseedDirTorrent builds a multi-file torrent from a directory tree. The
// webseed URL is the served root, and file paths are <torrent name>/<rel path>.
func makeWebseedDirTorrent(t *testing.T, torrentName string, files map[string][]byte) []byte {
	t.Helper()
	root := tempDir(t)
	srcDir := filepath.Join(root, torrentName)
	for rel, b := range files {
		p := filepath.Join(srcDir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(root)))
	t.Cleanup(srv.Close)

	info := metainfo.Info{PieceLength: 32 << 10}
	if err := info.BuildFromFilePath(srcDir); err != nil {
		t.Fatalf("BuildFromFilePath: %v", err)
	}
	ib, err := bencode.Marshal(info)
	if err != nil {
		t.Fatalf("bencode: %v", err)
	}
	mi := metainfo.MetaInfo{InfoBytes: bencode.Bytes(ib), UrlList: metainfo.UrlList{srv.URL + "/"}}
	return marshalTorrent(t, mi)
}

func marshalTorrent(t *testing.T, mi metainfo.MetaInfo) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := mi.Write(&buf); err != nil {
		t.Fatalf("write torrent: %v", err)
	}
	return buf.Bytes()
}

func TestDownloadTorrentSingleFile(t *testing.T) {
	mgr, m, st, bucket := newTestManager(t)
	payload := bytes.Repeat([]byte("storaged-torrent-payload-"), 6000) // ~150 KiB
	raw := makeWebseedTorrent(t, "hello.bin", payload)

	tr := meta.Transfer{
		ID:          "xf_test_single",
		Bucket:      bucket,
		Kind:        meta.KindTorrent,
		TorrentData: base64.StdEncoding.EncodeToString(raw),
	}
	if err := mgr.downloadTorrent(context.Background(), tr); err != nil {
		t.Fatalf("downloadTorrent: %v", err)
	}

	obj, err := m.GetObject(bucket, "hello.bin")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Errorf("object size = %d, want %d", obj.Size, len(payload))
	}
	f, err := st.OpenBlob(obj.BlobHash)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	defer f.Close()
	got, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("committed blob does not match the torrent payload (%d vs %d bytes)", len(got), len(payload))
	}
}

func TestDownloadTorrentMultiFile(t *testing.T) {
	mgr, m, _, bucket := newTestManager(t)
	files := map[string][]byte{
		"readme.txt":         []byte("hello from the multi file torrent"),
		"media/clip.bin":     bytes.Repeat([]byte("video-bytes-"), 4000),
		"media/sub/tiny.pad": []byte("pad"),
		"media/sub/a b.txt":  []byte("space and dot in name"),
	}
	raw := makeWebseedDirTorrent(t, "release", files)

	tr := meta.Transfer{
		ID:          "xf_test_multi",
		Bucket:      bucket,
		Key:         "release",
		Kind:        meta.KindTorrent,
		TorrentData: base64.StdEncoding.EncodeToString(raw),
	}
	if err := mgr.downloadTorrent(context.Background(), tr); err != nil {
		t.Fatalf("downloadTorrent: %v", err)
	}

	ents, _, err := m.ListObjects(context.Background(), bucket, meta.ListFilter{Prefix: "release/"})
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	var keys []string
	for _, e := range ents {
		keys = append(keys, e.Key)
	}
	t.Logf("committed keys: %v", keys)
	if len(keys) != 3 {
		t.Fatalf("expected 3 committed objects (padding file skipped), got %d: %v", len(keys), keys)
	}
	want := map[string]int{
		"release/readme.txt":        len(files["readme.txt"]),
		"release/media/clip.bin":    len(files["media/clip.bin"]),
		"release/media/sub/a b.txt": len(files["media/sub/a b.txt"]),
	}
	for key, size := range want {
		obj, err := m.GetObject(bucket, key)
		if err != nil {
			t.Errorf("GetObject(%q): %v", key, err)
			continue
		}
		if obj.Size != int64(size) {
			t.Errorf("%s size = %d, want %d", key, obj.Size, size)
		}
	}
}

// TestTransferWorkerTorrent drives the real worker loop (Start/drain/Notify) so
// queued -> downloading -> done transitions run the way the daemon runs them.
func TestTransferWorkerTorrent(t *testing.T) {
	mgr, m, st, bucket := newTestManager(t)
	raw := makeWebseedTorrent(t, "queued.bin", bytes.Repeat([]byte("queued-payload-"), 2000))

	id, err := m.CreateTorrentTransfer(bucket, "via-worker", "", base64.StdEncoding.EncodeToString(raw))
	if err != nil {
		t.Fatalf("CreateTorrentTransfer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Start(ctx)
	mgr.Notify()

	deadline := time.Now().Add(60 * time.Second)
	for {
		tr, err := m.GetTransfer(id)
		if err != nil {
			t.Fatalf("GetTransfer: %v", err)
		}
		switch tr.Status {
		case meta.StatusDone:
			obj, err := m.GetObject(bucket, "via-worker")
			if err != nil {
				t.Fatalf("GetObject after done: %v", err)
			}
			f, err := st.OpenBlob(obj.BlobHash)
			if err != nil {
				t.Fatalf("OpenBlob: %v", err)
			}
			defer f.Close()
			got, _ := io.ReadAll(f)
			if obj.Size == 0 || len(got) != int(obj.Size) {
				t.Errorf("blob size mismatch: %d vs %d", len(got), obj.Size)
			}
			return
		case meta.StatusFailed:
			t.Fatalf("transfer failed: %s", tr.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("transfer stuck in %q (progress %d/%d)", tr.Status, tr.Progress, tr.Size)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestDownloadTorrentMagnetViaTracker exercises the magnet path end to end: a
// local HTTP tracker hands out a seeder's address, metadata is fetched from the
// peer (BEP 9), pieces download, and the object is committed.
func TestDownloadTorrentMagnetViaTracker(t *testing.T) {
	payload := bytes.Repeat([]byte("magnet-payload-"), 3000) // ~45 KiB
	srcDir := tempDir(t)
	srcName := "hello.bin"
	if err := os.WriteFile(filepath.Join(srcDir, srcName), payload, 0o600); err != nil {
		t.Fatal(err)
	}

	info := metainfo.Info{PieceLength: 16 << 10}
	if err := info.BuildFromFilePath(filepath.Join(srcDir, srcName)); err != nil {
		t.Fatalf("BuildFromFilePath: %v", err)
	}

	var peerMu sync.Mutex
	seen := map[string]bool{}
	tracker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		if host == "::1" {
			host = "127.0.0.1"
		}
		self := host + ":" + r.URL.Query().Get("port")
		peerMu.Lock()
		seen[self] = true
		known := make([]string, 0, len(seen))
		for p := range seen {
			if p != self {
				known = append(known, p)
			}
		}
		peerMu.Unlock()

		var compact []byte
		for _, p := range known {
			hp, pp, err := net.SplitHostPort(p)
			if err != nil {
				continue
			}
			n, err := strconv.Atoi(pp)
			if err != nil {
				continue
			}
			ip := net.ParseIP(hp).To4()
			if ip == nil {
				continue
			}
			b := make([]byte, 6)
			copy(b, ip)
			binary.BigEndian.PutUint16(b[4:], uint16(n))
			compact = append(compact, b...)
		}
		var resp bytes.Buffer
		resp.WriteString("d8:intervali30e5:peers")
		resp.WriteString(strconv.Itoa(len(compact)))
		resp.WriteByte(':')
		resp.Write(compact)
		resp.WriteByte('e')
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(resp.Bytes())
	}))
	defer tracker.Close()

	announce := tracker.URL + "/announce"
	ib, err := bencode.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	seederMI := metainfo.MetaInfo{
		InfoBytes:    bencode.Bytes(ib),
		Announce:     announce,
		AnnounceList: metainfo.AnnounceList{{announce}},
	}
	sc := torrent.NewDefaultClientConfig()
	sc.DataDir = srcDir
	sc.ListenPort = 0
	sc.Seed = true
	sc.NoDHT = true
	sc.DisableIPv6 = true
	seeder, err := torrent.NewClient(sc)
	if err != nil {
		t.Fatalf("seeder client: %v", err)
	}
	defer seeder.Close()
	st, _, err := seeder.AddTorrentSpec(torrent.TorrentSpecFromMetaInfo(&seederMI))
	if err != nil {
		t.Fatalf("seeder add: %v", err)
	}
	<-st.GotInfo()

	// wait for the seeder's tracker announce
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		peerMu.Lock()
		n := len(seen)
		peerMu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	magnet := "magnet:?xt=urn:btih:" + seederMI.HashInfoBytes().HexString() + "&dn=" + srcName +
		"&tr=" + url.QueryEscape(announce)

	mgr, m, _, bucket := newTestManager(t)
	tr := meta.Transfer{ID: "xf_test_magnet", Bucket: bucket, Key: "from-magnet", Kind: meta.KindTorrent, URL: magnet}
	if err := mgr.downloadTorrent(context.Background(), tr); err != nil {
		t.Fatalf("downloadTorrent(magnet): %v", err)
	}
	obj, err := m.GetObject(bucket, "from-magnet")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if obj.Size != int64(len(payload)) {
		t.Errorf("object size = %d, want %d", obj.Size, len(payload))
	}
}

// TestTorrentRetryResumes cancels a torrent mid-download, then re-queues it and
// checks the retry resumes from the staged pieces and still commits the exact
// payload (a mis-resumed piece would change the content hash).
func TestTorrentRetryResumes(t *testing.T) {
	mgr, m, st, bucket := newTestManager(t)
	// big enough that the throttled webseed is still serving when we cancel
	payload := bytes.Repeat([]byte("resume-payload-0123456789"), 1<<17) // ~3.4 MiB
	srcDir := tempDir(t)
	srcFile := filepath.Join(srcDir, "resume.bin")
	if err := os.WriteFile(srcFile, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	const slowRate = 192 << 10 // ~192 KiB/s
	var rate, served atomic.Int64
	rate.Store(slowRate)
	srv := httptest.NewServer(throttleWebseed(payload, &rate, &served))
	t.Cleanup(srv.Close)
	raw := buildSingleFileTorrent(t, srcFile, []string{srv.URL + "/"})

	id, err := m.CreateTorrentTransfer(bucket, "resumed", "", base64.StdEncoding.EncodeToString(raw))
	if err != nil {
		t.Fatalf("CreateTorrentTransfer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Start(ctx)
	mgr.Notify()

	// cancel once roughly half the payload has arrived, so a retry that actually
	// resumes has less left to fetch than a from-scratch download would.
	half := int64(len(payload)) / 2
	waitFor(t, 90*time.Second, func() bool {
		tr, err := m.GetTransfer(id)
		if err != nil {
			return false
		}
		if tr.Status == meta.StatusFailed {
			t.Fatalf("transfer failed before we could cancel it: %s", tr.Error)
		}
		return tr.Progress >= half
	}, "half the payload to download")
	mgr.Cancel(id)

	// pieces in flight live in <name>.part; the cancel must keep them so a retry
	// can resume, and must not leak them when the transfer is dropped
	partFile := stagedPath(mgr.torrentStageDir(), "resume.bin") + ".part"
	if !fileExists(partFile) {
		t.Errorf("expected the in-progress staging file %s to exist before the cancel", partFile)
	}
	waitFor(t, 30*time.Second, func() bool {
		tr, err := m.GetTransfer(id)
		return err == nil && tr.Status == meta.StatusCancelled
	}, "transfer to reach cancelled")
	if !fileExists(partFile) {
		t.Errorf("staged data %s was deleted on cancel; a retry cannot resume", partFile)
	}
	firstAttempt := served.Load()
	t.Logf("cancelled after %s served by the webseed", byteSize(firstAttempt))

	// resume: let the webseed run at full speed and re-queue the transfer
	rate.Store(0)
	if err := m.RetryTransfer(id); err != nil {
		t.Fatalf("RetryTransfer: %v", err)
	}
	mgr.Notify()
	waitFor(t, 60*time.Second, func() bool {
		tr, err := m.GetTransfer(id)
		if err != nil {
			return false
		}
		if tr.Status == meta.StatusFailed {
			t.Fatalf("resumed transfer failed: %s", tr.Error)
		}
		return tr.Status == meta.StatusDone
	}, "resumed transfer to finish")

	obj, err := m.GetObject(bucket, "resumed")
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	f, err := st.OpenBlob(obj.BlobHash)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	defer f.Close()
	got, _ := io.ReadAll(f)
	if !bytes.Equal(got, payload) {
		t.Errorf("resumed blob mismatch: %d bytes, want %d (data was not resumed correctly)", len(got), len(payload))
	}

	// the retry fetched the remainder, not the whole torrent again
	refetched := served.Load() - firstAttempt
	if refetched > int64(len(payload))*3/4 {
		t.Errorf("retry re-downloaded %s of a %s payload (cancelled at %s): the staged data was not resumed",
			byteSize(refetched), byteSize(int64(len(payload))), byteSize(firstAttempt))
	}
}

// fileExists reports whether path exists (file or directory).
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestDownloadTorrentDeadSwarmFailsFast checks a magnet with no reachable peers
// fails with a readable reason instead of hanging forever.
func TestDownloadTorrentDeadSwarmFailsFast(t *testing.T) {
	mgr, _, _, bucket := newTestManager(t)
	old := TorrentMetadataTimeout
	TorrentMetadataTimeout = 2 * time.Second
	defer func() { TorrentMetadataTimeout = old }()

	// a real infohash with no trackers and no peers: DHT will not find it
	magnet := "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
	tr := meta.Transfer{ID: "xf_test_dead", Bucket: bucket, Key: "dead", Kind: meta.KindTorrent, URL: magnet}

	done := make(chan error, 1)
	go func() { done <- mgr.downloadTorrent(context.Background(), tr) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error for a magnet with no peers")
		}
		t.Logf("failure (as intended): %v", err)
	case <-time.After(45 * time.Second):
		t.Fatal("downloadTorrent hung instead of failing on the metadata timeout")
	}
}
