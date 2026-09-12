package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"

	"storaged/internal/auth"
	"storaged/internal/config"
	"storaged/internal/meta"
	"storaged/internal/store"
)

const (
	testAdmin = "admin-token"
	testRead  = "read-token"
)

func newTestAPI(t *testing.T) http.Handler {
	t.Helper()
	m, err := meta.New(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("meta.New: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	dataDir := t.TempDir()
	cfg := &config.Config{
		NodeID:       "testnode",
		Role:         config.RoleLeader,
		DataDir:      dataDir,
		MaxBodyBytes: 2 << 30,
	}
	a := auth.New(testAdmin, testRead, "peer-secret", 10000)
	st := store.New(dataDir)
	if err := st.Init(); err != nil {
		t.Fatalf("store.Init: %v", err)
	}
	return New(cfg, a, m, st).Handler()
}

func doReq(t *testing.T, h http.Handler, method, path, token, ctype string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if ctype != "" {
		r.Header.Set("Content-Type", ctype)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func createBucket(t *testing.T, h http.Handler, name string) {
	t.Helper()
	w := doReq(t, h, http.MethodPut, "/v1/buckets/"+name, testAdmin, "", nil)
	if w.Code != 200 {
		t.Fatalf("create bucket: status %d %s", w.Code, w.Body.String())
	}
}

func decodeTransfer(t *testing.T, w *httptest.ResponseRecorder) *meta.Transfer {
	t.Helper()
	var out struct {
		Transfer *meta.Transfer `json:"transfer"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode transfer: %v (body %s)", err, w.Body.String())
	}
	if out.Transfer == nil {
		t.Fatalf("no transfer in response: %s", w.Body.String())
	}
	return out.Transfer
}

// makeTorrent writes a real single-file torrent so the raw .torrent path is
// exercised with valid metainfo (correct piece hashes et al).
func makeTorrent(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), bytes.Repeat([]byte("x"), 256), 0o600); err != nil {
		t.Fatal(err)
	}
	info := metainfo.Info{}
	if err := info.BuildFromFilePath(dir); err != nil {
		t.Fatalf("BuildFromFilePath: %v", err)
	}
	b, err := bencode.Marshal(&info)
	if err != nil {
		t.Fatalf("bencode: %v", err)
	}
	mi := metainfo.MetaInfo{InfoBytes: bencode.Bytes(b)}
	var buf bytes.Buffer
	if err := mi.Write(&buf); err != nil {
		t.Fatalf("write torrent: %v", err)
	}
	return buf.Bytes()
}

func TestCreateTransferURL(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")

	w := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers", testAdmin, "application/json",
		[]byte(`{"url":"http://example.com/a/b/big.iso","key":"videos/big.iso"}`))
	if w.Code != 201 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	tf := decodeTransfer(t, w)
	if tf.Kind != meta.KindURL || tf.Bucket != "media" || tf.Key != "videos/big.iso" || tf.Status != "queued" {
		t.Errorf("unexpected transfer: %+v", tf)
	}
	if tf.URL != "http://example.com/a/b/big.iso" {
		t.Errorf("url = %q", tf.URL)
	}
}

func TestCreateTransferMagnetDefaultKey(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")

	magnet := "magnet:?xt=urn:btih:ABCDEF0123456789ABCDEF0123456789ABCDEF01&dn=Test&tr=udp%3A%2F%2Ftracker.example%3A6969"
	w := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers", testAdmin, "application/json",
		[]byte(`{"magnet":"`+magnet+`"}`))
	if w.Code != 201 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	tf := decodeTransfer(t, w)
	if tf.Kind != meta.KindTorrent {
		t.Errorf("kind = %q, want torrent", tf.Kind)
	}
	if tf.Key != "magnet_abcdef012345" {
		t.Errorf("default magnet key = %q, want magnet_abcdef012345", tf.Key)
	}
	if tf.URL != magnet {
		t.Errorf("url stores magnet URI")
	}
	if tf.TorrentData != "" {
		t.Errorf("magnet should not carry base64 metainfo")
	}
}

// The dashboard sends the object key as a ?key= query param; JSON bodies may
// override it. Both must land the object where the user asked.
func TestCreateTransferQueryKeyApplied(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")
	magnet := "magnet:?xt=urn:btih:ABCDEF0123456789ABCDEF0123456789ABCDEF01&dn=Test"

	w := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers?key=shows/s01", testAdmin, "application/json",
		[]byte(`{"magnet":"`+magnet+`"}`))
	if w.Code != 201 {
		t.Fatalf("magnet with query key: status %d: %s", w.Code, w.Body.String())
	}
	if tf := decodeTransfer(t, w); tf.Key != "shows/s01" {
		t.Errorf("magnet query key = %q, want shows/s01", tf.Key)
	}

	w = doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers?key=ignored", testAdmin, "application/json",
		[]byte(`{"magnet":"`+magnet+`","key":"body/wins"}`))
	if w.Code != 201 {
		t.Fatalf("magnet with body key: status %d: %s", w.Code, w.Body.String())
	}
	if tf := decodeTransfer(t, w); tf.Key != "body/wins" {
		t.Errorf("magnet body key = %q, want body/wins", tf.Key)
	}

	w = doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers?key=videos/big.iso", testAdmin, "application/json",
		[]byte(`{"url":"http://example.com/a/b/big.iso"}`))
	if w.Code != 201 {
		t.Fatalf("url with query key: status %d: %s", w.Code, w.Body.String())
	}
	if tf := decodeTransfer(t, w); tf.Key != "videos/big.iso" {
		t.Errorf("url query key = %q, want videos/big.iso", tf.Key)
	}

	// an invalid query key is still rejected
	w = doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers?key="+url.QueryEscape("../escape"), testAdmin, "application/json",
		[]byte(`{"magnet":"`+magnet+`"}`))
	if w.Code != 400 {
		t.Errorf("invalid query key: status %d, want 400", w.Code)
	}
}

func TestCreateTransferRawTorrent(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")
	raw := makeTorrent(t)

	w := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers", testAdmin, "application/x-bittorrent", raw)
	if w.Code != 201 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	tf := decodeTransfer(t, w)
	if tf.Kind != meta.KindTorrent || tf.Status != "queued" {
		t.Fatalf("unexpected transfer: %+v", tf)
	}
	b64, err := base64.StdEncoding.DecodeString(tf.TorrentData)
	if err != nil || !bytes.Equal(b64, raw) {
		t.Errorf("torrent_data does not roundtrip the uploaded bytes")
	}

	// listing must not leak the base64 payload
	lw := doReq(t, h, http.MethodGet, "/v1/buckets/media/transfers", testRead, "", nil)
	if lw.Code != 200 {
		t.Fatalf("list status %d", lw.Code)
	}
	var list struct {
		Transfers []meta.Transfer `json:"transfers"`
	}
	if err := json.Unmarshal(lw.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	for _, tr := range list.Transfers {
		if tr.TorrentData != "" {
			t.Errorf("listing leaked torrent_data for %s", tr.ID)
		}
	}
}

func TestCreateTransferMissingBucketRejected(t *testing.T) {
	h := newTestAPI(t)
	w := doReq(t, h, http.MethodPost, "/v1/buckets/nope/transfers", testAdmin, "application/json",
		[]byte(`{"url":"http://example.com/x.iso"}`))
	if w.Code != 404 {
		t.Errorf("missing bucket status = %d, want 404 (%s)", w.Code, w.Body.String())
	}
}

func TestCreateTransferRejectsBadURL(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")
	for _, body := range []string{`{"url":"ftp://host/file"}`, `{"url":"not a url"}`, `{}`} {
		w := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers", testAdmin, "application/json", []byte(body))
		if w.Code != 400 {
			t.Errorf("body %s -> status %d, want 400", body, w.Code)
		}
	}
}

func TestCancelTransfer(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")
	cw := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers", testAdmin, "application/json",
		[]byte(`{"url":"http://example.com/slow.iso","key":"k"}`))
	tf := decodeTransfer(t, cw)

	w := doReq(t, h, http.MethodDelete, "/v1/buckets/media/transfers/"+tf.ID, testAdmin, "", nil)
	if w.Code != 204 {
		t.Fatalf("cancel status = %d (%s)", w.Code, w.Body.String())
	}
	gw := doReq(t, h, http.MethodGet, "/v1/transfers/"+tf.ID, testRead, "", nil)
	var got struct {
		Transfer *meta.Transfer `json:"transfer"`
	}
	if err := json.Unmarshal(gw.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Transfer.Status != "cancelled" {
		t.Errorf("status after cancel = %q, want cancelled", got.Transfer.Status)
	}
}

func TestRetryTransferEndpoint(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")
	cw := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers", testAdmin, "application/json",
		[]byte(`{"url":"http://example.com/slow.iso","key":"k"}`))
	tf := decodeTransfer(t, cw)

	// queued transfers are still running: nothing to retry yet
	w := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers/"+tf.ID+"/retry", testAdmin, "", nil)
	if w.Code != 400 {
		t.Errorf("retry of a queued transfer = %d, want 400 (%s)", w.Code, w.Body.String())
	}

	// a stopped (cancelled or failed) transfer can be re-queued in place
	doReq(t, h, http.MethodDelete, "/v1/buckets/media/transfers/"+tf.ID, testAdmin, "", nil)
	w = doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers/"+tf.ID+"/retry", testAdmin, "", nil)
	if w.Code != 200 {
		t.Fatalf("retry after cancel = %d, want 200 (%s)", w.Code, w.Body.String())
	}
	if got := decodeTransfer(t, w); got.Status != "queued" || got.Error != "" {
		t.Errorf("retried transfer = %+v, want queued with no error", got)
	}

	// unknown id / wrong bucket
	w = doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers/nope/retry", testAdmin, "", nil)
	if w.Code != 404 {
		t.Errorf("retry of a missing transfer = %d, want 404", w.Code)
	}
	w = doReq(t, h, http.MethodPost, "/v1/buckets/other/transfers/"+tf.ID+"/retry", testAdmin, "", nil)
	if w.Code != 404 {
		t.Errorf("retry under the wrong bucket = %d, want 404", w.Code)
	}

	// and it needs admin credentials
	w = doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers/"+tf.ID+"/retry", testRead, "", nil)
	if w.Code != 401 {
		t.Errorf("retry with a read key = %d, want 401", w.Code)
	}
}

func TestTransferRequiresAuth(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")
	w := doReq(t, h, http.MethodPost, "/v1/buckets/media/transfers", "", "application/json",
		[]byte(`{"url":"http://example.com/x.iso"}`))
	if w.Code != 401 {
		t.Errorf("no-token create status = %d, want 401", w.Code)
	}
}
