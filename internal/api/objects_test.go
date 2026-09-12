package api

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// Regression: object keys derived from uploaded filenames commonly contain
// spaces (e.g. screenshot names), which used to be rejected as invalid.
func TestObjectKeyWithSpaceRoundTrip(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")

	const key = "Screenshot 2026-07-11 130201.png"
	path := "/v1/buckets/media/objects/" + url.PathEscape(key)
	body := []byte("png-bytes")

	if w := doReq(t, h, http.MethodPut, path, testAdmin, "image/png", body); w.Code != 201 {
		t.Fatalf("put: status %d %s", w.Code, w.Body.String())
	}

	w := doReq(t, h, http.MethodGet, path, testAdmin, "", nil)
	if w.Code != 200 {
		t.Fatalf("get: status %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != string(body) {
		t.Fatalf("get body = %q, want %q", got, body)
	}
}

// A presigned URL for a spaced key must be a valid, parseable URL (no raw
// space) that still verifies against the original key.
func TestPresignedURLWithSpacedKey(t *testing.T) {
	h := newTestAPI(t)
	createBucket(t, h, "media")

	const key = "Screenshot 2026-07-11 130201.png"
	w := doReq(t, h, http.MethodPost,
		"/v1/buckets/media/uploads/upload-url?key="+url.QueryEscape(key),
		testAdmin, "", nil)
	if w.Code != 200 {
		t.Fatalf("upload-url: status %d %s", w.Code, w.Body.String())
	}

	var out struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode upload-url: %v (body %s)", err, w.Body.String())
	}
	if strings.Contains(out.URL, " ") {
		t.Fatalf("presigned URL contains a raw space: %q", out.URL)
	}
	if _, err := url.Parse(out.URL); err != nil {
		t.Fatalf("presigned URL does not parse: %v (%q)", err, out.URL)
	}

	// The signed PUT must be accepted without a bearer token.
	if w := doReq(t, h, http.MethodPut, out.URL, "", "image/png", []byte("png-bytes")); w.Code != 201 {
		t.Fatalf("signed put: status %d %s", w.Code, w.Body.String())
	}
}
