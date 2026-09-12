package auth

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// Regression: SignURL used to splice the raw key into the path, producing an
// invalid URL (and broken signature verification) for keys with spaces.
func TestSignURLPercentEncodesKey(t *testing.T) {
	a := New("admin-token", "read-token", "peer-secret", 10000)
	const key = "Screenshot 2026-07-11 130201.png"

	raw := a.SignURL("GET", "media", key, time.Minute, "https://storage.example")
	if strings.Contains(raw, " ") {
		t.Fatalf("signed URL contains a raw space: %q", raw)
	}

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", raw, err)
	}
	// url.Parse decodes the escaped path, so it must end with the original key.
	if !strings.HasSuffix(u.Path, key) {
		t.Fatalf("decoded path %q does not end with key %q", u.Path, key)
	}
	if err := a.validateSignedURL("GET", "media", key,
		u.Query().Get("sig"), u.Query().Get("expires"), time.Now()); err != nil {
		t.Fatalf("validateSignedURL: %v", err)
	}
}
