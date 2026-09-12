package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Scope of an authenticated request.
type Scope int

const (
	ScopeNone Scope = iota
	ScopeRead
	ScopeAdmin
)

// Auth validates bearer keys, signs/validates presigned URLs, and rate-limits.
type Auth struct {
	adminHash [32]byte
	readHash  [32]byte
	peerHash  [32]byte

	signKey [32]byte // HMAC key for presigned URLs, derived from admin key

	perMinute int

	mu     sync.Mutex
	buckets map[string]*bucket

	// Supabase session validation (dashboard login). Any authenticated
	// Supabase user is granted admin scope.
	supabaseOn      bool
	supabaseURL     string
	supabaseAnon    string
	supabaseService string
	supClient       *http.Client
	supCache        map[[32]byte]supEntry
}

type supEntry struct {
	exp time.Time
}

const supTTL = 10 * time.Minute

type bucket struct {
	tokens float64
	last   time.Time
}

// New builds an Auth from raw secret strings. Keys may be anything; they are
// compared via sha256 hashes. A deterministic signature key is derived from
// the admin key so presigned URLs survive restarts.
func New(adminKey, readKey, peerSecret string, ratePerMinute int) *Auth {
	var a Auth
	a.adminHash = sha256.Sum256([]byte(adminKey))
	a.readHash = sha256.Sum256([]byte(readKey))
	a.peerHash = sha256.Sum256([]byte(peerSecret))
	a.signKey = sha256.Sum256([]byte("storaged:sig:" + adminKey))
	a.perMinute = ratePerMinute
	a.buckets = make(map[string]*bucket)
	return &a
}

// HashKey hashes a secret for config files / logs (never log raw keys).
func HashKey(secret string) string {
	h := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(h[:16])
}

// validBearer compares a presented secret against a stored sha256 hash.
func validBearer(presented string, hash [32]byte) bool {
	h := sha256.Sum256([]byte(presented))
	return subtle.ConstantTimeCompare(h[:], hash[:]) == 1
}

// Authenticate resolves bearer credentials to a scope.
func (a *Auth) Authenticate(bearer string) Scope {
	if bearer == "" {
		return ScopeNone
	}
	if validBearer(bearer, a.adminHash) {
		return ScopeAdmin
	}
	if validBearer(bearer, a.readHash) {
		return ScopeRead
	}
	if a.supabaseOn && a.validSupabaseToken(bearer) {
		return ScopeAdmin
	}
	return ScopeNone
}

// EnableSupabase turns on dashboard login: bearer tokens that successfully
// validate against a Supabase project's auth endpoint are granted admin scope.
// serviceKey (optional) additionally enables account creation and session
// minting through the /v1/auth/* proxy, which bypasses GoTrue's per-IP
// signup rate limits.
func (a *Auth) EnableSupabase(url, anonKey, serviceKey string) {
	if url == "" || anonKey == "" {
		return
	}
	a.supabaseURL = strings.TrimRight(url, "/")
	a.supabaseAnon = anonKey
	a.supabaseService = serviceKey
	a.supClient = &http.Client{Timeout: 10 * time.Second}
	a.supCache = make(map[[32]byte]supEntry)
	a.supabaseOn = true
}

// SupabaseEnabled reports whether this node validates dashboard (Supabase)
// sessions in addition to the static bearer keys.
func (a *Auth) SupabaseEnabled() bool { return a.supabaseOn }

// SupabaseSignup creates a user via the Admin API (service key, auto-confirmed
// email) so signup is not subject to GoTrue's public per-IP rate limit.
func (a *Auth) SupabaseSignup(ctx context.Context, email, password string) error {
	if !a.supabaseOn || a.supabaseService == "" {
		return fmt.Errorf("account creation is not configured on this node")
	}
	body, _ := json.Marshal(map[string]any{
		"email":         email,
		"password":      password,
		"email_confirm": true,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.supabaseURL+"/auth/v1/admin/users", bytes.NewReader(body))
	if err != nil {
		return err
	}
	a.setServiceHeaders(req)
	res, err := a.supClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusCreated {
		return supErr(res.StatusCode, raw)
	}
	return nil
}

// SupabaseToken mints a session for email/password via the password grant
// using the service key. It bypasses GoTrue's public per-IP login limiting
// while keeping the secret server-side. The returned map matches the GoTrue
// /token response plus an expires_at epoch for convenience.
func (a *Auth) SupabaseToken(ctx context.Context, email, password string) (map[string]any, error) {
	if !a.supabaseOn || a.supabaseService == "" {
		return nil, fmt.Errorf("login is not configured on this node")
	}
	body, _ := json.Marshal(map[string]any{"email": email, "password": password})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.supabaseURL+"/auth/v1/token?grant_type=password", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	a.setServiceHeaders(req)
	res, err := a.supClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusOK {
		return nil, supErr(res.StatusCode, raw)
	}
	var tok map[string]any
	if err := json.Unmarshal(raw, &tok); err != nil {
		return nil, fmt.Errorf("bad token response: %v", err)
	}
	if tok["access_token"] == nil {
		return nil, fmt.Errorf("no access_token in response")
	}
	if tok["expires_in"] != nil {
		if secs, ok := tok["expires_in"].(float64); ok {
			tok["expires_at"] = time.Now().Add(time.Duration(secs) * time.Second).Unix()
		}
	}
	return tok, nil
}

func (a *Auth) setServiceHeaders(req *http.Request) {
	req.Header.Set("apikey", a.supabaseService)
	req.Header.Set("Authorization", "Bearer "+a.supabaseService)
	req.Header.Set("Content-Type", "application/json")
}

// supErr extracts a readable message from a GoTrue error response.
func supErr(code int, raw []byte) error {
	var e struct {
		ErrorDescription string `json:"error_description"`
		Message          string `json:"message"`
		Error            string `json:"error"`
		Msg              string `json:"msg"`
	}
	_ = json.Unmarshal(raw, &e)
	msg := firstNonEmpty(e.ErrorDescription, e.Msg, e.Message, e.Error)
	if msg == "" {
		msg = strings.TrimSpace(string(raw))
	}
	if msg == "" {
		msg = "unknown error"
	}
	return fmt.Errorf("%s (HTTP %d)", msg, code)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// validSupabaseToken validates a session token with Supabase Auth. Verified
// tokens are cached until the JWT exp (capped at supTTL) so per-request API
// calls don't hammer the Supabase edge.
func (a *Auth) validSupabaseToken(token string) bool {
	h := sha256.Sum256([]byte(token))
	a.mu.Lock()
	if e, ok := a.supCache[h]; ok {
		a.mu.Unlock()
		return time.Now().Before(e.exp)
	}
	a.mu.Unlock()

	req, err := http.NewRequest(http.MethodGet, a.supabaseURL+"/auth/v1/user", nil)
	if err != nil {
		return false
	}
	req.Header.Set("apikey", a.supabaseAnon)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := a.supClient.Do(req)
	if err != nil {
		return false
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)
	if res.StatusCode != http.StatusOK {
		return false
	}

	ttl := supTTL
	if exp := jwtExp(token); exp > 0 {
		if until := time.Until(time.Unix(exp, 0)); until < time.Minute {
			return false
		} else if until < ttl {
			ttl = until
		}
	}
	a.mu.Lock()
	a.supCache[h] = supEntry{exp: time.Now().Add(ttl)}
	a.mu.Unlock()
	return true
}

// jwtExp reads the exp claim from a JWT's payload segment (best effort).
func jwtExp(token string) int64 {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return 0
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return 0
	}
	var c struct {
		Exp int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &c) != nil {
		return 0
	}
	return c.Exp
}

// IsPeerSecret reports whether the presented string equals the peer secret.
func (a *Auth) IsPeerSecret(presented string) bool {
	return validBearer(presented, a.peerHash)
}

// Allow checks the per-key+IP rate limit.
func (a *Auth) Allow(key string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	b, ok := a.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(a.perMinute), last: now}
		a.buckets[key] = b
	}
	// refill over elapsed time, capped at bucket size
	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min(float64(a.perMinute), b.tokens+elapsed*(float64(a.perMinute)/60.0))
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	// opportunistic prune of stale entries
	if len(a.buckets) > 10_000 {
		for k, bb := range a.buckets {
			if now.Sub(bb.last) > 10*time.Minute {
				delete(a.buckets, k)
			}
		}
	}
	return true
}

// Payload for presigned URL tokens.
type sigClaims struct {
	Ver     int    `json:"v"`
	Method  string `json:"m"` // "PUT" | "GET" | "DELETE"
	Bucket  string `json:"b"`
	Key     string `json:"k"`
	Expires int64  `json:"e"` // unix seconds
}

// validateSignedURL verifies sig for method/bucket/key against expires.
func (a *Auth) validateSignedURL(method, bucket, key, sig, expires string, now time.Time) error {
	var exp int64
	if expires != "" {
		n, err := strconv.ParseInt(expires, 10, 64)
		if err != nil {
			return fmt.Errorf("bad expires")
		}
		exp = n
	}
	claims := &sigClaims{
		Ver:    1,
		Method: method,
		Bucket: bucket,
		Key:    key,
		Expires: exp,
	}
	expected := a.mac(claims)
	if !hmac.Equal([]byte(expected), []byte(sig)) {
		return fmt.Errorf("invalid signature")
	}
	if exp > 0 {
		if now.Unix() > exp+60 {
			return fmt.Errorf("signature expired (allow +60s clock skew)")
		}
	}
	return nil
}

func (a *Auth) mac(c *sigClaims) string {
	raw, _ := json.Marshal(c)
	m := hmac.New(sha256.New, a.signKey[:])
	m.Write(raw)
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// SignURL builds a presigned URL string for method/bucket/key/ttl.
// base is the external base (e.g. https://storage.domain) without trailing slash.
func (a *Auth) SignURL(method, bucket, key string, ttl time.Duration, base string) string {
	if base != "" {
		base = strings.TrimRight(base, "/")
	}
	exp := time.Now().Add(ttl).Unix()
	c := &sigClaims{Ver: 1, Method: method, Bucket: bucket, Key: key, Expires: exp}
	sig := a.mac(c)
	q := fmt.Sprintf("sig=%s&expires=%d", sig, exp)
	return fmt.Sprintf("%s/v1/buckets/%s/objects/%s?%s", base, url.PathEscape(bucket), encodeKeyPath(key), q)
}

// encodeKeyPath percent-encodes each key segment so keys containing spaces or
// other reserved characters produce a valid URL while keeping '/' separators.
func encodeKeyPath(key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

// VerifySigned parses the required query params and returns whether they are
// valid for the request. If the request carries query params, they must be
// valid (no fallback to bearer for the same route).
func (a *Auth) VerifySigned(r *http.Request, method, bucket, key string) (bool, error) {
	sig := r.URL.Query().Get("sig")
	exp := r.URL.Query().Get("expires")
	if sig == "" {
		return false, nil // not a signed request
	}
	if err := a.validateSignedURL(method, bucket, key, sig, exp, time.Now()); err != nil {
		return false, err
	}
	return true, nil
}

// RequestScope computes the effective scope for a request, honoring
// presigned URLs for object routes.
func (a *Auth) RequestScope(r *http.Request, bucket, key string) (Scope, error) {
	if bucket != "" && key != "" {
		ok, err := a.VerifySigned(r, r.Method, bucket, key)
		if err != nil {
			return ScopeNone, err
		}
		if ok {
			return ScopeRead, nil
		}
	}
	bearer := BearerToken(r)
	return a.Authenticate(bearer), nil
}

// BearerToken extracts the bare token from an Authorization header.
func BearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "Bearer ") {
		return h[7:]
	}
	return ""
}

// ClientScope returns the scope of just the bearer credential (no signing).
func (a *Auth) ClientScope(r *http.Request) Scope {
	return a.Authenticate(BearerToken(r))
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}