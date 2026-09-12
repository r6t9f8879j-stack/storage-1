package meta

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrInvalid marks user-input validation failures (bucket or key names).
var ErrInvalid = errors.New("invalid input")

// ErrInvalidF wraps ErrInvalid with a formatted message.
func ErrInvalidF(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

var (
	bucketRe = regexp.MustCompile(`^[a-z0-9][a-z0-9\-._]{1,62}$`)
	keyRe    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9\-._/]{0,1000}$`)
)

// validBucket validates an S3-like bucket name (lowercase letters, digits,
// hyphens, dots, underscores; 3..63 chars).
func validBucket(name string) error {
	if len(name) < 3 || len(name) > 63 {
		return fmt.Errorf("%w: bucket name must be 3-63 characters", ErrInvalid)
	}
	if !bucketRe.MatchString(name) {
		return fmt.Errorf("%w: invalid bucket name %q: use a-z0-9, '-', '.', '_'", ErrInvalid, name)
	}
	return nil
}

// validKey validates an object key: printable URI-safe characters, slashes as
// separators, no leading/trailing slash, no dot-segments, length <= 1024.
func validKey(key string) error {
	if len(key) == 0 || len(key) > 1024 {
		return fmt.Errorf("%w: object key must be 1-1024 characters", ErrInvalid)
	}
	if strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") {
		return fmt.Errorf("%w: object key must not start or end with '/'", ErrInvalid)
	}
	if strings.Contains(key, "..") || strings.ContainsAny(key, "\x00\r\n\t\\:") {
		return fmt.Errorf("%w: object key contains invalid characters", ErrInvalid)
	}
	if !keyRe.MatchString(key) {
		return fmt.Errorf("%w: invalid object key %q", ErrInvalid, key)
	}
	return nil
}

// newID returns a random URL-safe id for upload sessions.
func newID(prefix string) string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}

// ValidKey exposes key validation for callers outside this package (store/api).
func ValidKey(key string) error { return validKey(key) }

// ValidBucket exposes bucket validation.
func ValidBucket(name string) error { return validBucket(name) }

// IsValidKey reports validity without an error allocation.
func IsValidKey(key string) bool     { return validKey(key) == nil }
func IsValidBucket(name string) bool { return validBucket(name) == nil }
