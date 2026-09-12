package api

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// httpdate parses an HTTP-date header (RFC 7231 formats).
func httpdate(v string) (time.Time, error) {
	return http.ParseTime(v)
}

// etagMatches implements RFC 7232 entity-tag comparison (strong comparison).
func etagMatches(header, objETag string) bool {
	header = strings.TrimSpace(header)
	if header == "" {
		return false
	}
	if strings.HasPrefix(header, "W/") {
		return header[2:] == objETag || ("W/"+header[2:] == objETag)
	}
	for _, tag := range strings.Split(header, ",") {
		tag = strings.TrimSpace(tag)
		if tag == "*" || tag == objETag {
			return true
		}
	}
	return false
}

// parseRange parses a single Range header value for a resource of size.
// Returns (start, length) and true if satisfiable. Suffix ranges and open
// ranges are supported.
func parseRange(header string, size int64) (start, length int64, ok bool) {
	if header == "" || !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(header, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, 0, false // multi-range unsupported
	}
	if size < 0 {
		return 0, 0, false
	}
	if strings.HasPrefix(spec, "-") { // suffix
		n, err := strconv.ParseInt(strings.TrimPrefix(spec, "-"), 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, false
		}
		if n > size {
			n = size
		}
		return size - n, n, true
	}
	parts := strings.SplitN(spec, "-", 2)
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	var end int64 = size - 1
	if len(parts) == 2 && parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false
		}
	}
	if start > end || start >= size {
		return 0, 0, false // unsatisfiable
	}
	if end >= size {
		end = size - 1
	}
	return start, end - start + 1, true
}

func bytesRangeHeader(start, length, size int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", start, start+length-1, size)
}