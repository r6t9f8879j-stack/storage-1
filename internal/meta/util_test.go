package meta

import (
	"errors"
	"strings"
	"testing"
)

func TestValidKey(t *testing.T) {
	valid := []string{
		"a.png",
		"photos/2026/img.jpg",
		"Screenshot 2026-07-11 130201.png",
		"folder/Screenshot 2026-07-11 130201.png",
		"trailing .png",
		"file (1).png",
		"a+b,c@d.png",
		"a'b!d#e$f&g.png",
		"a?b=c.png",
		"a[1].png",
		strings.Repeat("a", 1024),
	}
	for _, key := range valid {
		if err := ValidKey(key); err != nil {
			t.Errorf("ValidKey(%q) = %v, want nil", key, err)
		}
	}

	invalid := []string{
		"",
		"/leading.png",
		"trailing/",
		" leading.png",
		"trailing ",
		"a..b",
		"a\\b.png",
		"a:b.png",
		"a\nb.png",
		"a\tb.png",
		"résumé.pdf",
		strings.Repeat("a", 1025),
	}
	for _, key := range invalid {
		if err := ValidKey(key); err == nil {
			t.Errorf("ValidKey(%q) = nil, want error", key)
		} else if !errors.Is(err, ErrInvalid) {
			t.Errorf("ValidKey(%q) = %v, want ErrInvalid", key, err)
		}
	}
}

func TestIsValidKey(t *testing.T) {
	if !IsValidKey("Screenshot 2026-07-11 130201.png") {
		t.Error("IsValidKey rejected a filename with spaces")
	}
	if IsValidKey("a\\b") {
		t.Error("IsValidKey accepted a backslash")
	}
}
