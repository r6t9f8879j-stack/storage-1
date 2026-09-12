package transfer

import (
	"testing"

	"github.com/anacrolix/torrent"
)

func TestSanitizeName(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"hello.txt", "hello.txt"},
		{"My File (1).mov", "My File _1_.mov"},
		{"  padded name  ", "padded name"},
		{"é.jpg", "jpg"},
		{"a/b/c.txt", "a/b/c.txt"},
		{"C:\\dir\\file.bin", "C__dir_file.bin"},
		{"../evil", "evil"},
		{"..", ""},
		{"/", ""},
		{"<script>alert(1)</script>.png", "script_alert_1__/script_.png"},
	}
	for _, c := range cases {
		if got := sanitizeName(c.in); got != c.want {
			t.Errorf("sanitizeName(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestAddFallbackTrackers(t *testing.T) {
	fallback := []string{"udp://tracker.example:1337/announce"}

	// a magnet with no trackers of its own gets the configured fallbacks
	spec, err := torrent.TorrentSpecFromMagnetUri("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("parse magnet: %v", err)
	}
	addFallbackTrackers(spec, fallback)
	if got := trackerCount(spec); got != len(fallback) {
		t.Errorf("tracker count = %d, want %d", got, len(fallback))
	}

	// a magnet that already has trackers is left alone
	spec, err = torrent.TorrentSpecFromMagnetUri("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&tr=udp%3A%2F%2Fown.example%3A80")
	if err != nil {
		t.Fatalf("parse magnet: %v", err)
	}
	addFallbackTrackers(spec, fallback)
	if got := trackerCount(spec); got != 1 {
		t.Errorf("tracker count = %d, want the magnet's own 1", got)
	}

	// disabling (empty list) is a no-op
	spec, err = torrent.TorrentSpecFromMagnetUri("magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567")
	if err != nil {
		t.Fatalf("parse magnet: %v", err)
	}
	addFallbackTrackers(spec, nil)
	if got := trackerCount(spec); got != 0 {
		t.Errorf("tracker count = %d, want 0", got)
	}
}

func TestIsPadFile(t *testing.T) {
	cases := []struct {
		name, attr string
		want       bool
	}{
		{"movie.mkv", "", false},
		{"movie.mkv", "p", true},
		{"audio.flac", "l", false},
		{"0.pad", "", true},
		{"____padding_file_0_16_.pad", "", true},
		{"____padding_file_0_16.txt", "", true},
		{"sub/____padding_file_3_4096_.pad", "", true},
		{"subtitles/en.srt", "", false},
	}
	for _, c := range cases {
		if got := isPadFile(c.name, c.attr); got != c.want {
			t.Errorf("isPadFile(%q,%q) = %v, want %v", c.name, c.attr, got, c.want)
		}
	}
}

func TestFileContentType(t *testing.T) {
	cases := []struct {
		name, override, want string
	}{
		{"report.txt", "", "text/plain; charset=utf-8"},
		{"photo.jpg", "", "image/jpeg"},
		{"clip.mp4", "", "video/mp4"},
		{"photo.jpg", "application/x-bittorrent", "image/jpeg"},
		{"file", "", "application/octet-stream"},
		{"odd.bin", "", "application/octet-stream"},
		{"custom.stream", "application/x-custom", "application/x-custom"},
	}
	for _, c := range cases {
		if got := fileContentType(c.name, c.override); got != c.want {
			t.Errorf("fileContentType(%q,%q) = %q, want %q", c.name, c.override, got, c.want)
		}
	}
}
