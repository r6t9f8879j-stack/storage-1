package transfer

import "testing"

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
