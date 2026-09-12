package meta

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
)

func newTestMeta(t *testing.T) *Meta {
	t.Helper()
	m, err := New(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func seedKeys(t *testing.T, m *Meta, bucket string, keys []string) {
	t.Helper()
	if err := m.PutBucket(bucket, false); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	for _, k := range keys {
		obj := &Object{Bucket: bucket, Key: k, BlobHash: "h" + k, Size: int64(len(k)),
			ContentType: "text/plain", ETag: `"` + k + `"`, Metadata: map[string]string{}}
		if err := m.PutObject(obj); err != nil {
			t.Fatalf("PutObject(%s): %v", k, err)
		}
	}
}

func allPages(t *testing.T, m *Meta, bucket, prefix, delim string, limit int) ([]string, []string) {
	t.Helper()
	var files, folders []string
	after := ""
	for {
		f, d, next, err := m.ListFolder(context.Background(), bucket, prefix, delim, limit, after)
		if err != nil {
			t.Fatalf("ListFolder(page): %v", err)
		}
		for _, e := range f {
			files = append(files, e.Key)
		}
		if len(d) == 0 {
			d = []string{}
		}
		folders = append(folders, d...)
		if next == "" {
			break
		}
		if next == after {
			t.Fatalf("cursor did not advance: %q", next)
		}
		after = next
	}
	return files, folders
}

func TestListFolderBasics(t *testing.T) {
	m := newTestMeta(t)
	keys := []string{"a.txt", "b/x.txt", "b/y.txt", "b/z/w.txt", "c.txt", "d/e/f.txt"}
	seedKeys(t, m, "smoke", keys)

	files, folders := allPages(t, m, "smoke", "", "/", 100)
	wantF := []string{"a.txt", "c.txt"}
	wantD := []string{"b/", "d/"}
	if !equalStrings(files, wantF) {
		t.Errorf("files = %v, want %v", files, wantF)
	}
	if !equalStrings(folders, wantD) {
		t.Errorf("folders = %v, want %v", folders, wantD)
	}
}

func TestListFolderPagination(t *testing.T) {
	m := newTestMeta(t)
	keys := []string{"a.txt", "b/x.txt", "b/y.txt", "c.txt", "d/e/f.txt", "e.txt"}
	seedKeys(t, m, "bucket-p", keys)

	all, folders := allPages(t, m, "bucket-p", "", "/", 2)
	all = append(all, folders...)
	want := []string{"a.txt", "b/", "c.txt", "d/", "e.txt"}
	if !equalStrings(all, want) {
		t.Fatalf("paginated result = %v, want %v", all, want)
	}
}

func TestListFolderNestedPrefix(t *testing.T) {
	m := newTestMeta(t)
	keys := []string{"b/z/w.txt", "b/z/x/y.txt", "b/a.txt"}
	seedKeys(t, m, "bucket-n", keys)

	files, folders := allPages(t, m, "bucket-n", "b/", "/", 10)
	if !equalStrings(files, []string{"b/a.txt"}) {
		t.Errorf("files = %v, want [b/a.txt]", files)
	}
	if !equalStrings(folders, []string{"b/z/"}) {
		t.Errorf("folders = %v, want [b/z/]", folders)
	}

	// drill into z/
	files2, folders2 := allPages(t, m, "bucket-n", "b/z/", "/", 10)
	if !equalStrings(files2, []string{"b/z/w.txt"}) {
		t.Errorf("files2 = %v, want [b/z/w.txt]", files2)
	}
	if !equalStrings(folders2, []string{"b/z/x/"}) {
		t.Errorf("folders2 = %v, want [b/z/x/]", folders2)
	}
}

func TestListFolderNoDelimiter(t *testing.T) {
	m := newTestMeta(t)
	keys := []string{"a.txt", "b/x.txt"}
	seedKeys(t, m, "flat", keys)

	files, folders := allPages(t, m, "flat", "", "", 10)
	if !equalStrings(files, keys) {
		t.Errorf("files = %v, want %v", files, keys)
	}
	if len(folders) != 0 {
		t.Errorf("folders = %v, want none", folders)
	}
}

func TestListFolderTrailingGroupStraddlesPage(t *testing.T) {
	m := newTestMeta(t)
	// a lot of keys in one folder, then another folder after
	keys := []string{"dir/z1.txt", "dir/z2.txt", "dir/z3.txt", "dir/z4.txt", "next/q.txt"}
	seedKeys(t, m, "bucket-s", keys)

	files, folders := allPages(t, m, "bucket-s", "", "/", 3)
	if !equalStrings(folders, []string{"dir/", "next/"}) {
		t.Fatalf("folders = %v, want [dir/ next/]", folders)
	}
	if len(files) != 0 {
		t.Fatalf("files = %v, want none", files)
	}

	// small limit straddles a folder mid-way: still yields each folder once
	files2, folders2 := allPages(t, m, "bucket-s", "", "/", 2)
	// dir/ fits on page 1 (2 keys) — page 2 must continue with dir/ suppressed
	// and yield next/ without re-emitting dir/.
	if !equalStrings(folders2, []string{"dir/", "next/"}) {
		t.Fatalf("folders2 = %v, want [dir/ next/]", folders2)
	}
	_ = files2
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string{}, a...)
	bb := append([]string{}, b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}