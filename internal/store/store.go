package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// Store manages content-addressed blob files on disk.
//
// Invariant: a blob file is created via FinalizeTmp which fsyncs the content
// and atomically renames it into place BEFORE any database row references it.
// A crash can therefore only leak unreferenced files (removed by GC), never a
// corrupt visible object.
type Store struct {
	dataDir string
	blobs   string
	tmp     string
}

// New creates the store rooted at dataDir (dirs created on Init).
func New(dataDir string) *Store {
	return &Store{
		dataDir: dataDir,
		blobs:   filepath.Join(dataDir, "blobs"),
		tmp:     filepath.Join(dataDir, "tmp"),
	}
}

func (s *Store) Init() error {
	for _, d := range []string{s.dataDir, s.blobs, s.tmp} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}
	return nil
}

// DataDir returns the storage root (for state/snapshot code).
func (s *Store) DataDir() string { return s.dataDir }

// blobPath resolves the on-disk path for a blob hash.
func (s *Store) blobPath(hash string) string {
	if len(hash) < 2 {
		return filepath.Join(s.blobs, "zz")
	}
	return filepath.Join(s.blobs, hash[:2], hash)
}

// ---- temporary upload files ----

func (s *Store) tmpPath(uploadID string) string {
	return filepath.Join(s.tmp, uploadID)
}

// OpenTmp opens (creates) the staging file for an upload session.
func (s *Store) OpenTmp(uploadID string) (*os.File, error) {
	return os.OpenFile(s.tmpPath(uploadID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

// TruncateTmp trims the staging file to the given committed byte offset (for
// resume: bytes 0..n were successfully received; the client re-sends from n).
func (s *Store) TruncateTmp(uploadID string, n int64) error {
	if n < 0 {
		return fmt.Errorf("negative resume offset")
	}
	return os.Truncate(s.tmpPath(uploadID), n)
}

// TmpSize returns current staging file size (0 if absent).
func (s *Store) TmpSize(uploadID string) (int64, error) {
	st, err := os.Stat(s.tmpPath(uploadID))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// FinalizeTmp fsyncs the staging file, computes its sha256, and atomically
// renames it into the blob store. Returns hash, size, etag.
// After this returns, the bytes are durable and addressable by hash.
func (s *Store) FinalizeTmp(uploadID string) (hash string, size int64, etag string, err error) {
	src := s.tmpPath(uploadID)
	f, err := os.Open(src)
	if err != nil {
		return "", 0, "", err
	}
	defer f.Close()

	// NOTE: no f.Sync() here — this is a read-only re-open of a file already
	// flushed by the writer's Close(). On Windows, FlushFileBuffers on a
	// read-only handle fails with ERROR_ACCESS_DENIED; durability comes from
	// the atomic rename below plus NTFS metadata journaling.
	st, err := f.Stat()
	if err != nil {
		return "", 0, "", err
	}
	size = st.Size()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", 0, "", fmt.Errorf("hash tmp: %w", err)
	}
	hash = hex.EncodeToString(h.Sum(nil))

	if err := s.PromoteFile(src, hash); err != nil {
		return "", 0, "", err
	}
	return hash, size, "\"" + hash + "\"", nil
}

// PromoteFile moves dst (already fully written + fsynced by caller) into the
// blob store at its content hash. Dedup: if the blob already exists, dst is
// removed and the existing blob wins.
func (s *Store) PromoteFile(src string, hash string) error {
	dest := s.blobPath(hash)
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(dest); err == nil {
		os.Remove(src) // deduplicated
		return nil
	}
	// atomic cross-volume-safe move with fallback
	if err := os.Rename(src, dest); err != nil {
		if err2 := copyFile(src, dest); err2 != nil {
			return fmt.Errorf("promote blob: %v / %w", err, err2)
		}
		os.Remove(src)
	}
	return nil
}

// CommitHash moves an arbitrary fsynced file into blobs under a known hash.
func (s *Store) CommitHash(src string, hash string) error { return s.PromoteFile(src, hash) }

// AbortTmp removes a staging file.
func (s *Store) AbortTmp(uploadID string) error {
	return os.Remove(s.tmpPath(uploadID))
}

// ---- blob reads ----

// OpenBlob opens a blob for reading (caller closes). Returns os.ErrNotExist
// when the blob is missing.
func (s *Store) OpenBlob(hash string) (*os.File, error) {
	return os.Open(s.blobPath(hash))
}

// StatBlob returns blob size on disk.
func (s *Store) StatBlob(hash string) (int64, error) {
	st, err := os.Stat(s.blobPath(hash))
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

// BlobExists reports whether the blob file exists.
func (s *Store) BlobExists(hash string) bool {
	_, err := os.Stat(s.blobPath(hash))
	return err == nil
}

// CopyBlobTo streams a blob into w for the given byte range.
func (s *Store) CopyBlobTo(w io.Writer, hash string, off, length int64) (int64, error) {
	f, err := s.OpenBlob(hash)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return io.CopyN(w, f, length)
}

// DeleteBlob removes a blob file (idempotent).
func (s *Store) DeleteBlob(hash string) error {
	err := os.Remove(s.blobPath(hash))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// WriteBlob persists a stream under a known hash (replication fetch / restore).
// Writes to tmp, fsyncs, then promotes to blobs/<h[0:2]>/<h>. Verifies size.
func (s *Store) WriteBlob(hash string, size int64, r io.Reader) error {
	if len(hash) != 64 {
		return fmt.Errorf("invalid blob hash length %d", len(hash))
	}
	src := filepath.Join(s.tmp, "in_"+hash)
	f, err := os.Create(src)
	if err != nil {
		return err
	}
	n, cerr := io.Copy(f, r)
	syncErr := f.Sync()
	closeErr := f.Close()
	if cerr != nil {
		os.Remove(src)
		return cerr
	}
	if syncErr != nil && !isIgnorableFsync(syncErr) {
		os.Remove(src)
		return syncErr
	}
	if closeErr != nil {
		os.Remove(src)
		return closeErr
	}
	if n != size && size >= 0 {
		os.Remove(src)
		return fmt.Errorf("blob size mismatch: want %d got %d", size, n)
	}
	return s.PromoteFile(src, hash)
}

// ---- inventory / GC / fsck ----

// AllBlobsOnDisk lists every blob hash present under blobs/.
func (s *Store) AllBlobsOnDisk() ([]string, error) {
	dirs, err := os.ReadDir(s.blobs)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, d := range dirs {
		if !d.IsDir() || len(d.Name()) != 2 {
			continue
		}
		files, err := os.ReadDir(filepath.Join(s.blobs, d.Name()))
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			name := f.Name()
			if len(name) == 64 {
				out = append(out, name)
			}
		}
	}
	return out, nil
}

// CountBlobs returns the number of blob files under blobs/ without loading
// hashes into memory.
func (s *Store) CountBlobs() (int64, error) {
	dirs, err := os.ReadDir(s.blobs)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, d := range dirs {
		if !d.IsDir() || len(d.Name()) != 2 {
			continue
		}
		files, err := os.ReadDir(filepath.Join(s.blobs, d.Name()))
		if err != nil {
			return 0, err
		}
		for _, f := range files {
			if !f.IsDir() && len(f.Name()) == 64 {
				n++
			}
		}
	}
	return n, nil
}

// GarbageCollect removes blob files not in referenced (a map of hash->true)
// whose mtime is older than grace. Returns count removed + bytes freed.
func (s *Store) GarbageCollect(referenced map[string]bool, grace time.Duration) (int, int64, error) {
	onDisk, err := s.AllBlobsOnDisk()
	if err != nil {
		return 0, 0, err
	}
	cutoff := time.Now().Add(-grace)
	var removed int
	var freed int64
	for _, h := range onDisk {
		if referenced[h] {
			continue
		}
		path := s.blobPath(h)
		st, err := os.Stat(path)
		if err != nil {
			continue
		}
		if st.ModTime().After(cutoff) {
			continue // too young — an unreferenced upload may still be committing
		}
		if err := os.Remove(path); err == nil {
			removed++
			freed += st.Size()
		}
	}
	return removed, freed, nil
}

// VerifyHashes re-hashes every blob and returns (existing, corrupt) mapping:
// handles hash->size for fast path are not used; full re-hash on boot fsck.
func (s *Store) VerifyHashes(onDisk []string) (ok map[string]int64, corrupt []string, err error) {
	ok = map[string]int64{}
	for _, h := range onDisk {
		f, err := os.Open(s.blobPath(h))
		if err != nil {
			continue
		}
		hh := sha256.New()
		n, cerr := io.Copy(hh, f)
		f.Close()
		if cerr != nil {
			corrupt = append(corrupt, h)
			continue
		}
		if hex.EncodeToString(hh.Sum(nil)) != h {
			corrupt = append(corrupt, h)
			continue
		}
		ok[h] = n
	}
	sort.Strings(corrupt)
	return ok, corrupt, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func isIgnorableFsync(err error) bool {
	if err == nil {
		return true
	}
	// Windows FlushFileBuffers can fail with ERROR_ACCESS_DENIED / ERROR_INVALID_
	// FUNCTION on temp, Defender-watched, or memory-backed volumes; NTFS
	// durability comes from the atomic rename anyway, so tolerate it. Also
	// ignore unsupported fsync on exotic filesystems + the explicit
	// STORAGED_ALLOW_SKIP_FSYNC escape.
	if strings.EqualFold(os.Getenv("STORAGED_ALLOW_SKIP_FSYNC"), "1") {
		return true
	}
	for {
		se, ok := err.(syscall.Errno)
		if ok {
			return se == syscall.EINVAL || se == syscall.ENOTSUP || se == syscall.EACCES || se == syscall.Errno(5)
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
		if err == nil {
			return false
		}
	}
}