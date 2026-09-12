package transfer

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"

	"storaged/internal/meta"
)

// torrentClient returns the shared BitTorrent client, creating it on first
// use. Downloaded piece data is staged under the store's data dir and cleaned
// up after each torrent commits.
func (m *Manager) torrentClient() (*torrent.Client, error) {
	m.tclientMu.Lock()
	defer m.tclientMu.Unlock()
	if m.tclient != nil {
		return m.tclient, nil
	}
	m.tdir = filepath.Join(m.sto.DataDir(), "torrents")
	if err := os.MkdirAll(m.tdir, 0o755); err != nil {
		return nil, err
	}
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = m.tdir
	cfg.ListenPort = 0 // random ephemeral port; single node
	cfg.Seed = false
	cfg.NoUpload = true
	cfg.NoDHT = false
	cl, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("torrent client: %w", err)
	}
	m.tclient = cl
	return cl, nil
}

// closeTorrentClient shuts the shared client down and removes leftover stages.
func (m *Manager) closeTorrentClient() {
	m.tclientMu.Lock()
	cl := m.tclient
	m.tclient = nil
	tdir := m.tdir
	m.tdir = ""
	m.tclientMu.Unlock()
	if cl != nil {
		_ = cl.Close()
	}
	if tdir != "" {
		_ = os.RemoveAll(tdir)
	}
}

// downloadTorrent downloads a magnet/.torrent and commits each contained file
// as an object. The transfer's Key is the base key (or the torrent name when
// empty); multi-file torrents become base/<relative path> objects.
func (m *Manager) downloadTorrent(ctx context.Context, t meta.Transfer) error {
	cl, err := m.torrentClient()
	if err != nil {
		return err
	}

	var tt *torrent.Torrent
	var spec *torrent.TorrentSpec
	switch {
	case t.TorrentData != "":
		raw, err := base64.StdEncoding.DecodeString(t.TorrentData)
		if err != nil {
			return fmt.Errorf("bad .torrent payload: %w", err)
		}
		mi, err := metainfo.Load(bytes.NewReader(raw))
		if err != nil {
			return fmt.Errorf("bad .torrent metainfo: %w", err)
		}
		spec, err = torrent.TorrentSpecFromMetaInfoErr(mi)
		if err != nil {
			return err
		}
	case strings.HasPrefix(t.URL, "magnet:"):
		spec, err = torrent.TorrentSpecFromMagnetUri(t.URL)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("torrent transfer has neither magnet nor metainfo")
	}
	tt, fresh, err := cl.AddTorrentSpec(spec)
	if err != nil {
		return err
	}
	if !fresh {
		// Dropping releases only our reference; the active transfer owns it.
		tt.Drop()
		tt = nil
		return fmt.Errorf("this torrent is already being downloaded by another transfer; cancel that one first")
	}

	var info *metainfo.Info
	defer func() {
		if tt != nil {
			tt.Drop()
		}
		if info != nil && m.tdir != "" {
			_ = os.RemoveAll(filepath.Join(m.tdir, info.Name))
		}
	}()

	// wait for metadata to arrive (magnet announces / tracker / DHT)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tt.GotInfo():
	}
	info = tt.Info()
	tt.DownloadAll()

	total := tt.Length()
	files := tt.Files()
	if len(files) == 0 {
		return fmt.Errorf("torrent contains no files")
	}

	// Phase 1: download all pieces into the staging dir, reporting progress.
	{
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if bc := tt.BytesCompleted(); bc > 0 && bc < total {
				_ = m.meta.UpdateTransferProgress(t.ID, bc, total)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tt.Complete().On():
				goto downloaded
			case <-ticker.C:
			}
		}
	}
downloaded:

	base := t.Key
	if base == "" {
		base = sanitizeName(info.BestName())
	}
	if !meta.IsValidKey(base) {
		base = "torrent"
	}
	multi := len(files) > 1
	tracker := &aggregateTracker{m: m, id: t.ID, total: total}

	var committed int
	for i, f := range files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if multi && (isPadFile(f.DisplayPath(), f.FileInfo().Attr) || f.Length() == 0) {
			m.log.Printf("transfer %s: skipping padding file %q", t.ID, f.DisplayPath())
			continue
		}
		key := base
		if multi {
			segs := strings.FieldsFunc(filepath.ToSlash(f.DisplayPath()), func(r rune) bool { return r == '/' })
			var clean []string
			for _, s := range segs {
				s = sanitizeName(s)
				if s != "" && s != "." && s != ".." {
					clean = append(clean, s)
				}
			}
			if len(clean) == 0 {
				continue
			}
			key = path.Join(append([]string{base}, clean...)...)
		}
		if !meta.IsValidKey(key) {
			m.log.Printf("transfer %s: skipping file with invalid key %q", t.ID, key)
			continue
		}

		tmpID := fmt.Sprintf("%s-%02d", t.ID, i)
		_ = m.sto.AbortTmp(tmpID)
		tmp, err := m.sto.OpenTmp(tmpID)
		if err != nil {
			return err
		}
		rd := f.NewReader()
		rd.SetContext(ctx)
		pr := &progressReader{r: rd, cb: func(n, _ int64) {
			tracker.add(n)
		}}
		// CopyN bounds reads to the file's exact length: anacrolix's torrent
		// reader over-reads across piece boundaries into later files, so a
		// plain io.Copy would capture the next file's leading bytes.
		_, copyErr := io.CopyN(tmp, pr, f.Length())
		closeErr := rd.Close()
		closeErr2 := tmp.Close()
		if copyErr != nil {
			_ = m.sto.AbortTmp(tmpID)
			return copyErr
		}
		if closeErr != nil {
			_ = m.sto.AbortTmp(tmpID)
			return closeErr
		}
		if closeErr2 != nil {
			_ = m.sto.AbortTmp(tmpID)
			return closeErr2
		}

		// re-check the lease before committing each file
		if !m.canWrite() {
			_ = m.sto.AbortTmp(tmpID)
			return fmt.Errorf("node is no longer the writer; transfer not committed")
		}
		hash, size, _, err := m.sto.FinalizeTmp(tmpID)
		if err != nil {
			return err
		}
		obj := &meta.Object{
			Bucket:      t.Bucket,
			Key:         key,
			BlobHash:    hash,
			Size:        size,
			ContentType: fileContentType(f.DisplayPath(), t.ContentType),
			ETag:        `"` + hash + `"`,
			Metadata:    map[string]string{},
		}
		if err := m.meta.PutObject(obj); err != nil {
			return fmt.Errorf("commit %s: %w", key, err)
		}
		committed++
		m.log.Printf("transfer %s committed %s/%s (%s)", t.ID, t.Bucket, key, byteSize(size))
	}
	if committed == 0 {
		return fmt.Errorf("no torrent files could be committed")
	}
	_ = m.meta.SetTransferStatus(t.ID, StatusDone, "")
	m.log.Printf("transfer %s torrent done: %d file(s) -> %s/%s", t.ID, committed, t.Bucket, base)
	return nil
}

// aggregateTracker reports aggregate commit progress (~1 Hz).
type aggregateTracker struct {
	m      *Manager
	id     string
	total  int64
	done   int64
	lastTS time.Time
}

func (a *aggregateTracker) add(n int64) {
	a.done += n
	if time.Since(a.lastTS) >= time.Second {
		a.lastTS = time.Now()
		_ = a.m.meta.UpdateTransferProgress(a.id, a.done, a.total)
	}
}

// sanitizeName turns an untrusted torrent file/dir name into key-safe segments.
func sanitizeName(s string) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_' || r == '.' || r == '/' || r == ' ':
			return r
		}
		return '_'
	}, s)
	return strings.Trim(s, " /._")
}

// isPadFile reports whether a multi-file torrent entry is a BEP 47 padding
// file (attr "p") or matches the common release-group `*.pad` / pre-1.x
// `____padding_file_*` naming conventions. Padding files carry no real data
// and should never become objects.
func isPadFile(name, attr string) bool {
	if strings.Contains(strings.ToLower(attr), "p") {
		return true
	}
	n := strings.ToLower(filepath.Base(name))
	return strings.HasSuffix(n, ".pad") || strings.HasPrefix(n, "____padding_file_")
}

// fileContentType guesses a content type from a file extension, falling back
// to an explicit override (unless it's the neutral x-bittorrent default).
func fileContentType(name, override string) string {
	if override != "" && override != "application/x-bittorrent" {
		return override
	}
	ext := strings.ToLower(filepath.Ext(name))
	if ct := mime.TypeByExtension(ext); ct != "" {
		return ct
	}
	return "application/octet-stream"
}
