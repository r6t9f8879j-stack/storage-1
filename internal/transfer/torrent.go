package transfer

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
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

// Torrent transfers are bounded in time so a dead swarm can never occupy a
// worker slot (and confuse the dashboard) forever: without these a magnet with
// no reachable peers sits at "downloading 0%" until the 15-minute stuck-requeue
// silently restarts it.
var (
	// TorrentMetadataTimeout bounds the BEP 9 metadata fetch for magnets.
	TorrentMetadataTimeout = 3 * time.Minute
	// TorrentStallTimeout bounds how long a torrent may receive no new piece
	// data before the transfer is failed with a readable reason.
	TorrentStallTimeout = 5 * time.Minute
)

// torrentStageDir returns the directory anacrolix stages torrent data in.
func (m *Manager) torrentStageDir() string {
	m.tclientMu.Lock()
	defer m.tclientMu.Unlock()
	return m.tdir
}

// retryableErr marks failures where re-queuing the transfer can make progress:
// a dead swarm, an unreachable webseed, a timeout, or a cancel. The staged
// pieces are kept for these, so a retry resumes from where it stopped.
type retryableErr struct{ err error }

func (e retryableErr) Error() string { return e.err.Error() }
func (e retryableErr) Unwrap() error { return e.err }

// retryableF builds a retryable failure with a formatted message.
func retryableF(format string, a ...any) error {
	return retryableErr{fmt.Errorf(format, a...)}
}

// isRetryable reports whether a failed transfer should keep its staged data so
// that a retry resumes instead of starting from zero.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	var r retryableErr
	if errors.As(err, &r) {
		return true
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// removeStaging deletes a torrent's staged data.
//
// On Windows anacrolix's default (mmap) file IO keeps the staged files locked
// for the life of the process, so this can legitimately fail: retry briefly,
// then leave it for the next daemon start (torrentClient clears the whole
// staging dir before the client is created). Setting
// TORRENT_STORAGE_DEFAULT_FILE_IO=classic makes deletion work in-process.
func (m *Manager) removeStaging(dir string) {
	if dir == "" {
		return
	}
	for i := 0; i < 5; i++ {
		if err := os.RemoveAll(dir); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := os.Stat(dir); err != nil {
		return // gone after all (e.g. removed while we retried)
	}
	m.log.Printf("torrent staging %s is still locked; it will be reclaimed on the next daemon start "+
		"(set TORRENT_STORAGE_DEFAULT_FILE_IO=classic to allow in-process removal on Windows)", dir)
}

// stagingTargets lists everything a torrent stages: the payload path plus
// anacrolix's in-progress counterpart. Piece data lands in "<name>.part" and is
// only renamed to the final name when the torrent completes, so a transfer that
// fails (or is cancelled) mid-download would otherwise leak its .part file.
func stagingTargets(stage, name string) []string {
	base := stagedPath(stage, name)
	if base == "" {
		return nil
	}
	return []string{base, base + ".part"}
}

// stagedPath resolves the staging path for a torrent name, refusing names that
// would escape the staging directory.
func stagedPath(stage, name string) string {
	if stage == "" || name == "" {
		return ""
	}
	clean := filepath.Clean("/" + strings.ReplaceAll(name, "\\", "/"))
	target := filepath.Join(stage, clean)
	rel, err := filepath.Rel(stage, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return ""
	}
	return target
}

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
	// Reclaim staging data from a previous daemon run. This is the only moment
	// stale torrent files can reliably be deleted on Windows.
	if err := os.RemoveAll(m.tdir); err != nil {
		m.log.Printf("could not clear stale torrent staging %s: %v", m.tdir, err)
	}
	if err := os.MkdirAll(m.tdir, 0o755); err != nil {
		return nil, err
	}
	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = m.tdir
	cfg.ListenPort = 0 // random ephemeral port; single node
	cfg.Seed = false
	cfg.NoUpload = true
	cfg.NoDHT = false
	// GitHub-hosted runners sit behind an Azure NAT that filters UDP and offers
	// no inbound connectivity: anacrolix's default uTP peer transport (which is
	// UDP) and IPv6 connections are exactly what stalls there. Force plain TCP
	// for peer data — outbound TCP is the one transport the runners' egress
	// allows — and skip IPv6 to avoid slow dual-stack timeouts.
	cfg.DisableUTP = true
	cfg.DisableIPv6 = true
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
	m.mu.Lock()
	m.kept = map[string]*torrent.Torrent{}
	m.keptOrder = nil
	m.mu.Unlock()
	if cl != nil {
		_ = cl.Close() // drops every torrent, including parked ones
	}
	m.removeStaging(tdir)
}

// MaxKeptTorrents bounds how many failed/cancelled torrents stay parked in the
// client (with their staged pieces) waiting for a retry.
const MaxKeptTorrents = 4

// keepTorrent parks a torrent so that a retry resumes it. Its data download is
// paused so a cancelled or stalled transfer stops consuming bandwidth, while
// the pieces already on disk (and the in-memory completion) are retained.
func (m *Manager) keepTorrent(id string, tt *torrent.Torrent) {
	if tt == nil {
		return
	}
	tt.DisallowDataDownload()
	m.mu.Lock()
	if _, dup := m.kept[id]; !dup {
		m.keptOrder = append(m.keptOrder, id)
	}
	m.kept[id] = tt
	var evicted []*torrent.Torrent
	for len(m.keptOrder) > MaxKeptTorrents {
		oldest := m.keptOrder[0]
		m.keptOrder = m.keptOrder[1:]
		if e, ok := m.kept[oldest]; ok {
			delete(m.kept, oldest)
			evicted = append(evicted, e)
		}
	}
	m.mu.Unlock()

	stage := m.torrentStageDir()
	for _, e := range evicted {
		name := ""
		if info := e.Info(); info != nil {
			name = info.BestName()
		}
		e.Drop()
		for _, target := range stagingTargets(stage, name) {
			m.removeStaging(target)
		}
		m.log.Printf("dropped a parked torrent to stay within %d kept transfers", MaxKeptTorrents)
	}
}

// takeKeptTorrent pops a parked torrent for a retry (nil when there is none).
func (m *Manager) takeKeptTorrent(id string) *torrent.Torrent {
	m.mu.Lock()
	defer m.mu.Unlock()
	tt, ok := m.kept[id]
	if !ok {
		return nil
	}
	delete(m.kept, id)
	for i, k := range m.keptOrder {
		if k == id {
			m.keptOrder = append(m.keptOrder[:i], m.keptOrder[i+1:]...)
			break
		}
	}
	return tt
}

// downloadTorrent downloads a magnet/.torrent and commits each contained file
// as an object. The transfer's Key is the base key (or the torrent name when
// empty); multi-file torrents become base/<relative path> objects.
func (m *Manager) downloadTorrent(ctx context.Context, t meta.Transfer) (err error) {
	cl, err := m.torrentClient()
	if err != nil {
		return err
	}

	stage := m.torrentStageDir()
	// A retry of a parked torrent continues that very torrent: anacrolix stores
	// in-progress data as "<name>.part" and resets a file's piece completion
	// whenever its torrent is re-added, so dropping and re-adding would silently
	// re-download everything.
	var spec *torrent.TorrentSpec
	tt := m.takeKeptTorrent(t.ID)
	if tt != nil {
		tt.AllowDataDownload()
	} else {
		spec, err = torrentSpecFor(t, m.cfg.TorrentTrackers)
		if err != nil {
			return err
		}
		var fresh bool
		tt, fresh, err = cl.AddTorrentSpec(spec)
		if err != nil {
			return err
		}
		if !fresh {
			// Dropping releases only our reference; the active transfer owns it.
			tt.Drop()
			return fmt.Errorf("this torrent is already being downloaded by another transfer; cancel that one first")
		}
	}

	defer func() {
		info := tt.Info()
		if info == nil {
			// no metadata yet -> nothing was staged
			if err != nil && isRetryable(err) {
				m.keepTorrent(t.ID, tt)
				return
			}
			tt.Drop()
			return
		}
		if err != nil && isRetryable(err) {
			// keep the pieces (and the torrent) so POST .../transfers/{id}/retry resumes
			m.log.Printf("transfer %s: keeping staged data for a retry (%v)", t.ID, err)
			m.keepTorrent(t.ID, tt)
			return
		}
		tt.Drop()
		for _, target := range stagingTargets(stage, info.BestName()) {
			m.removeStaging(target)
		}
	}()

	// Wait for metadata to arrive (magnet announces / tracker / DHT). A magnet
	// with no reachable peers would otherwise block here forever. A parked
	// torrent that already has metadata resumes immediately.
	if tt.Info() == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tt.GotInfo():
		case <-time.After(TorrentMetadataTimeout):
			return retryableF("no metadata after %s: no peer or tracker supplied the torrent "+
				"(peers=%d, trackers=%d) — check the magnet's trackers or upload the .torrent file",
				TorrentMetadataTimeout, tt.Stats().TotalPeers, trackerCount(spec))
		}
	}
	info := tt.Info()
	tt.DownloadAll()

	total := tt.Length()
	files := tt.Files()
	if len(files) == 0 {
		return fmt.Errorf("torrent contains no files")
	}

	// Phase 1: download all pieces into the staging dir, reporting progress and
	// failing when the swarm stops delivering data (dead torrent, no seeders,
	// blocked inbound/outbound BitTorrent traffic).
	{
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		lastBytes := tt.BytesCompleted()
		lastAdvance := time.Now()
		// The fixed 5-minute stall timeout is too twitchy for 50 GB swarms: a
		// healthy giant torrent routinely pauses for longer than that between
		// piece deliveries (slow seeder, tracker re-announce windows, ISP
		// throttling). Scale the allowance with the torrent's size: 1 minute
		// per GiB, bounded to [10 min, 1 h].
		stallTimeout := time.Duration(total/(1<<30)) * time.Minute
		if stallTimeout < 10*time.Minute {
			stallTimeout = 10 * time.Minute
		}
		if stallTimeout > time.Hour {
			stallTimeout = time.Hour
		}
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Report progress every tick, even when byte count is unchanged:
			// UpdateTransferProgress also refreshes updated_at, which doubles as
			// the liveness heartbeat. A 50 GB download can legitimately sit at
			// the same byte count for a while during swarm ramp-up; without the
			// keepalive it looks "stuck" and gets requeued.
			bc := tt.BytesCompleted()
			_ = m.meta.UpdateTransferProgress(t.ID, bc, total)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tt.Complete().On():
				goto downloaded
			case <-ticker.C:
				bc := tt.BytesCompleted()
				if bc > lastBytes {
					lastBytes, lastAdvance = bc, time.Now()
					continue
				}
				if time.Since(lastAdvance) >= stallTimeout {
					st := tt.Stats()
					return retryableF("stalled for %s at %s of %s (peers=%d, seeders=%d, trackers=%d, webseeds=%d): "+
						"no data is arriving — the swarm may have no seeders or BitTorrent traffic may be blocked",
						stallTimeout, byteSize(bc), byteSize(total), st.TotalPeers, st.ConnectedSeeders,
						trackerCount(spec), webseedCount(spec))
				}
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

		hash, size, commitErr := m.commitTorrentFile(ctx, t, i, f, key, tracker)
		if commitErr != nil {
			return commitErr
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

// commitTorrentFile materializes one torrent file as a durable blob and
// returns its (hash, size).
//
// For large files (torrents routinely deliver 50 GB+), copying the staged data
// into the store's tmp dir first doubles peak disk usage and adds a full
// sequential read+write pass. When the staged data is complete on disk, we
// hash it in place and promote it directly instead — no tmp copy. Any failure
// along that fast path falls back to the original copy-through-tmp route.
func (m *Manager) commitTorrentFile(ctx context.Context, t meta.Transfer, i int, f *torrent.File, key string, tracker *aggregateTracker) (string, int64, error) {
	tmpID := fmt.Sprintf("%s-%02d", t.ID, i)
	_ = m.sto.AbortTmp(tmpID)

	// fast path: the staged file is complete, so hash + promote in place.
	// mmap-backed storage may also expose a sparse .part tail, so accept the
	// fast path only when the final (non-.part) staged file exists at full size.
	if fi, err := os.Stat(f.Path()); err == nil && fi.Size() == f.Length() {
		hash, size, perr := m.sto.PromoteExistingFile(f.Path())
		if perr == nil {
			m.log.Printf("transfer %s promoted %s/%s (%s) from staged data without a tmp copy", t.ID, t.Bucket, key, byteSize(size))
			return hash, size, nil
		}
		m.log.Printf("transfer %s: in-place promote of %s failed (%v); falling back to a tmp copy", t.ID, key, perr)
	}

	tmp, err := m.sto.OpenTmp(tmpID)
	if err != nil {
		return "", 0, err
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
		return "", 0, copyErr
	}
	if closeErr != nil {
		_ = m.sto.AbortTmp(tmpID)
		return "", 0, closeErr
	}
	if closeErr2 != nil {
		_ = m.sto.AbortTmp(tmpID)
		return "", 0, closeErr2
	}

	// a cancel that lands while the file was being copied out must still win:
	// otherwise the transfer commits after the user cancelled it
	if ctx.Err() != nil {
		_ = m.sto.AbortTmp(tmpID)
		return "", 0, ctx.Err()
	}

	// re-check the lease before committing each file
	if !m.canWrite() {
		_ = m.sto.AbortTmp(tmpID)
		return "", 0, fmt.Errorf("node is no longer the writer; transfer not committed")
	}
	hash, size, _, err := m.sto.FinalizeTmp(tmpID)
	if err != nil {
		return "", 0, err
	}
	return hash, size, nil
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

// addFallbackTrackers appends the configured announce URLs to a torrent spec.
// They are added *in addition to* any trackers the source already lists (so a
// magnet or .torrent whose own trackers are all udp:// still gets TCP-based
// HTTP(S) announce URLs), deduplicated against what is already present.
// Duplicate URLs within the existing tiers are preserved — anacrolix tolerates
// them, and rewriting tiers risks dropping the distinction between tiers.
func addFallbackTrackers(spec *torrent.TorrentSpec, extra []string) {
	if spec == nil || len(extra) == 0 {
		return
	}
	seen := map[string]bool{}
	for _, tier := range spec.Trackers {
		for _, u := range tier {
			seen[u] = true
		}
	}
	// Keep the caller's tiers untouched unless we actually add something: a
	// tracker-less spec starts with a nil/empty Trackers field and anacrolix
	// treats an *empty* slice differently from "no trackers".
	var add []string
	for _, u := range extra {
		if !seen[u] {
			seen[u] = true
			add = append(add, u)
		}
	}
	if len(add) == 0 {
		return
	}
	if len(spec.Trackers) == 0 {
		spec.Trackers = [][]string{add}
		return
	}
	tiers := make([][]string, 0, len(spec.Trackers)+1)
	tiers = append(tiers, spec.Trackers...)
	tiers = append(tiers, add)
	spec.Trackers = tiers
}

// torrentSpecFor builds the torrent spec for a transfer's source (uploaded
// .torrent metainfo or a magnet link).
func torrentSpecFor(t meta.Transfer, fallbackTrackers []string) (*torrent.TorrentSpec, error) {
	switch {
	case t.TorrentData != "":
		raw, err := base64.StdEncoding.DecodeString(t.TorrentData)
		if err != nil {
			return nil, fmt.Errorf("bad .torrent payload: %w", err)
		}
		mi, err := metainfo.Load(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("bad .torrent metainfo: %w", err)
		}
		spec, err := torrent.TorrentSpecFromMetaInfoErr(mi)
		if err != nil {
			return nil, err
		}
		// Same TCP-first fallback as magnets: a .torrent whose own announce URLs
		// are all udp:// would otherwise get no peer list on networks that filter
		// UDP (e.g. GitHub-hosted runner egress).
		addFallbackTrackers(spec, fallbackTrackers)
		return spec, nil
	case strings.HasPrefix(t.URL, "magnet:"):
		spec, err := torrent.TorrentSpecFromMagnetUri(t.URL)
		if err != nil {
			return nil, fmt.Errorf("bad magnet link: %w", err)
		}
		addFallbackTrackers(spec, fallbackTrackers)
		return spec, nil
	}
	return nil, fmt.Errorf("torrent transfer has neither magnet nor metainfo")
}

// webseedCount counts the webseed URLs in a torrent spec.
func webseedCount(spec *torrent.TorrentSpec) int {
	if spec == nil {
		return 0
	}
	return len(spec.Webseeds)
}

// trackerCount counts the tracker URLs in a torrent spec (tiers flattened).
func trackerCount(spec *torrent.TorrentSpec) int {
	if spec == nil {
		return 0
	}
	n := 0
	for _, tier := range spec.Trackers {
		n += len(tier)
	}
	return n
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
