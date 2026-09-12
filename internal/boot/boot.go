// Package boot wires config, auth, store, meta, replication and the HTTP
// server into a runnable process with a single-instance lock, heartbeat and
// periodic maintenance.
package boot

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"storaged/internal/api"
	"storaged/internal/auth"
	"storaged/internal/config"
	"storaged/internal/debug"
	"storaged/internal/meta"
	"storaged/internal/repl"
	"storaged/internal/store"
	"storaged/internal/transfer"
)

// Run loads config and starts serving until ctx is cancelled.
func Run(ctx context.Context, cfgPath string) error {
	debug.Attach()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	a := auth.New(cfg.AdminKey, cfg.ReadKey, cfg.PeerSecret, cfg.RatePerMinute)
	a.EnableSupabase(cfg.SupabaseURL, cfg.SupabaseAnonKey, cfg.SupabaseServiceKey)
	st := store.New(cfg.DataDir)
	if err := st.Init(); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	m, err := meta.New(filepath.Join(cfg.DataDir, "db", "meta.db"))
	if err != nil {
		return fmt.Errorf("meta: %w", err)
	}
	defer m.Close()

	lock, err := acquireLock(cfg.DataDir, cfg.NodeID)
	if err != nil {
		return err
	}
	defer lock.release()
	go lock.heartbeat(ctx)

	if cfg.IsLeader() {
		_, _ = m.TakeLease(cfg.NodeID)
	}

	srv := api.New(cfg, a, m, st)
	if !cfg.IsLeader() && cfg.Peer.Enabled {
		r := repl.New(cfg, m, st)
		srv.SetReplicator(r)
		go r.Start(ctx)
	}
	tm := transfer.NewManager(cfg, m, st, srv.WritesAllowed)
	srv.SetTransferer(tm)
	go tm.Start(ctx)

	hs := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       120 * time.Second,
		// WriteTimeout deliberately 0: large uploads/downloads stream indefinitely.
	}

	go maintenance(ctx, cfg, m, st, srv)

	errCh := make(chan error, 1)
	go func() {
		log.Printf("storaged %s listening on %s (role=%s writes=%v)", cfg.NodeID, cfg.Listen, cfg.Role, srv.WritesAllowed())
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = hs.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		return err
	}
}

// maintenance runs hourly GC + WAL checkpoint and a daily snapshot that can be
// picked up by artifact rescue.
func maintenance(ctx context.Context, cfg *config.Config, m *meta.Meta, st *store.Store, srv *api.Server) {
	gcTicker := time.NewTicker(time.Hour)
	defer gcTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-gcTicker.C:
			runGC(cfg, m, st)
			_ = m.Checkpoint()
			if srv.WritesAllowed() {
				_ = snapshotForArtifact(cfg, m)
			}
		}
	}
}

func runGC(cfg *config.Config, m *meta.Meta, st *store.Store) {
	// purge objects that have been in the trash longer than gc_grace
	purgedOrb, err := m.PurgeExpiredTrash(time.Now().Add(-cfg.GCGrace()))
	if err != nil {
		log.Printf("gc: trash purge: %v", err)
	}
	for hash := range purgedOrb {
		_ = st.DeleteBlob(hash)
	}
	// remove unreferenced blobs
	ref, err := m.ReferencedHashes()
	if err != nil {
		log.Printf("gc: referenced: %v", err)
		return
	}
	if _, fid := sremoveStaleUploads(cfg, m, st); fid > 0 {
		_ = fid
	}
	removed, freed, err := st.GarbageCollect(ref, cfg.GCGrace())
	if err != nil {
		log.Printf("gc: %v", err)
		return
	}
	if removed > 0 || len(purgedOrb) > 0 {
		log.Printf("gc: purged %d trash blobs, removed %d blobs, freed %d bytes", len(purgedOrb), removed, freed)
	}
}

func sremoveStaleUploads(cfg *config.Config, m *meta.Meta, st *store.Store) (int, int) {
	stale, err := m.StaleUploadIDs(time.Now().Add(-cfg.UploadTTL()))
	if err != nil {
		return 0, 0
	}
	for _, id := range stale {
		_ = st.AbortTmp(id)
		_ = st.AbortTmp(id + ".mp")
	}
	return len(stale), 0
}

// snapshotForArtifact writes a VACUUM INTO snapshot beside the live DB so the
// artifact-rescue job can preserve state even if both runners die.
func snapshotForArtifact(cfg *config.Config, m *meta.Meta) error {
	dst := filepath.Join(cfg.DataDir, "state-artifact", "meta-snapshot.db")
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return m.Snapshot(dst)
}

// ---- single instance lock ----

type lock struct {
	nodeID       string
	lockPath     string
	heartbeatPath string
}

func acquireLock(dataDir, nodeID string) (*lock, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(dataDir, ".instance.lock")
	heartbeat := filepath.Join(dataDir, ".instance.heartbeat")

	if data, err := os.ReadFile(lockPath); err == nil {
		parts := strings.Fields(string(data))
		if len(parts) >= 1 {
			oldNode := ""
			if len(parts) >= 2 {
				oldNode = parts[1]
			}
			age := time.Since(modTime(heartbeat))
			if age < 2*time.Minute {
				return nil, fmt.Errorf("another instance appears active (lock from %s, %s ago)", oldNode, age.Round(time.Second))
			}
		}
		// stale lock → take over
		_ = os.Remove(lockPath)
	}
	payload := fmt.Sprintf("%d %s %s\n", os.Getpid(), nodeID, time.Now().UTC().Format(time.RFC3339))
	if err := os.WriteFile(lockPath, []byte(payload), 0o644); err != nil {
		return nil, err
	}
	return &lock{nodeID: nodeID, lockPath: lockPath, heartbeatPath: heartbeat}, nil
}

func (l *lock) heartbeat(ctx context.Context) {
	write := func() {
		_ = os.WriteFile(l.heartbeatPath, []byte(fmt.Sprintf("%d\n", time.Now().Unix())), 0o644)
	}
	write()
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			write()
		}
	}
}

func (l *lock) release() {
	_ = os.Remove(l.heartbeatPath)
	_ = os.Remove(l.lockPath)
}

func modTime(p string) time.Time {
	st, err := os.Stat(p)
	if err != nil {
		return time.Time{}
	}
	return st.ModTime()
}