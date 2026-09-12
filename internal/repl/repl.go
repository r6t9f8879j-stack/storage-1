// Package repl implements follower replication: it pulls the leader's oplog,
// fetches missing blobs over HTTP, and applies mutations to the local database
// WITHOUT appending further ops (no relay loop).
package repl

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"storaged/internal/config"
	"storaged/internal/meta"
	"storaged/internal/store"
)

// Replicator pulls leader state on an interval until this node becomes the
// writer (promotion), at which point pulling stops naturally.
type Replicator struct {
	cfg  *config.Config
	meta *meta.Meta
	sto  *store.Store
	log  *log.Logger

	peerURL   string
	peerKey   string
	pollEvery time.Duration
	batchSize int

	client *http.Client

	mu           sync.RWMutex
	lastLeaderLSN int64
}

// New builds a replicator from config+meta+store. Caller should only construct
// when cfg.Peer.Enabled is set and this node is a follower.
func New(cfg *config.Config, m *meta.Meta, st *store.Store) *Replicator {
	transport := &http.Transport{
		Proxy: nil, // GitHub runners export proxy vars; strip them for peer traffic
	}
	return &Replicator{
		cfg: cfg, meta: m, sto: st,
		log:      log.New(log.Default().Writer(), "[repl] ", log.LstdFlags),
		peerURL:  strings.TrimRight(cfg.Peer.URL, "/"),
		peerKey:  cfg.Peer.Key,
		pollEvery: time.Duration(cfg.Peer.PollIntervalSeconds) * time.Second,
		batchSize: cfg.Peer.ApplyBatchSize,
		client:   &http.Client{Transport: transport, Timeout: 0},
	}
}

// LeaderLSN returns the last leader watermark observed in a poll.
func (r *Replicator) LeaderLSN() int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastLeaderLSN
}

// Start runs the pull loop until ctx is cancelled.
func (r *Replicator) Start(ctx context.Context) {
	t := time.NewTicker(r.pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.poll(ctx)
		}
	}
}

func (r *Replicator) poll(ctx context.Context) {
	// Stop pulling once we are the writer (failover).
	holder, err := r.meta.LeaseHolder()
	if err != nil {
		return
	}
	if holder == r.cfg.NodeID {
		return
	}
	wm, _ := r.meta.Watermark()

	ops, lastLSN, err := r.fetchOps(ctx, wm)
	if err != nil {
		r.log.Printf("oplog poll error: %v", err)
		return
	}
	if len(ops) == 0 {
		return
	}

	// Apply one gap-free batch; on missing blobs fetch and retry the batch.
	applied := int64(0)
	for applied < int64(len(ops)) {
		op := ops[applied]
		if op.Op == "put" {
			if !r.sto.BlobExists(op.BlobHash) {
				if err := r.fetchBlob(ctx, op.BlobHash); err != nil {
					r.log.Printf("blob fetch %s failed: %v", short(op.BlobHash), err)
					return // retry next tick
				}
			}
		}
		if err := r.applyOp(op); err != nil {
			r.log.Printf("apply op lsn=%d failed: %v", op.LSN, err)
			return
		}
		applied++
	}
	if err := r.meta.SetWatermark(lastLSN); err != nil {
		r.log.Printf("set watermark: %v", err)
		return
	}
	r.mu.Lock()
	r.lastLeaderLSN = lastLSN
	r.mu.Unlock()
	r.log.Printf("applied %d ops, watermark=%d", len(ops), lastLSN)
}

func (r *Replicator) applyOp(op meta.Op) error {
	switch op.Op {
	case "put":
		return r.meta.ApplyObjectPut(opObj(op), op.CreatedAt)
	case "delete":
		return r.meta.ApplyObjectDelete(op.Bucket, op.Key)
	case "bucket_put":
		return r.meta.ApplyBucketPut(op.Bucket, op.IsPublic == 1, op.CreatedAt)
	case "bucket_delete":
		return r.meta.ApplyBucketDelete(op.Bucket)
	case "trash":
		return r.meta.ApplyTrash(op.Bucket, op.Key, op.CreatedAt)
	case "restore":
		return r.meta.ApplyRestore(op.Bucket, op.Key)
	case "purge":
		if err := r.meta.ApplyPurge(op.Bucket, op.Key); err != nil {
			return err
		}
		if op.BlobHash != "" {
			_ = r.sto.DeleteBlob(op.BlobHash)
		}
		return nil
	}
	return fmt.Errorf("unknown op %q", op.Op)
}

func opObj(op meta.Op) *meta.Object {
	return &meta.Object{
		Bucket: op.Bucket, Key: op.Key, BlobHash: op.BlobHash,
		Size: op.Size, ContentType: op.ContentType,
		ETag: `"` + op.BlobHash + `"`, Metadata: op.Metadata,
	}
}

func (r *Replicator) fetchOps(ctx context.Context, since int64) ([]meta.Op, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	u := fmt.Sprintf("%s/_internal/oplog?since=%d&limit=%d", r.peerURL, since, r.batchSize)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("X-Peer-Secret", r.peerKey)
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, 0, fmt.Errorf("oplog %d", resp.StatusCode)
	}
	var out struct {
		Ops   []meta.Op `json:"ops"`
		LastLSN int64     `json:"last_lsn"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, 0, err
	}
	return out.Ops, out.LastLSN, nil
}

func (r *Replicator) fetchBlob(ctx context.Context, hash string) error {
	u := fmt.Sprintf("%s/_internal/blob/%s", r.peerURL, hash)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Peer-Secret", r.peerKey)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	req = req.WithContext(ctx)
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("blob %d", resp.StatusCode)
	}
	var size int64
	if resp.ContentLength > 0 {
		size = resp.ContentLength
	} else {
		size = -1
	}
	return r.sto.WriteBlob(hash, size, resp.Body)
}

func short(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}