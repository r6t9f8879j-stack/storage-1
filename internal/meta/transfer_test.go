package meta

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestTransferKindsAndTorrentData(t *testing.T) {
	m := newTestMeta(t)
	bucket := "tf1"
	if err := m.PutBucket(bucket, false); err != nil {
		t.Fatalf("PutBucket: %v", err)
	}

	urlID, err := m.CreateTransfer(bucket, "u", "http://example.com/a.iso", "application/octet-stream")
	if err != nil {
		t.Fatalf("CreateTransfer: %v", err)
	}
	gt, err := m.GetTransfer(urlID)
	if err != nil {
		t.Fatalf("GetTransfer: %v", err)
	}
	if gt.Kind != KindURL {
		t.Errorf("url transfer kind = %q, want %q", gt.Kind, KindURL)
	}
	if gt.URL != "http://example.com/a.iso" {
		t.Errorf("url = %q", gt.URL)
	}
	if gt.Status != "queued" {
		t.Errorf("status = %q, want queued", gt.Status)
	}

	magnet := "magnet:?xt=urn:btih:abcdef0123456789abcdef0123456789abcdef01&dn=Test"
	mgID, err := m.CreateTorrentTransfer(bucket, "mag", magnet, "")
	if err != nil {
		t.Fatalf("CreateTorrentTransfer(magnet): %v", err)
	}
	gt, err = m.GetTransfer(mgID)
	if err != nil {
		t.Fatalf("GetTransfer: %v", err)
	}
	if gt.Kind != KindTorrent {
		t.Errorf("magnet transfer kind = %q, want %q", gt.Kind, KindTorrent)
	}
	if gt.URL != magnet {
		t.Errorf("magnet url = %q", gt.URL)
	}
	if gt.TorrentData != "" {
		t.Errorf("magnet torrent_data = %q, want empty", gt.TorrentData)
	}

	raw := []byte{0x64, 0x31, 0x3a, 0x61, 0x64, 0x32, 0x3a, 0x62, 0x63, 0x65}
	td := base64.StdEncoding.EncodeToString(raw)
	tdID, err := m.CreateTorrentTransfer(bucket, "tor", "", td)
	if err != nil {
		t.Fatalf("CreateTorrentTransfer(raw): %v", err)
	}
	gt, err = m.GetTransfer(tdID)
	if err != nil {
		t.Fatalf("GetTransfer: %v", err)
	}
	if gt.Kind != KindTorrent {
		t.Errorf("raw torrent kind = %q, want %q", gt.Kind, KindTorrent)
	}
	if gt.TorrentData != td {
		t.Errorf("torrent_data roundtrip failed")
	}
}

func TestListTransfersOmitsTorrentDataAndFilters(t *testing.T) {
	m := newTestMeta(t)
	if err := m.PutBucket("bka", false); err != nil {
		t.Fatal(err)
	}
	if err := m.PutBucket("bkb", false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTransfer("bka", "one", "http://a.test/1", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTorrentTransfer("bkb", "two", "", base64.StdEncoding.EncodeToString([]byte("payload"))); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTorrentTransfer("bka", "three", "magnet:?xt=urn:btih:1111111111111111111111111111111111111111", ""); err != nil {
		t.Fatal(err)
	}

	list, err := m.ListTransfers(context.Background(), "", "")
	if err != nil {
		t.Fatalf("ListTransfers(all): %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("all-bucket list len = %d, want 3", len(list))
	}
	for _, tr := range list {
		if tr.TorrentData != "" {
			t.Errorf("listing leaked torrent_data for %s", tr.ID)
		}
		if tr.Progress != 0 || tr.Size != 0 {
			t.Errorf("listing includes progress/size detail for %s", tr.ID)
		}
	}

	filtered, err := m.ListTransfers(context.Background(), "bka", "")
	if err != nil {
		t.Fatalf("ListTransfers(a): %v", err)
	}
	if len(filtered) != 2 {
		t.Errorf("bucket filter len = %d, want 2", len(filtered))
	}

	// switch one to done, filter by status
	if err := m.SetTransferStatus(filtered[0].ID, "done", ""); err != nil {
		t.Fatal(err)
	}
	done, err := m.ListTransfers(context.Background(), "bka", "done")
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 1 {
		t.Errorf("status filter len = %d, want 1", len(done))
	}
}

func TestTransferLifecycle(t *testing.T) {
	m := newTestMeta(t)
	if err := m.PutBucket("lcx", false); err != nil {
		t.Fatal(err)
	}
	id, err := m.CreateTransfer("lcx", "k", "http://a.test/x", "")
	if err != nil {
		t.Fatal(err)
	}

	if err := m.SetTransferStatus(id, "downloading", ""); err != nil {
		t.Fatal(err)
	}
	if err := m.UpdateTransferProgress(id, 1234, 4096); err != nil {
		t.Fatal(err)
	}
	tf, err := m.GetTransfer(id)
	if err != nil {
		t.Fatal(err)
	}
	if tf.Progress != 1234 || tf.Size != 4096 {
		t.Errorf("progress = %d/%d, want 1234/4096", tf.Progress, tf.Size)
	}

	if err := m.SetTransferStatus(id, "failed", "boom"); err != nil {
		t.Fatal(err)
	}
	tf, _ = m.GetTransfer(id)
	if tf.Status != "failed" || !strings.Contains(tf.Error, "boom") {
		t.Errorf("status = %q error = %q, want failed/boom", tf.Status, tf.Error)
	}
}

func TestRetryTransfer(t *testing.T) {
	m := newTestMeta(t)
	if err := m.PutBucket("rtx", false); err != nil {
		t.Fatal(err)
	}
	id, err := m.CreateTransfer("rtx", "k", "http://a.test/x", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.SetTransferStatus(id, "downloading", ""); err != nil {
		t.Fatal(err)
	}
	if err := m.UpdateTransferProgress(id, 900, 4096); err != nil {
		t.Fatal(err)
	}
	if err := m.SetTransferStatus(id, "failed", "stalled for 5m0s"); err != nil {
		t.Fatal(err)
	}

	// a queued (still running) transfer is not retryable
	if err := m.SetTransferStatus(id, "queued", ""); err != nil {
		t.Fatal(err)
	}
	if err := m.RetryTransfer(id); !errors.Is(err, ErrInvalid) {
		t.Errorf("retry of a queued transfer: err = %v, want ErrInvalid", err)
	}

	if err := m.SetTransferStatus(id, "failed", "stalled for 5m0s"); err != nil {
		t.Fatal(err)
	}
	if err := m.RetryTransfer(id); err != nil {
		t.Fatalf("RetryTransfer: %v", err)
	}
	tf, err := m.GetTransfer(id)
	if err != nil {
		t.Fatal(err)
	}
	if tf.Status != "queued" || tf.Error != "" {
		t.Errorf("after retry: status = %q error = %q, want queued/empty", tf.Status, tf.Error)
	}
	// progress survives so a resumed torrent reports where it left off
	if tf.Progress != 900 {
		t.Errorf("progress after retry = %d, want 900", tf.Progress)
	}
	if err := m.RetryTransfer("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("retry of a missing transfer: err = %v, want ErrNotFound", err)
	}
}

func TestRequeueStuckTransfers(t *testing.T) {
	m := newTestMeta(t)
	if err := m.PutBucket("stk", false); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CreateTransfer("stk", "job", "http://a.test/stuck", ""); err != nil {
		t.Fatal(err)
	}
	idle, err := m.CreateTransfer("stk", "idle", "http://a.test/idle", "")
	if err != nil {
		t.Fatal(err)
	}
	// mark the job downloading with a stale heartbeat (older than the cutoff)
	if err := m.SetTransferStatus(idle, "downloading", ""); err != nil {
		t.Fatal(err)
	}
	requeued, err := m.RequeueStuckTransfers(time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("RequeueStuckTransfers: %v", err)
	}
	if requeued != 1 {
		t.Errorf("requeued = %d, want 1", requeued)
	}
	tf, _ := m.GetTransfer(idle)
	if tf.Status != "queued" || tf.Error != "" {
		t.Errorf("requeued status = %q error = %q, want queued/empty", tf.Status, tf.Error)
	}
}
