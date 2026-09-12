// Package client provides a thin HTTP client for the storaged REST API,
// used by the ost CLI and by keep-alive scripts.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Client talks to one storaged node.
type Client struct {
	base string
	h    http.Header
	hc   *http.Client
}

// New builds a client. base is like "http://127.0.0.1:5000". bearer may be
// empty; if non-empty it's sent as an Authorization header on every call.
func New(base, bearer string) *Client {
	h := http.Header{}
	if bearer != "" {
		h.Set("Authorization", "Bearer "+bearer)
	}
	return &Client{
		base: base,
		h:    h,
		hc:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, out any) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, err
	}
	for k, vs := range c.h {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Error != "" {
			return resp.StatusCode, fmt.Errorf("HTTP %d: %s", resp.StatusCode, e.Error)
		}
		return resp.StatusCode, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(data), 200))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// ---- health / status ----

type Health struct {
	Status        string `json:"status"`
	NodeID        string `json:"node_id"`
	Role          string `json:"role"`
	Writes        bool   `json:"writes"`
	DiskFree      int64  `json:"disk_free"`
	LSN           int64  `json:"lsn"`
	Watermark     int64  `json:"repl_watermark"`
	ActiveUploads int64  `json:"active_uploads"`
}

func (c *Client) Health(ctx context.Context) (*Health, error) {
	var h Health
	if _, err := c.do(ctx, "GET", "/v1/health", nil, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

// ---- buckets ----

type Bucket struct {
	Name      string `json:"name"`
	IsPublic  bool   `json:"is_public"`
	CreatedAt string `json:"created_at"`
}

type BucketList struct {
	Buckets []Bucket `json:"buckets"`
}

func (c *Client) ListBuckets(ctx context.Context) ([]Bucket, error) {
	var out BucketList
	if _, err := c.do(ctx, "GET", "/v1/buckets", nil, &out); err != nil {
		return nil, err
	}
	return out.Buckets, nil
}

func (c *Client) PutBucket(ctx context.Context, name string, public bool) (*Bucket, error) {
	var out struct {
		Bucket Bucket `json:"bucket"`
	}
	p := "/v1/buckets/" + url.PathEscape(name)
	if public {
		p += "?public=true"
	}
	if _, err := c.do(ctx, "PUT", p, nil, &out); err != nil {
		return nil, err
	}
	return &out.Bucket, nil
}

func (c *Client) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	var out struct {
		Bucket Bucket `json:"bucket"`
	}
	if _, err := c.do(ctx, "GET", "/v1/buckets/"+url.PathEscape(name), nil, &out); err != nil {
		return nil, err
	}
	return &out.Bucket, nil
}

func (c *Client) DeleteBucket(ctx context.Context, name string) error {
	code, err := c.do(ctx, "DELETE", "/v1/buckets/"+url.PathEscape(name), nil, nil)
	if err != nil {
		return err
	}
	if code != 204 {
		return fmt.Errorf("unexpected status %d", code)
	}
	return nil
}

// ---- objects ----

type Object struct {
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	BlobHash    string            `json:"blob_hash"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	ETag        string            `json:"etag"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   string            `json:"created_at"`
	UpdatedAt   string            `json:"updated_at"`
}

type ListResult struct {
	Entries   []ListEntry `json:"entries"`
	Next      string      `json:"next"`
	Truncated bool        `json:"truncated"`
}

type ListEntry struct {
	Object
	IsPublic bool `json:"is_public"`
}

func (c *Client) PutFile(ctx context.Context, bucket, key, file string, contentType string, progress func(done, total int64)) (*Object, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pr := &progressReader{r: f, total: st.Size(), cb: progress}
	req, err := http.NewRequestWithContext(ctx, "PUT", c.base+"/v1/buckets/"+url.PathEscape(bucket)+"/objects/"+escapeKey(key), pr)
	if err != nil {
		return nil, err
	}
	req.ContentLength = st.Size()
	for k, vs := range c.h {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Error != "" {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, e.Error)
		}
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var out struct {
		Object Object `json:"object"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &out.Object, nil
}

// GetFile downloads an object to a writer, returning its headers/object meta.
func (c *Client) GetFile(ctx context.Context, bucket, key string, w io.Writer) (*Object, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", c.base+"/v1/buckets/"+url.PathEscape(bucket)+"/objects/"+escapeKey(key), nil)
	if err != nil {
		return nil, err
	}
	for k, vs := range c.h {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &e)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, e.Error)
	}
	if _, err := io.Copy(w, resp.Body); err != nil {
		return nil, err
	}
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	obj := &Object{
		Bucket: bucket, Key: key, Size: size,
		ETag: resp.Header.Get("ETag"), ContentType: resp.Header.Get("Content-Type"),
	}
	return obj, nil
}

func (c *Client) DeleteObject(ctx context.Context, bucket, key string) error {
	code, err := c.do(ctx, "DELETE", "/v1/buckets/"+url.PathEscape(bucket)+"/objects/"+escapeKey(key), nil, nil)
	if err != nil {
		return err
	}
	if code == 204 {
		return nil
	}
	if code >= 400 {
		return fmt.Errorf("HTTP %d (delete returns 200 when soft-deleted)", code)
	}
	return nil
}

func (c *Client) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]ListEntry, error) {
	var all []ListEntry
	after := ""
	if limit <= 0 {
		limit = 1000
	}
	for {
		p := "/v1/buckets/" + url.PathEscape(bucket) + "/objects?limit=" + strconv.Itoa(limit)
		if prefix != "" {
			p += "&prefix=" + url.QueryEscape(prefix)
		}
		if after != "" {
			p += "&after=" + url.QueryEscape(after)
		}
		var out ListResult
		if _, err := c.do(ctx, "GET", p, nil, &out); err != nil {
			return nil, err
		}
		all = append(all, out.Entries...)
		if out.Next == "" {
			break
		}
		after = out.Next
	}
	return all, nil
}

// ---- admin ----

type AdminResult struct {
	BlobsRemoved int   `json:"blobs_removed"`
	BytesFreed   int64 `json:"bytes_freed"`
	Checkpointed bool  `json:"checkpointed"`
	Writes       bool  `json:"writes"`
}

func (c *Client) GC(ctx context.Context) (*AdminResult, error) {
	var out AdminResult
	if _, err := c.do(ctx, "POST", "/v1/admin/gc", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) Checkpoint(ctx context.Context) (*AdminResult, error) {
	var out AdminResult
	if _, err := c.do(ctx, "POST", "/v1/admin/checkpoint", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) Promote(ctx context.Context) (*AdminResult, error) {
	var out AdminResult
	if _, err := c.do(ctx, "POST", "/v1/admin/promote", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) Demote(ctx context.Context) (*AdminResult, error) {
	var out AdminResult
	if _, err := c.do(ctx, "POST", "/v1/admin/demote", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ---- helpers ----

func escapeKey(key string) string {
	return url.PathEscape(key)
}

type FolderResult struct {
	Entries []ListEntry `json:"entries"`
	Folders []string    `json:"folders"`
	Next    string      `json:"next"`
}

// ListFolder returns an S3-style directory listing: files directly under
// prefix plus one-level-deep subfolders. Pass next back verbatim until empty.
func (c *Client) ListFolder(ctx context.Context, bucket, prefix, after string, limit int) ([]ListEntry, []string, string, error) {
	if limit <= 0 || limit > 2000 {
		limit = 1000
	}
	p := "/v1/buckets/" + url.PathEscape(bucket) + "/objects?delimiter=/&limit=" + strconv.Itoa(limit)
	if prefix != "" {
		p += "&prefix=" + url.QueryEscape(prefix)
	}
	if after != "" {
		p += "&after=" + url.QueryEscape(after)
	}
	var out FolderResult
	if _, err := c.do(ctx, "GET", p, nil, &out); err != nil {
		return nil, nil, "", err
	}
	if out.Entries == nil {
		out.Entries = []ListEntry{}
	}
	if out.Folders == nil {
		out.Folders = []string{}
	}
	return out.Entries, out.Folders, out.Next, nil
}

type progressReader struct {
	r     io.Reader
	total int64
	done  int64
	cb    func(done, total int64)
	last  int64
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	p.done += int64(n)
	if p.cb != nil && p.done-p.last >= 128<<10 {
		p.last = p.done
		p.cb(p.done, p.total)
	}
	return n, err
}

// ---- transfers (remote URL fetch / torrent) ----

type Transfer struct {
	ID          string `json:"id"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	URL         string `json:"url"`
	Kind        string `json:"kind"`
	Status      string `json:"status"`
	Progress    int64  `json:"progress"`
	Size        int64  `json:"size"`
	ContentType string `json:"content_type"`
	Error       string `json:"error,omitempty"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

type TransferResult struct {
	Transfer Transfer `json:"transfer"`
}

func (c *Client) CreateTransfer(ctx context.Context, bucket, objectKey, srcURL, contentType string) (*Transfer, error) {
	payload := map[string]string{"url": srcURL, "key": objectKey, "content_type": contentType}
	b, _ := json.Marshal(payload)
	var out TransferResult
	if _, err := c.do(ctx, "POST", "/v1/buckets/"+url.PathEscape(bucket)+"/transfers", strings.NewReader(string(b)), &out); err != nil {
		return nil, err
	}
	return &out.Transfer, nil
}

// CreateMagnetTransfer queues a BitTorrent download from a magnet link.
func (c *Client) CreateMagnetTransfer(ctx context.Context, bucket, objectKey, magnet string) (*Transfer, error) {
	payload := map[string]string{"magnet": magnet, "key": objectKey}
	b, _ := json.Marshal(payload)
	var out TransferResult
	if _, err := c.do(ctx, "POST", "/v1/buckets/"+url.PathEscape(bucket)+"/transfers", strings.NewReader(string(b)), &out); err != nil {
		return nil, err
	}
	return &out.Transfer, nil
}

// CreateTorrentTransfer uploads raw .torrent bytes (application/x-bittorrent)
// for the server to download and commit.
func (c *Client) CreateTorrentTransfer(ctx context.Context, bucket, objectKey string, metaData []byte) (*Transfer, error) {
	q := url.Values{}
	if objectKey != "" {
		q.Set("key", objectKey)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", c.base+"/v1/buckets/"+url.PathEscape(bucket)+"/transfers?"+q.Encode(), bytes.NewReader(metaData))
	if err != nil {
		return nil, err
	}
	for k, vs := range c.h {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Content-Type", "application/x-bittorrent")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var out TransferResult
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Error != "" {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, e.Error)
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(data), 200))
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &out.Transfer, nil
}

func (c *Client) ListTransfers(ctx context.Context, bucket string) ([]Transfer, error) {
	var out struct {
		Transfers []Transfer `json:"transfers"`
	}
	p := "/v1/buckets/" + url.PathEscape(bucket) + "/transfers"
	if _, err := c.do(ctx, "GET", p, nil, &out); err != nil {
		return nil, err
	}
	return out.Transfers, nil
}

func (c *Client) GetTransfer(ctx context.Context, id string) (*Transfer, error) {
	var out TransferResult
	if _, err := c.do(ctx, "GET", "/v1/transfers/"+url.PathEscape(id), nil, &out); err != nil {
		return nil, err
	}
	return &out.Transfer, nil
}

func (c *Client) CancelTransfer(ctx context.Context, bucket, id string) error {
	code, err := c.do(ctx, "DELETE", "/v1/buckets/"+url.PathEscape(bucket)+"/transfers/"+url.PathEscape(id), nil, nil)
	if err != nil {
		return err
	}
	if code != 204 {
		return fmt.Errorf("HTTP %d", code)
	}
	return nil
}

// RetryTransfer re-queues a failed or cancelled transfer. Torrents keep their
// staged pieces, so a retry resumes from where it stopped.
func (c *Client) RetryTransfer(ctx context.Context, bucket, id string) (*Transfer, error) {
	var out TransferResult
	p := "/v1/buckets/" + url.PathEscape(bucket) + "/transfers/" + url.PathEscape(id) + "/retry"
	if _, err := c.do(ctx, "POST", p, nil, &out); err != nil {
		return nil, err
	}
	return &out.Transfer, nil
}

// ---- trash (soft delete) ----

type TrashEntry struct {
	Object
	TrashedAt string `json:"trashed_at"`
}

func (c *Client) ListTrash(ctx context.Context) ([]TrashEntry, error) {
	var out struct {
		Entries []TrashEntry `json:"entries"`
	}
	if _, err := c.do(ctx, "GET", "/v1/trash", nil, &out); err != nil {
		return nil, err
	}
	return out.Entries, nil
}

func (c *Client) RestoreObject(ctx context.Context, bucket, key string) error {
	payload := map[string]string{"bucket": bucket, "key": key}
	b, _ := json.Marshal(payload)
	code, err := c.do(ctx, "POST", "/v1/trash/restore", strings.NewReader(string(b)), nil)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("HTTP %d", code)
	}
	return nil
}

func (c *Client) PurgeObject(ctx context.Context, bucket, key string) error {
	payload := map[string]string{"bucket": bucket, "key": key}
	b, _ := json.Marshal(payload)
	code, err := c.do(ctx, "POST", "/v1/trash/purge", strings.NewReader(string(b)), nil)
	if err != nil {
		return err
	}
	if code != 204 {
		return fmt.Errorf("HTTP %d", code)
	}
	return nil
}

func (c *Client) PurgeAllTrash(ctx context.Context) (int, error) {
	var out struct {
		Purged int `json:"purged"`
	}
	if _, err := c.do(ctx, "DELETE", "/v1/trash", nil, &out); err != nil {
		return 0, err
	}
	return out.Purged, nil
}
