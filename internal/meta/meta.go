package meta

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// Object is the metadata row for a stored object.
type Object struct {
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	BlobHash    string            `json:"blob_hash"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	ETag        string            `json:"etag"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	TrashedAt   time.Time         `json:"trashed_at,omitempty"`
}

// Op is a replicated mutation record.
type Op struct {
	LSN         int64             `json:"lsn"`
	Op          string            `json:"op"` // put | delete | bucket_put | bucket_delete
	Bucket      string            `json:"bucket"`
	Key         string            `json:"key"`
	BlobHash    string            `json:"blob_hash,omitempty"`
	Size        int64             `json:"size,omitempty"`
	ContentType string            `json:"content_type,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	IsPublic    int               `json:"is_public,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
}

// ListEntry is a row from object listing.
type ListEntry struct {
	Object
	IsPublic bool `json:"is_public"`
}

// Meta owns the SQLite database (single open handle; sql.DB serializes writes).
type Meta struct {
	db   *sql.DB
	mu   sync.Mutex // serializes mutations (SQLite single-writer)
	path string
}

// New opens (creating if needed) the metadata database at path.
func New(path string) (*Meta, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdirdb %s: %w", filepath.Dir(path), err)
	}
	dsn := "file:" + filepath.ToSlash(path) +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(30000)" +
		"&_pragma=synchronous(FULL)" +
		"&_pragma=foreign_keys(1)" +
		"&_pragma=temp_store(MEMORY)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open meta db: %w", err)
	}
	db.SetMaxOpenConns(1) // serialize access; avoids SQLITE_BUSY entirely for writes
	m := &Meta{db: db, path: path}
	if err := m.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return m, nil
}

func (m *Meta) migrate() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := m.db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	// idempotent column additions for databases created by older binaries.
	cols, err := m.columns("objects")
	if err != nil {
		return err
	}
	if !hasColumn(cols, "trashed_at") {
		if _, err := m.db.ExecContext(ctx, `ALTER TABLE objects ADD COLUMN trashed_at TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate objects.trashed_at: %w", err)
		}
	}
	// transfers.kind / transfers.torrent_data (torrent & magnet transfers).
	tcols, err := m.columns("transfers")
	if err != nil {
		return err
	}
	if !hasColumn(tcols, "kind") {
		if _, err := m.db.ExecContext(ctx, `ALTER TABLE transfers ADD COLUMN kind TEXT NOT NULL DEFAULT 'url'`); err != nil {
			return fmt.Errorf("migrate transfers.kind: %w", err)
		}
	}
	if !hasColumn(tcols, "torrent_data") {
		if _, err := m.db.ExecContext(ctx, `ALTER TABLE transfers ADD COLUMN torrent_data TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("migrate transfers.torrent_data: %w", err)
		}
	}
	return nil
}

// columns returns the column names of a table, in order.
func (m *Meta) columns(table string) ([]string, error) {
	rows, err := m.db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func hasColumn(cols []string, name string) bool {
	for _, c := range cols {
		if c == name {
			return true
		}
	}
	return false
}

// Close closes the database.
func (m *Meta) Close() error { return m.db.Close() }

// Path returns the on-disk location (used by snapshot/restore).
func (m *Meta) Path() string { return m.path }

// DB exposes the handle for VACUUM INTO / snapshotting.
func (m *Meta) DB() *sql.DB { return m.db }

// Snapshot writes a consistent copy of the DB to dstPath via VACUUM INTO.
func (m *Meta) Snapshot(dstPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := os.Stat(dstPath); err == nil {
		os.Remove(dstPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	dst := strings.ReplaceAll(dstPath, "'", "''")
	_, err := m.db.Exec("VACUUM INTO '" + dst + "'")
	return err
}

// ---- buckets ----

func (m *Meta) PutBucket(name string, public bool) error {
	if err := validBucket(name); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := m.db.Exec(`
		INSERT INTO buckets(name, is_public, created_at, updated_at)
		VALUES(?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET is_public=excluded.is_public, updated_at=excluded.updated_at`,
		name, boolInt(public), now, now)
	if err != nil {
		return fmt.Errorf("put bucket: %w", err)
	}
	return m.appendOp(Op{Op: "bucket_put", Bucket: name, IsPublic: boolInt(public)})
}

type Bucket struct {
	Name      string    `json:"name"`
	IsPublic  bool      `json:"is_public"`
	CreatedAt time.Time `json:"created_at"`
}

// BucketTotal is an aggregate row for the dashboard: per-bucket object/size.
type BucketTotal struct {
	Name     string `json:"name"`
	IsPublic bool   `json:"is_public"`
	Objects  int64  `json:"objects"`
	Size     int64  `json:"size"`
}

// BucketTotals returns per-bucket object counts and sizes (buckets with no
// objects included), ordered by object count descending.
func (m *Meta) BucketTotals(ctx context.Context) ([]BucketTotal, error) {
	rows, err := m.db.QueryContext(ctx, `
		SELECT b.name, b.is_public, COUNT(o.key), COALESCE(SUM(o.size),0)
		FROM buckets b LEFT JOIN objects o ON o.bucket = b.name
		GROUP BY b.name ORDER BY COUNT(o.key) DESC, b.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BucketTotal
	for rows.Next() {
		var t BucketTotal
		var pub int
		if err := rows.Scan(&t.Name, &pub, &t.Objects, &t.Size); err != nil {
			return nil, err
		}
		t.IsPublic = pub == 1
		out = append(out, t)
	}
	return out, rows.Err()
}

func (m *Meta) GetBucket(name string) (*Bucket, error) {
	var b Bucket
	var pub int
	var created string
	err := m.db.QueryRow(`SELECT name, is_public, created_at FROM buckets WHERE name=?`, name).
		Scan(&b.Name, &pub, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	b.IsPublic = pub == 1
	if t, err := time.Parse(time.RFC3339Nano, created); err == nil {
		b.CreatedAt = t
	}
	return &b, nil
}

func (m *Meta) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT name, is_public, created_at FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		var b Bucket
		var pub int
		var created string
		if err := rows.Scan(&b.Name, &pub, &created); err != nil {
			return nil, err
		}
		b.IsPublic = pub == 1
		if t, err := time.Parse(time.RFC3339Nano, created); err == nil {
			b.CreatedAt = t
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (m *Meta) DeleteBucket(name string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res, err := m.db.Exec(`DELETE FROM buckets WHERE name=?`, name)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		// also drop the bucket's objects (blobs are GC'd separately)
		if _, err := m.db.Exec(`DELETE FROM objects WHERE bucket=?`, name); err != nil {
			return n, err
		}
		if err := m.appendOp(Op{Op: "bucket_delete", Bucket: name}); err != nil {
			return n, err
		}
	}
	return n, nil
}

// ---- objects ----

type objectRow struct {
	bucket      string
	key         string
	blobHash    string
	size        int64
	contentType string
	etag        string
	metadata    string
	createdAt   string
	updatedAt   string
	trashedAt   string
}

func scanObject(sc interface {
	Scan(dest ...any) error
}) (*Object, error) {
	var r objectRow
	if err := sc.Scan(&r.bucket, &r.key, &r.blobHash, &r.size, &r.contentType, &r.etag, &r.metadata, &r.createdAt, &r.updatedAt, &r.trashedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	obj := &Object{
		Bucket: r.bucket, Key: r.key, BlobHash: r.blobHash,
		Size: r.size, ContentType: r.contentType, ETag: r.etag,
		Metadata: map[string]string{},
	}
	if r.metadata != "" {
		_ = json.Unmarshal([]byte(r.metadata), &obj.Metadata)
	}
	if obj.Metadata == nil {
		obj.Metadata = map[string]string{}
	}
	if t, err := time.Parse(time.RFC3339Nano, r.createdAt); err == nil {
		obj.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, r.updatedAt); err == nil {
		obj.UpdatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, r.trashedAt); err == nil && r.trashedAt != "" {
		obj.TrashedAt = t
	}
	return obj, nil
}

// PutObject inserts/overwrites an object row and appends an oplog op.
// Must be called AFTER the blob bytes are safely on disk. Overwriting an
// existing object (including one sitting in the trash) clears its trashed_at.
func (m *Meta) PutObject(obj *Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := validKey(obj.Key); err != nil {
		return err
	}
	metaJSON, _ := json.Marshal(obj.Metadata)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := m.db.Exec(`
		INSERT INTO objects(bucket,key,blob_hash,size,content_type,etag,metadata,created_at,updated_at,trashed_at)
		VALUES(?,?,?,?,?,?,?,?,?,'')
		ON CONFLICT(bucket,key) DO UPDATE SET
			blob_hash=excluded.blob_hash, size=excluded.size,
			content_type=excluded.content_type, etag=excluded.etag,
			metadata=excluded.metadata, updated_at=excluded.updated_at,
			trashed_at=''`,
		obj.Bucket, obj.Key, obj.BlobHash, obj.Size, obj.ContentType, obj.ETag, string(metaJSON), now, now)
	if err != nil {
		return err
	}
	return m.appendOp(Op{
		Op: "put", Bucket: obj.Bucket, Key: obj.Key,
		BlobHash: obj.BlobHash, Size: obj.Size, ContentType: obj.ContentType, Metadata: obj.Metadata,
	})
}

func (m *Meta) GetObject(bucket, key string) (*Object, error) {
	row := m.db.QueryRow(`SELECT bucket,key,blob_hash,size,content_type,etag,metadata,created_at,updated_at,trashed_at FROM objects WHERE bucket=? AND key=?`, bucket, key)
	o, err := scanObject(row)
	if err != nil {
		return nil, err
	}
	if !o.TrashedAt.IsZero() {
		return nil, ErrNotFound // trashed objects are hidden from normal reads
	}
	b, err := m.GetBucket(bucket)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	_ = b
	return o, nil
}

// GetObjectAny returns an object row regardless of trash state (used by the
// trash listing/restore paths).
func (m *Meta) GetObjectAny(bucket, key string) (*Object, error) {
	row := m.db.QueryRow(`SELECT bucket,key,blob_hash,size,content_type,etag,metadata,created_at,updated_at,trashed_at FROM objects WHERE bucket=? AND key=?`, bucket, key)
	return scanObject(row)
}

// GetObjectRef returns blob hash + size for a key (used internally).
func (m *Meta) GetObjectRef(bucket, key string) (string, int64, error) {
	var h string
	var sz int64
	err := m.db.QueryRow(`SELECT blob_hash, size FROM objects WHERE bucket=? AND key=?`, bucket, key).Scan(&h, &sz)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, ErrNotFound
	}
	return h, sz, err
}

func (m *Meta) DeleteObject(bucket, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res, err := m.db.Exec(`DELETE FROM objects WHERE bucket=? AND key=?`, bucket, key)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, nil
	}
	if err := m.appendOp(Op{Op: "delete", Bucket: bucket, Key: key}); err != nil {
		return true, err
	}
	return true, nil
}

// ListFilter controls object listing.
type ListFilter struct {
	Prefix      string // key prefix (LIKE 'prefix%')
	After       string // exclusive cursor on key for pagination
	Search      string // substring match against key (and metadata JSON)
	Limit       int    // cap per page (1..2000, default 100)
	OnlyTrashed bool   // when true, return only trashed objects
}

// ListObjects returns entries matching the filter, ordered by key, paginated by
// After (exclusive) + Limit. Trashed objects are excluded unless
// OnlyTrashed is set. Returns next token (last key) if more remain.
func (m *Meta) ListObjects(ctx context.Context, bucket string, f ListFilter) ([]ListEntry, string, error) {
	if f.Limit <= 0 || f.Limit > 2000 {
		f.Limit = 100
	}
	q := `SELECT o.bucket,o.key,o.blob_hash,o.size,o.content_type,o.etag,o.metadata,o.created_at,o.updated_at,
	         b.is_public,o.trashed_at
	      FROM objects o LEFT JOIN buckets b ON b.name=o.bucket
	      WHERE o.bucket=?`
	args := []any{bucket}
	if f.OnlyTrashed {
		q += " AND o.trashed_at != ''"
	} else {
		q += " AND o.trashed_at = ''"
	}
	if f.Prefix != "" {
		q += " AND o.key LIKE ?"
		args = append(args, f.Prefix+"%")
	}
	if f.After != "" {
		q += " AND o.key > ?"
		args = append(args, f.After)
	}
	if f.Search != "" {
		q += " AND (o.key LIKE ? OR o.metadata LIKE ?)"
		pat := "%" + f.Search + "%"
		args = append(args, pat, pat)
	}
	q += " ORDER BY o.key LIMIT ?"
	args = append(args, f.Limit+1)

	rows, err := m.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []ListEntry
	for rows.Next() {
		var r objectRow
		var pub int
		if err := rows.Scan(&r.bucket, &r.key, &r.blobHash, &r.size, &r.contentType, &r.etag, &r.metadata, &r.createdAt, &r.updatedAt, &pub, &r.trashedAt); err != nil {
			return nil, "", err
		}
		obj := &Object{Bucket: r.bucket, Key: r.key, BlobHash: r.blobHash, Size: r.size, ContentType: r.contentType, ETag: r.etag, Metadata: map[string]string{}}
		if r.metadata != "" {
			_ = json.Unmarshal([]byte(r.metadata), &obj.Metadata)
		}
		if t, err := time.Parse(time.RFC3339Nano, r.createdAt); err == nil {
			obj.CreatedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, r.updatedAt); err == nil {
			obj.UpdatedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, r.trashedAt); err == nil && r.trashedAt != "" {
			obj.TrashedAt = t
		}
		out = append(out, ListEntry{Object: *obj, IsPublic: pub == 1})
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > f.Limit {
		out = out[:f.Limit]
		next = out[len(out)-1].Key
	}
	return out, next, nil
}

// folderCursor remembers where a delimiter listing left off. It is opaque to
// callers and survives pagination via the `after` query parameter.
type folderCursor struct {
	Group string `json:"g"` // last yielded group (so we never re-emit it)
	After string `json:"a"` // raw key to resume scanning from (exclusive)
}

func encodeCursor(c folderCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (folderCursor, error) {
	var c folderCursor
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(b, &c)
	return c, err
}

// ListFolder is an S3-style directory listing: given a prefix and a delimiter
// (typically "/"), it returns the objects that live directly under the prefix
// plus the distinct "folders" (common prefixes) one level deeper. Trashed
// objects are excluded. Pagination uses the same `after` token channel; pass
// back the returned token verbatim until it is empty.
func (m *Meta) ListFolder(ctx context.Context, bucket, prefix, delimiter string, limit int, after string) (files []ListEntry, folders []string, next string, err error) {
	if limit <= 0 || limit > 2000 {
		limit = 100
	}
	var d byte
	if delimiter != "" {
		d = delimiter[0]
	}
	cur := folderCursor{}
	if after != "" {
		if c, e := decodeCursor(after); e == nil {
			cur = c
		} else {
			cur.After = after // backwards-compatible raw key cursor
		}
	}
	files = []ListEntry{}
	folders = []string{}

	// groupOf returns the common-prefix of a key (e.g. "a/" for "a/b/c"),
	// or the key itself when it is a leaf directly under prefix. The bool is
	// true for folders.
	groupOf := func(key string) (string, bool) {
		if d == 0 {
			return key, false
		}
		if len(key) < len(prefix) || key[:len(prefix)] != prefix {
			return key, false
		}
		rel := key[len(prefix):]
		if i := strings.IndexByte(rel, d); i >= 0 {
			return prefix + rel[:i+1], true
		}
		return key, false
	}

	entryOf := func(r objectRow, pub int) ListEntry {
		obj := &Object{Bucket: r.bucket, Key: r.key, BlobHash: r.blobHash, Size: r.size,
			ContentType: r.contentType, ETag: r.etag, Metadata: map[string]string{}}
		if r.metadata != "" {
			_ = json.Unmarshal([]byte(r.metadata), &obj.Metadata)
		}
		if t, e := time.Parse(time.RFC3339Nano, r.createdAt); e == nil {
			obj.CreatedAt = t
		}
		if t, e := time.Parse(time.RFC3339Nano, r.updatedAt); e == nil {
			obj.UpdatedAt = t
		}
		if t, e := time.Parse(time.RFC3339Nano, r.trashedAt); e == nil && r.trashedAt != "" {
			obj.TrashedAt = t
		}
		return ListEntry{Object: *obj, IsPublic: pub == 1}
	}

	const cols = "o.bucket,o.key,o.blob_hash,o.size,o.content_type,o.etag,o.metadata,o.created_at,o.updated_at,b.is_public,o.trashed_at"
	const where = " FROM objects o LEFT JOIN buckets b ON b.name=o.bucket WHERE o.bucket=? AND o.trashed_at='' AND o.key>? AND instr(o.key, ?)=1"

	lastRaw := cur.After   // last key consumed (exclusive resume point)
	lastGroup := cur.Group // group already emitted (suppressed on resume)
	emitted := 0
	// Once the page fills we keep consuming keys of the already-emitted
	// trailing group (discarding them) until we hit its boundary, so a folder
	// spanning many keys never hides an un-yielded group behind it.
	const skipChunk = 256
	for {
		limitArg := limit - emitted + 1
		if emitted >= limit {
			limitArg = skipChunk
		}
		rows, qerr := m.db.QueryContext(ctx,
			"SELECT "+cols+where+" ORDER BY o.key LIMIT ?", bucket, lastRaw, prefix, limitArg)
		if qerr != nil {
			return nil, nil, "", qerr
		}
		scanned := 0
		bail := false
		for rows.Next() {
			var r objectRow
			var pub int
			if e := rows.Scan(&r.bucket, &r.key, &r.blobHash, &r.size, &r.contentType, &r.etag, &r.metadata, &r.createdAt, &r.updatedAt, &pub, &r.trashedAt); e != nil {
				rows.Close()
				return nil, nil, "", e
			}
			scanned++
			g, isFolder := groupOf(r.key)
			if g != lastGroup {
				if emitted >= limit {
					// g is a brand-new group that won't fit this page: stop and
					// resume just past the last key consumed (inside the already
					// emitted group), so g is yielded first thing next call.
					bail = true
					break
				}
				lastGroup = g
				if isFolder {
					folders = append(folders, g)
				} else {
					files = append(files, entryOf(r, pub))
				}
				emitted++
			}
			lastRaw = r.key
		}
		if e := rows.Err(); e != nil {
			rows.Close()
			return nil, nil, "", e
		}
		rows.Close()
		if bail {
			return files, folders, encodeCursor(folderCursor{After: lastRaw, Group: lastGroup}), nil
		}
		if scanned < limitArg {
			// fewer rows than requested: nothing left beyond this batch
			return files, folders, "", nil
		}
		// batch was full and everything fit (or was skipped); keep scanning so
		// we learn whether an un-yielded group follows.
	}
}

// ---- trash ----

// TrashObject soft-deletes an object (moves it to the trash). Returns false if
// the object was absent or already trashed.
func (m *Meta) TrashObject(bucket, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := m.db.Exec(`UPDATE objects SET trashed_at=? WHERE bucket=? AND key=? AND trashed_at=''`, now, bucket, key)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, nil
	}
	if err := m.appendOp(Op{Op: "trash", Bucket: bucket, Key: key}); err != nil {
		return true, err
	}
	return true, nil
}

// RestoreObject brings an object back from the trash. Returns false if the
// object is absent or not trashed.
func (m *Meta) RestoreObject(bucket, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res, err := m.db.Exec(`UPDATE objects SET trashed_at='' WHERE bucket=? AND key=? AND trashed_at!=''`, bucket, key)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return false, nil
	}
	if err := m.appendOp(Op{Op: "restore", Bucket: bucket, Key: key}); err != nil {
		return true, err
	}
	return true, nil
}

// PurgeObject permanently deletes an object and its blob. Returns blob hash +
// size (for eager blob removal on both nodes) and whether a row was deleted.
func (m *Meta) PurgeObject(bucket, key string) (hash string, size int64, deleted bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row := m.db.QueryRow(`SELECT blob_hash,size FROM objects WHERE bucket=? AND key=?`, bucket, key)
	if err := row.Scan(&hash, &size); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, false, nil
		}
		return "", 0, false, err
	}
	if _, err := m.db.Exec(`DELETE FROM objects WHERE bucket=? AND key=?`, bucket, key); err != nil {
		return "", 0, false, err
	}
	if err := m.appendOp(Op{Op: "purge", Bucket: bucket, Key: key, BlobHash: hash}); err != nil {
		return hash, size, true, err
	}
	return hash, size, true, nil
}

// ListTrashed lists trashed objects across all buckets, newest first.
func (m *Meta) ListTrashed(ctx context.Context, limit int) ([]ListEntry, error) {
	if limit <= 0 || limit > 2000 {
		limit = 100
	}
	rows, err := m.db.QueryContext(ctx, `
		SELECT o.bucket,o.key,o.blob_hash,o.size,o.content_type,o.etag,o.metadata,o.created_at,o.updated_at,
		       b.is_public,o.trashed_at
		FROM objects o LEFT JOIN buckets b ON b.name=o.bucket
		WHERE o.trashed_at != ''
		ORDER BY o.trashed_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ListEntry
	for rows.Next() {
		var r objectRow
		var pub int
		if err := rows.Scan(&r.bucket, &r.key, &r.blobHash, &r.size, &r.contentType, &r.etag, &r.metadata, &r.createdAt, &r.updatedAt, &pub, &r.trashedAt); err != nil {
			return nil, err
		}
		obj := &Object{Bucket: r.bucket, Key: r.key, BlobHash: r.blobHash, Size: r.size, ContentType: r.contentType, ETag: r.etag, Metadata: map[string]string{}}
		if r.metadata != "" {
			_ = json.Unmarshal([]byte(r.metadata), &obj.Metadata)
		}
		if t, err := time.Parse(time.RFC3339Nano, r.trashedAt); err == nil {
			obj.TrashedAt = t
		}
		out = append(out, ListEntry{Object: *obj, IsPublic: pub == 1})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []ListEntry{}
	}
	return out, nil
}

// PurgeExpiredTrash permanently deletes trashed objects older than cutoff,
// returning how many were removed. Runs from maintenance so blobs then fall to
// GC. Blob hashes of purged items are returned so callers can remove the blobs.
func (m *Meta) PurgeExpiredTrash(cutoff time.Time) (map[string]bool, error) {
	cur := cutoff.UTC().Format(time.RFC3339Nano)
	rows, err := m.db.Query(`SELECT bucket,key,blob_hash FROM objects WHERE trashed_at != '' AND trashed_at < ?`, cur)
	if err != nil {
		return nil, err
	}
	type item struct{ bucket, key, hash string }
	var items []item
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.bucket, &it.key, &it.hash); err != nil {
			rows.Close()
			return nil, err
		}
		items = append(items, it)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	orb := map[string]bool{}
	for _, it := range items {
		if _, _, deleted, err := m.PurgeObject(it.bucket, it.key); err != nil {
			return orb, err
		} else if deleted {
			orb[it.hash] = true
		}
	}
	return orb, nil
}

// ---- upload sessions (resume + multipart) ----

// BeginUpload creates (or returns existing) an upload session for key.
func (m *Meta) BeginUpload(bucket, key, contentType string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := validKey(key); err != nil {
		return "", err
	}
	var id string
	err := m.db.QueryRow(`SELECT upload_id FROM uploads WHERE bucket=? AND key=?`, bucket, key).Scan(&id)
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	id = newID("up")
	_, err = m.db.Exec(`INSERT INTO uploads(upload_id,bucket,key,content_type,created_at) VALUES(?,?,?,?,?)`,
		id, bucket, key, contentType, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return "", err
	}
	return id, nil
}

func (m *Meta) GetUpload(bucket, key, uploadID string) (string, bool, error) {
	var ct string
	err := m.db.QueryRow(`SELECT content_type FROM uploads WHERE upload_id=? AND bucket=? AND key=?`, uploadID, bucket, key).Scan(&ct)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return ct, true, nil
}

func (m *Meta) SetUploadContentType(uploadID, ct string) error {
	_, err := m.db.Exec(`UPDATE uploads SET content_type=? WHERE upload_id=?`, ct, uploadID)
	return err
}

// UploadByID reports whether an upload session exists by id alone.
func (m *Meta) UploadByID(uploadID string) (string, string, bool, error) {
	var bucket, key string
	err := m.db.QueryRow(`SELECT bucket,key FROM uploads WHERE upload_id=?`, uploadID).Scan(&bucket, &key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	return bucket, key, true, nil
}

func (m *Meta) DeleteUpload(uploadID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.db.Exec(`DELETE FROM upload_parts WHERE upload_id=?`, uploadID); err != nil {
		return err
	}
	_, err := m.db.Exec(`DELETE FROM uploads WHERE upload_id=?`, uploadID)
	return err
}

func (m *Meta) AddPart(uploadID string, partNum int, blobHash string, size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`
		INSERT INTO upload_parts(upload_id,part_num,blob_hash,size) VALUES(?,?,?,?)
		ON CONFLICT(upload_id,part_num) DO UPDATE SET blob_hash=excluded.blob_hash, size=excluded.size`,
		uploadID, partNum, blobHash, size)
	return err
}

type Part struct {
	PartNum  int    `json:"part_num"`
	BlobHash string `json:"blob_hash"`
	Size     int64  `json:"size"`
}

func (m *Meta) ListParts(uploadID string) ([]Part, error) {
	rows, err := m.db.Query(`SELECT part_num,blob_hash,size FROM upload_parts WHERE upload_id=? ORDER BY part_num`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Part
	for rows.Next() {
		var p Part
		if err := rows.Scan(&p.PartNum, &p.BlobHash, &p.Size); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// StaleUploadIDs returns upload_ids older than cutoff (for GC of tmp dirs).
func (m *Meta) StaleUploadIDs(cutoff time.Time) ([]string, error) {
	rows, err := m.db.Query(`SELECT upload_id FROM uploads WHERE created_at < ?`, cutoff.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---- replication: oplog + watermark ----

// appendOp records a mutation. Called under m.mu.
func (m *Meta) appendOp(op Op) error {
	metaJSON := "{}"
	if op.Metadata != nil {
		b, _ := json.Marshal(op.Metadata)
		metaJSON = string(b)
	}
	op.CreatedAt = time.Now().UTC()
	_, err := m.db.Exec(`
		INSERT INTO oplog(op,bucket,key,blob_hash,size,content_type,metadata,is_public,created_at)
		VALUES(?,?,?,?,?,?,?,?,?)`,
		op.Op, op.Bucket, op.Key, op.BlobHash, op.Size, op.ContentType, metaJSON, op.IsPublic, op.CreatedAt.Format(time.RFC3339Nano))
	return err
}

// OplogSince returns ops with lsn > since, up to limit.
func (m *Meta) OplogSince(since int64, limit int) ([]Op, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := m.db.Query(`
		SELECT lsn,op,bucket,key,blob_hash,size,content_type,metadata,is_public,created_at
		FROM oplog WHERE lsn > ? ORDER BY lsn LIMIT ?`, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Op
	for rows.Next() {
		var o Op
		var metaJSON string
		var created string
		if err := rows.Scan(&o.LSN, &o.Op, &o.Bucket, &o.Key, &o.BlobHash, &o.Size, &o.ContentType, &metaJSON, &o.IsPublic, &created); err != nil {
			return nil, err
		}
		o.Metadata = map[string]string{}
		if metaJSON != "" && metaJSON != "{}" {
			_ = json.Unmarshal([]byte(metaJSON), &o.Metadata)
		}
		if t, err := time.Parse(time.RFC3339Nano, created); err == nil {
			o.CreatedAt = t
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// LastLSN returns the highest oplog lsn (0 if empty).
func (m *Meta) LastLSN() (int64, error) {
	var l int64
	err := m.db.QueryRow(`SELECT COALESCE(MAX(lsn),0) FROM oplog`).Scan(&l)
	return l, err
}

// Watermark is the follower's last applied leader LSN.
func (m *Meta) Watermark() (int64, error) {
	return m.getKVInt("repl_watermark")
}

func (m *Meta) SetWatermark(lsn int64) error {
	return m.setKVInt("repl_watermark", lsn)
}

func (m *Meta) getKVInt(k string) (int64, error) {
	var v int64
	err := m.db.QueryRow(`SELECT value FROM kv WHERE key=?`, k).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

func (m *Meta) setKVInt(k string, v int64) error {
	_, err := m.db.Exec(`INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, v)
	return err
}

// ---- lease / role ----

func (m *Meta) LeaseHolder() (string, error) {
	var h string
	err := m.db.QueryRow(`SELECT holder FROM lease WHERE name='write'`).Scan(&h)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return h, err
}

// TakeLease acquires the write lease for nodeID. Returns true if acquired.
func (m *Meta) TakeLease(nodeID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	holder, err := m.LeaseHolder()
	if err != nil {
		return false, err
	}
	if holder != "" && holder != nodeID {
		return false, nil
	}
	_, err = m.db.Exec(`INSERT INTO lease(name,holder,acquired_at) VALUES('write',?,?)
		ON CONFLICT(name) DO UPDATE SET holder=excluded.holder, acquired_at=excluded.acquired_at`,
		nodeID, time.Now().UTC().Format(time.RFC3339Nano))
	return true, err
}

func (m *Meta) ReleaseLease(nodeID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	res, err := m.db.Exec(`DELETE FROM lease WHERE name='write' AND holder=?`, nodeID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// ---- remote transfers (writer-local, not replicated) ----

// Transfer status values.
const (
	StatusQueued      = "queued"
	StatusDownloading = "downloading"
	StatusDone        = "done"
	StatusFailed      = "failed"
	StatusCancelled   = "cancelled"
)

// Transfer kind values.
const (
	KindURL     = "url"
	KindTorrent = "torrent"
)

// Transfer represents a remote fetch job: either an http(s) URL (kind=url)
// or a BitTorrent download (kind=torrent) sourced from a magnet URI (url
// column) or an uploaded .torrent file (torrent_data, base64 metainfo).
type Transfer struct {
	ID          string    `json:"id"`
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	URL         string    `json:"url"`
	Kind        string    `json:"kind"`
	TorrentData string    `json:"torrent_data,omitempty"`
	Status      string    `json:"status"`
	Progress    int64     `json:"progress"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type"`
	Error       string    `json:"error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (m *Meta) CreateTransfer(bucket, key, url, ct string) (string, error) {
	if err := validKey(key); err != nil {
		return "", err
	}
	id := newID("xf")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := m.db.Exec(`INSERT INTO transfers(id,bucket,key,url,kind,status,progress,size,content_type,error,created_at,updated_at)
		VALUES(?,?,?,?,?,?,0,0,?,'',?,?)`,
		id, bucket, key, url, KindURL, "queued", ct, now, now)
	return id, err
}

// CreateTorrentTransfer queues a magnet or .torrent-upload job. src is the
// magnet URI (may be empty for raw metainfo uploads) and torrentB64 the
// base64-encoded .torrent bytes (may be empty for magnets).
func (m *Meta) CreateTorrentTransfer(bucket, key, src, torrentB64 string) (string, error) {
	if err := validKey(key); err != nil {
		return "", err
	}
	id := newID("xf")
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := m.db.Exec(`INSERT INTO transfers(id,bucket,key,url,kind,torrent_data,status,progress,size,content_type,error,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,0,0,?,'',?,?)`,
		id, bucket, key, src, KindTorrent, torrentB64, "queued", "application/x-bittorrent", now, now)
	return id, err
}

func (m *Meta) GetTransfer(id string) (*Transfer, error) {
	var t Transfer
	var created, updated string
	err := m.db.QueryRow(`SELECT id,bucket,key,url,kind,torrent_data,status,progress,size,content_type,error,created_at,updated_at FROM transfers WHERE id=?`, id).
		Scan(&t.ID, &t.Bucket, &t.Key, &t.URL, &t.Kind, &t.TorrentData, &t.Status, &t.Progress, &t.Size, &t.ContentType, &t.Error, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if created != "" {
		t.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	}
	if updated != "" {
		t.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	}
	return &t, nil
}

func (m *Meta) ListTransfers(ctx context.Context, bucket string, status string) ([]Transfer, error) {
	// deliberately omit torrent_data (large base64 blobs) from listings; the
	// worker pulls it via QueuedTransfers/GetTransfer.
	q := `SELECT id,bucket,key,url,kind,status,progress,size,content_type,error,created_at,updated_at, '' FROM transfers WHERE 1=1`
	var args []any
	if bucket != "" {
		q += " AND bucket=?"
		args = append(args, bucket)
	}
	if status != "" {
		q += " AND status=?"
		args = append(args, status)
	}
	q += " ORDER BY created_at DESC LIMIT 200"
	rows, err := m.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Transfer
	for rows.Next() {
		var t Transfer
		var created, updated string
		if err := rows.Scan(&t.ID, &t.Bucket, &t.Key, &t.URL, &t.Kind, &t.Status, &t.Progress, &t.Size, &t.ContentType, &t.Error, &created, &updated, &t.TorrentData); err != nil {
			return nil, err
		}
		if created != "" {
			t.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		}
		if updated != "" {
			t.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if out == nil {
		out = []Transfer{}
	}
	return out, nil
}

func (m *Meta) UpdateTransferProgress(id string, progress, size int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	// progress is monotonic (retry/skip paths can re-report lower counts), but
	// the update must fire even when progress has NOT advanced: updated_at is
	// also the liveness heartbeat RequeueStuckTransfers checks, and a multi-hour
	// 50 GB download can sit at the same byte count for a whole tick.
	_, err := m.db.Exec(`UPDATE transfers SET progress=MAX(progress,?), updated_at=? WHERE id=?`, progress, now, id)
	if size > 0 {
		_, _ = m.db.Exec(`UPDATE transfers SET size=? WHERE id=? AND size=0`, size, id)
	}
	return err
}

func (m *Meta) SetTransferStatus(id, status, errMsg string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if status == StatusDownloading {
		// starting a fresh (or retried) download always resets progress so a
		// crashed partial download never reports stale bytes
		_, err := m.db.Exec(`UPDATE transfers SET status=?, error=?, progress=0, updated_at=? WHERE id=?`, status, errMsg, now, id)
		return err
	}
	_, err := m.db.Exec(`UPDATE transfers SET status=?, error=?, updated_at=? WHERE id=?`, status, errMsg, now, id)
	return err
}

// RetryTransfer re-queues a finished transfer so the worker picks it up again.
// Progress is left untouched on purpose: a failed torrent keeps its staged
// pieces, so the retry resumes from where it stopped instead of starting over.
func (m *Meta) RetryTransfer(id string) error {
	t, err := m.GetTransfer(id)
	if err != nil {
		return err
	}
	switch t.Status {
	case StatusFailed, StatusCancelled:
	default:
		return ErrInvalidF("transfer is %s; only failed or cancelled transfers can be retried", t.Status)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = m.db.Exec(`UPDATE transfers SET status='queued', error='', updated_at=? WHERE id=?`, now, id)
	return err
}

func (m *Meta) QueuedTransfers(limit int) ([]Transfer, error) {
	if limit <= 0 {
		limit = 4
	}
	rows, err := m.db.Query(`SELECT id,bucket,key,url,status,content_type,kind,torrent_data FROM transfers WHERE status='queued' ORDER BY created_at LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Transfer
	for rows.Next() {
		var t Transfer
		if err := rows.Scan(&t.ID, &t.Bucket, &t.Key, &t.URL, &t.Status, &t.ContentType, &t.Kind, &t.TorrentData); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (m *Meta) RequeueStuckTransfers(olderThan time.Time) (int, error) {
	cutoff := olderThan.UTC().Format(time.RFC3339Nano)
	res, err := m.db.Exec(`UPDATE transfers SET status='queued', error='' WHERE status='downloading' AND updated_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// ---- maintenance helpers ----

// ReferencedHashes returns the set of blob hashes referenced by objects or
// in-flight upload parts (blobs GC must not remove).
func (m *Meta) ReferencedHashes() (map[string]bool, error) {
	ref := map[string]bool{}
	rows, err := m.db.Query(`SELECT DISTINCT blob_hash FROM objects UNION SELECT DISTINCT blob_hash FROM upload_parts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		ref[h] = true
	}
	return ref, rows.Err()
}

// Checkpoint truncates the WAL so the sqlite file is a full snapshot on disk.
func (m *Meta) Checkpoint() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	return err
}

// ErrNotFound is returned when a row is absent.
var ErrNotFound = errors.New("not found")

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
