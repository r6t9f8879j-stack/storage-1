package meta

import (
	"encoding/json"
	"time"
)

// Apply* methods mutate local state WITHOUT appending replication ops. These
// are used by the follower when replaying the leader's oplog, so ops are never
// double-logged locally.

// ApplyBucketPut creates/updates a bucket without logging an op.
func (m *Meta) ApplyBucketPut(name string, public bool, created time.Time) error {
	if err := validBucket(name); err != nil {
		return err
	}
	now := created.UTC().Format(time.RFC3339Nano)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`
		INSERT INTO buckets(name,is_public,created_at,updated_at) VALUES(?,?,?,?)
		ON CONFLICT(name) DO UPDATE SET is_public=excluded.is_public, updated_at=excluded.updated_at`,
		name, boolInt(public), now, now)
	return err
}

// ApplyBucketDelete removes a bucket without logging an op.
func (m *Meta) ApplyBucketDelete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.db.Exec(`DELETE FROM buckets WHERE name=?`, name); err != nil {
		return err
	}
	_, err := m.db.Exec(`DELETE FROM objects WHERE bucket=?`, name)
	return err
}

// ApplyObjectPut inserts/overwrites an object row without logging an op.
func (m *Meta) ApplyObjectPut(obj *Object, created time.Time) error {
	if err := validKey(obj.Key); err != nil {
		return err
	}
	metaJSON, _ := marshalMeta(obj.Metadata)
	now := created.UTC().Format(time.RFC3339Nano)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// ensure bucket exists (defensive; bucket op precedes in oplog)
	_, err := m.db.Exec(`INSERT OR IGNORE INTO buckets(name,is_public,created_at,updated_at) VALUES(?,0,?,?)`,
		obj.Bucket, now, now)
	if err != nil {
		return err
	}
	_, err = m.db.Exec(`
		INSERT INTO objects(bucket,key,blob_hash,size,content_type,etag,metadata,created_at,updated_at,trashed_at)
		VALUES(?,?,?,?,?,?,?,?,?,'')
		ON CONFLICT(bucket,key) DO UPDATE SET
			blob_hash=excluded.blob_hash, size=excluded.size, content_type=excluded.content_type,
			etag=excluded.etag, metadata=excluded.metadata, updated_at=excluded.updated_at,
			trashed_at=''`,
		obj.Bucket, obj.Key, obj.BlobHash, obj.Size, obj.ContentType, obj.ETag, metaJSON, now, now)
	return err
}

// ApplyObjectDelete removes an object row without logging an op.
func (m *Meta) ApplyObjectDelete(bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`DELETE FROM objects WHERE bucket=? AND key=?`, bucket, key)
	return err
}

// ApplyTrash marks an object as trashed without logging an op.
func (m *Meta) ApplyTrash(bucket, key string, created time.Time) error {
	now := created.UTC().Format(time.RFC3339Nano)
	if now == "" {
		now = time.Now().UTC().Format(time.RFC3339Nano)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`UPDATE objects SET trashed_at=? WHERE bucket=? AND key=?`, now, bucket, key)
	return err
}

// ApplyRestore clears an object's trash marker without logging an op.
func (m *Meta) ApplyRestore(bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`UPDATE objects SET trashed_at='' WHERE bucket=? AND key=?`, bucket, key)
	return err
}

// ApplyPurge removes an object row without logging an op (the caller deletes
// the blob — on the follower that is done once the purge op is applied).
func (m *Meta) ApplyPurge(bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.db.Exec(`DELETE FROM objects WHERE bucket=? AND key=?`, bucket, key)
	return err
}

func marshalMeta(m map[string]string) (string, error) {
	if m == nil {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "{}", err
	}
	return string(b), nil
}