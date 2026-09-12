-- storaged metadata schema (SQLite, WAL mode). Applied idempotently.

CREATE TABLE IF NOT EXISTS kv (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS buckets (
    name       TEXT PRIMARY KEY,
    is_public  INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS objects (
    bucket       TEXT NOT NULL,
    key          TEXT NOT NULL,
    blob_hash    TEXT NOT NULL,
    size         INTEGER NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    etag         TEXT NOT NULL,
    metadata     TEXT NOT NULL DEFAULT '{}',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    trashed_at   TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (bucket, key),
    FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
CREATE INDEX IF NOT EXISTS idx_objects_trashed ON objects(bucket, trashed_at);

CREATE TABLE IF NOT EXISTS oplog (
    lsn          INTEGER PRIMARY KEY AUTOINCREMENT,
    op           TEXT NOT NULL,
    bucket       TEXT NOT NULL,
    key          TEXT NOT NULL,
    blob_hash    TEXT NOT NULL DEFAULT '',
    size         INTEGER NOT NULL DEFAULT 0,
    content_type TEXT NOT NULL DEFAULT '',
    metadata     TEXT NOT NULL DEFAULT '{}',
    is_public    INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_oplog_lsn ON oplog(lsn);

CREATE TABLE IF NOT EXISTS uploads (
    upload_id    TEXT PRIMARY KEY,
    bucket       TEXT NOT NULL,
    key          TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    created_at   TEXT NOT NULL,
    UNIQUE (bucket, key)
);

CREATE TABLE IF NOT EXISTS upload_parts (
    upload_id TEXT NOT NULL,
    part_num  INTEGER NOT NULL,
    blob_hash TEXT NOT NULL,
    size      INTEGER NOT NULL,
    PRIMARY KEY (upload_id, part_num),
    FOREIGN KEY (upload_id) REFERENCES uploads(upload_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lease (
    name        TEXT PRIMARY KEY,
    holder      TEXT NOT NULL,
    acquired_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transfers (
    id           TEXT PRIMARY KEY,
    bucket       TEXT NOT NULL,
    key          TEXT NOT NULL,
    url          TEXT NOT NULL,               -- http(s) URL, magnet URI, or '' for .torrent uploads
    kind         TEXT NOT NULL DEFAULT 'url', -- url | torrent
    torrent_data TEXT NOT NULL DEFAULT '',    -- base64 .torrent metainfo (kind=torrent)
    status       TEXT NOT NULL,               -- queued | downloading | done | failed | cancelled
    progress     INTEGER NOT NULL DEFAULT 0,
    size         INTEGER NOT NULL DEFAULT 0, -- total bytes once known (0 = unknown)
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    error        TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transfers_status ON transfers(status);