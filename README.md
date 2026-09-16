# storaged — self-hosted object storage on GitHub Actions

A from-scratch object storage system that lives on GitHub Actions self-hosted
Windows runners, persists to the runner's persistent `D:\storage`, replicates
across two nodes, is fronted by a Cloudflare named tunnel, and survives
runner wipes via a GitHub artifact rescue.

No S3, no third-party backends. Everything is in this repo.

## What it is

- **Daemon** (`cmd/storaged`) — a custom REST API built in Go.
- **CLI** (`cmd/ost`) — ops tool: health, buckets, objects, transfers, trash, gc, promote.
- **Replication** — leader writes; follower pulls a monotonic LSN op-log and
  fetches missing blobs over HTTP (`/_internal/*`). On leader death the
  follower promotes itself and takes over the tunnel.
- **Durability pipeline** — bytes are fsynced and atomically renamed into a
  content-addressed blob store *before* the SQLite metadata row is written
  (commit-after-fsync). The op-log keeps both nodes converged.
- **Artifact rescue** — at each 6-hour handoff (and on cancellation) a
  `VACUUM INTO` SQLite snapshot + blob manifest (and, only when the sibling is
  dark, all blobs) is uploaded as a 90-day GitHub artifact; the next run
  restores it.
- **Tunnel ownership** — only the current writer runs `cloudflared`, so
  `storage.<your-domain>` always points at the authoritive node.

## Layout

```
cmd/storaged        daemon entrypoint
cmd/ost             ops CLI
internal/config     TOML + STORAGED_* env config (secrets never logged)
internal/auth       bearer scopes, presigned URLs, rate limiting
internal/meta       SQLite metadata (WAL), op-log, upload sessions
internal/store      content-addressed blob store (hash[0:2]/hash)
internal/api        REST + /_internal replication endpoints
internal/transfer   background remote-URL fetch worker
internal/repl       follower pull loop
internal/boot       lock/heartbeat, HTTP server, hourly GC+checkpoint
scripts/            keep-alive, parallel-setup, artifact-snapshot
.github/workflows/  storage-host.yml runner host
conf/               example TOML
.env.example        env names used by the runner lifecycle
```

## Quick start (local)

```powershell
# Windows, PowerShell
$env:STORAGED_ADMIN_KEY="dev-admin"; $env:STORAGED_READ_KEY="dev-read"
$env:STORAGED_DATA_DIR="C:\Users\you\storaged-dev\storage"
go run ./cmd/storaged          # listens on 127.0.0.1:5000
```

In another shell:

```powershell
go run ./cmd/ost health
go run ./cmd/ost buckets create media --public
go run ./cmd/ost put media photos/first.jpg C:\Users\you\photo.jpg
go run ./cmd/ost get media photos/first.jpg out.jpg
go run ./cmd/ost list media
```

Dual-node replication (both local):

```powershell
$env:STORAGED_ROLE          = "leader"
$env:STORAGED_NODE_ID       = "node-a"
$env:STORAGED_DATA_DIR      = "…\a\storage"
go run ./cmd/storaged       # port 5000 (leader)

$env:STORAGED_ROLE          = "follower"
$env:STORAGED_NODE_ID       = "node-b"
$env:STORAGED_DATA_DIR      = "…\b\storage"
$env:STORAGED_PEER_URL      = "http://127.0.0.1:5000"
$env:STORAGED_PEER_KEY      = "peer-secret"
go run ./cmd/storaged       # set STORAGED_LISTEN=127.0.0.1:5001 first
```

## API (v1)

Auth: `Authorization: Bearer <admin|read key>` unless noted.

| Method/Path | What |
|---|---|
| `GET /v1/health` | node status, lsn, watermark, disk free (no auth) |
| `PUT /v1/buckets/{b}?public=` | create/update bucket |
| `GET /v1/buckets` · `GET /v1/buckets/{b}` · `DELETE /v1/buckets/{b}` | manage buckets |
| `PUT /v1/buckets/{b}/objects/{key...}` | full upload (auto-commit). `?upload_id=&offset=` resumes |
| `GET/HEAD /v1/buckets/{b}/objects/{key...}` | download w/ Range (206), ETag, conditionals, 416 |
| `DELETE /v1/buckets/{b}/objects/{key...}` | soft-delete (move to trash) |
| `GET /v1/buckets/{b}/objects?prefix=&after=&limit=&q=&delimiter=` | paginated listing, search (`q`), or directory view (`delimiter=/` → `folders`) |
| `POST /v1/buckets/{b}/uploads` | begin upload session (body `{"key":...}`) |
| `POST /v1/buckets/{b}/uploads/commit?upload_id=` | finalize session |
| `POST /v1/buckets/{b}/uploads/abort?upload_id=` | abort session |
| `POST /v1/buckets/{b}/uploads/upload-url?key=` | presigned PUT URL (returns `url`, `expires_at`) |
| `POST /v1/buckets/{b}/uploads/download-url?key=` | presigned GET URL |
| `POST /v1/buckets/{b}/uploads/multipart?key=` | multipart session |
| `PUT /v1/multipart/{upload_id}/parts/{n}` | upload a part |
| `POST /v1/buckets/{b}/uploads/multipart/complete?upload_id=` | concat parts → object |
| `POST /v1/buckets/{b}/transfers` | queue a remote URL / magnet / `.torrent` fetch |
| `GET /v1/buckets/{b}/transfers` · `GET /v1/transfers/{id}` | transfer status / progress |
| `DELETE /v1/buckets/{b}/transfers/{id}` | cancel a queued/running transfer |
| `POST /v1/buckets/{b}/transfers/{id}/retry` | re-queue a failed/cancelled transfer (torrents resume) |
| `GET /v1/debug/net` | probe each tracker URL over its own transport (TCP for HTTP(S), raw UDP for udp://) — shows what the runner's egress allows |
| `GET /v1/trash` | list trashed objects |
| `POST /v1/trash/restore` · `POST /v1/trash/purge` | restore one / permanently delete one |
| `DELETE /v1/trash` | purge everything in the trash |
| `GET /v1/buckets/{b}/zip?prefix=&name=` | stream a ZIP of a bucket |
| `GET /v1/public/{b}/{key...}` | public (no-auth) raw download |
| `POST /v1/admin/gc` · `/checkpoint` · `/promote` · `/demote` | maintenance |
| `/_internal/*` | peer-only: health, oplog, inventory, blob, lease |

Upload resume: `POST uploads` → response has `upload_id`. Then
`PUT …/objects/{key}?upload_id=…` with header `X-Final: false` to stream bytes
without committing (repeat with `&offset=<received>` to append). Finish with
`POST …/uploads/commit?upload_id=`. A plain PUT (no `X-Final: false`) commits
at EOF.

**Trash & remote fetch** (`ost` examples):

```powershell
go run ./cmd/ost delete media photos/first.jpg   # moves to trash (soft delete)
go run ./cmd/ost trash                           # list trashed objects
go run ./cmd/ost trash restore media photos/first.jpg
go run ./cmd/ost trash purge media photos/first.jpg   # permanent
go run ./cmd/ost trash purge-all
go run ./cmd/ost transfer media https://example.com/big.iso --key=videos/big.iso
go run ./cmd/ost transfers media                  # watch status/progress
go run ./cmd/ost transfers media retry <id>       # re-queue a failed transfer (resumes)

# torrents: paste a magnet link or upload a .torrent file
go run ./cmd/ost transfer media "magnet:?xt=urn:btih:..." --key=show
go run ./cmd/ost transfer media C:\torrents\episode.torrent
go run ./cmd/ost transfer media C:\torrents\album.torrent --key=albums/new
```

Same over plain HTTP (e.g. from a script):

```bash
curl -s -X POST -H "Authorization: Bearer $K" -H 'Content-Type: application/json' \
  -d '{"magnet":"magnet:?xt=urn:btih:...","key":"show"}' \
  https://HOST/v1/buckets/media/transfers

curl -s -X POST -H "Authorization: Bearer $K" -H 'Content-Type: application/x-bittorrent' \
  --data-binary @episode.torrent \
  'https://HOST/v1/buckets/media/transfers?key=episodes/1'
```

**Folders** — pass `delimiter=/` to list the same way a file browser does:
folders (one level deep) come back as `folders`, files directly under the
prefix as `entries`. `ost ls` paginates this for you:

```powershell
go run ./cmd/ost ls media              # dirs + root files
go run ./cmd/ost ls media videos/      # contents of videos/
```

- **Transfers** pull a remote URL or BitTorrent download in the background
  (2 at a time, 2 GiB URL cap), are writer-local, and commit as ordinary
  objects once the fetch finishes, so the follower converges through the normal
  op-log. Torrent sources are a magnet link (`{"magnet":"magnet:?...", "key":...}`
  body) or an uploaded `.torrent` file (`Content-Type: application/x-bittorrent`,
  `?key=` query param). The object key may be given either as a `?key=` query
  param or a `"key"` field in the JSON body (the body wins); with no key the
  torrent name (or `magnet_<infohash12>`) is the base. Multi-file torrents
  become `key/path/file` objects and BEP 47 padding files (`.pad`) are skipped.
  Progress is persisted to `transfers`, and the dashboard's Transfers table
  shows the failure reason when a job fails. Magnets that list no trackers of
  their own fall back to `torrent_trackers` / `STORAGED_TORRENT_TRACKERS`
  (a few public trackers by default), so peer discovery does not depend on DHT
  alone.
  **GitHub-hosted runners** (`windows-latest`) sit behind an Azure NAT that
  provides no inbound connectivity and commonly filters outbound UDP — while
  allowing outbound TCP. Peer discovery and transport are therefore tuned for
  TCP: the fallback tracker list leads with HTTP(S) announce URLs (only
  TCP 80/443 needed), fallback trackers are appended to a torrent/magnet even
  when it lists its own (which may be udp://-only), and the torrent client
  disables uTP/IPv6 so all peer data flows over plain TCP. `GET /v1/debug/net`
  probes every configured tracker over its own transport and returns
  per-tracker reachability — run it from the dashboard's Debug tab to confirm
  the runner can actually reach the tracker network before blaming a dead
  swarm.
  Failures instead of hangs: a magnet no peer or tracker can serve fails after
  `TorrentMetadataTimeout` (3 min), and a swarm that stops delivering data fails
  after `TorrentStallTimeout` (5 min) with peer/seeder/tracker counts. A crashed
  download is automatically requeued after 15 min.
  **Retry with resume:** a failed or cancelled torrent is parked in the client
  (download paused) with its pieces, so `POST …/transfers/{id}/retry` — the
  dashboard's *retry* button, or `ost transfers <bucket> retry <id>` — continues
  from what has already been fetched instead of starting over. Parking a torrent
  is required because anacrolix stores in-progress data as `<name>.part` and
  resets a file's piece completion whenever its torrent is re-added. At most
  `MaxKeptTorrents` (4) transfers are parked; older ones are dropped and their
  staging reclaimed. Re-queuing a torrent whose earlier transfer is still active
  fails with "already being downloaded"; cancel that one first. Torrent staging
  lives in `<data_dir>/torrents` and is cleared before the torrent client starts;
  on Windows also set `TORRENT_STORAGE_DEFAULT_FILE_IO=classic`, otherwise
  anacrolix's default mmap IO keeps the staged files locked for the life of the
  daemon and they are only reclaimed at the next start. Transfer records live
  only on the writing node, so if the leader dies before the job finishes the
  new leader does not inherit it (the 15-min requeue applies only when the same
  node recovers); re-submit from the dashboard or `ost transfer` after failover.
- **Trash** is soft delete: trashed objects are hidden from listing/get before
  the restore page. Overwriting a trashed object restores it in place. The
  hourly GC permanently purges trash older than `gc_grace`.

## Deployment (two runners)

The tunnel is a **dedicated `storage` tunnel** (uuid `f8cde1bb-…`),
independent from the `supabase-selfhosted` tunnel on the docker host, so
both can run simultaneously without conflict. `conf/cloudflared.yml` routes
`storage.chuglii.in` to the daemon; `conf/tunnel-credentials.json` is the
credentials file the runner uses. Both are materialized onto the runner by
the workflow from the `CF_TUNNEL_CREDENTIALS` secret (base64 of that JSON).

1. **DNS:** `storage.chuglii.in` → `f8cde1bb-85e9-44fd-abbd-a652d16fa63f.cfargotunnel.com`
   (CNAME, proxied) — already done via `cloudflared tunnel route dns`.
2. Create two node repos (mirroring the `-node-X` fleet pattern), set the
   same secrets (`STORAGED_ADMIN_KEY`, `STORAGED_READ_KEY`,
   `STORAGED_PEER_SECRET`, `CF_TUNNEL_CREDENTIALS`, `GH_TOKEN`), plus per-node
   `STORAGED_NODE_ID` / `STORAGED_ROLE` / `STORAGED_PEER_URL` (and
   `STORAGED_PUBLIC_URL`) as repo variables/secrets.
3. Push the workflow. `keep-alive.ps1` supervises the daemon + tunnel and
   re-dispatches before the 420-minute timeout; on failover the follower
   promotes and takes the tunnel.

## Failure behavior

- Runner wiped → next run starts fresh; replication from the healthy peer
  rebuilds state (blob fetch is on-demand per op).
- Both runners dead → each run's rescue artifact (SQLite snapshot + manifest,
  or full blobs when the sibling was dark) is restored by the next successful
  run on that node.
- 6h hard kill → WAL + commit-after-fsync mean committed objects are intact;
  boot markers let startups detect unclean shutdowns and re-sync.