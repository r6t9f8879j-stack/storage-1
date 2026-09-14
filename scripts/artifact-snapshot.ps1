# artifact-snapshot.ps1 — packages node state for the GitHub artifact rescue.
#
# The artifact always contains:
#   * meta.db snapshot   (VACUUM INTO, consistent point-in-time DB)
#   * manifest.json      (bucket/key/blob_hash/size for cross-node re-sync)
# Plus, ONLY when the sibling is unhealthy (we may be the last survivor), all
# blob files.
#
# Restore (parallel-setup or workflow rescue step):
#   * drop meta.db next to storaged's db dir
#   * blobs/* may be copied straight into blobs/ (content-addressed)
#   * run `ost list` to confirm; blobs missing re-fetch via replication.
param(
    [string]$ArtifactDir = "",
    [switch]$ForceBlobs
)

$ErrorActionPreference = "Continue"

# The workflow's upload-artifact step reads from ${{ runner.temp }}. GitHub
# sets RUNNER_TEMP to exactly that; fall back to TEMP so the zip lands where
# the upload step expects it.
$runnerTemp = $env:RUNNER_TEMP
if (-not $runnerTemp) { $runnerTemp = $env:TEMP }
if (-not $ArtifactDir) { $ArtifactDir = Join-Path $runnerTemp "storaged-artifact" }

$dataDir = $env:STORAGED_DATA_DIR
if (-not $dataDir) { $dataDir = "D:\storage" }
$ost = Join-Path $env:GITHUB_WORKSPACE "cmd\ost\ost.exe"
if (Test-Path "$PSScriptRoot\..\cmd\ost\ost.exe") {
    $ost = (Resolve-Path "$PSScriptRoot\..\cmd\ost\ost.exe").Path
}
if (-not (Test-Path $ost)) { $ost = (Get-Command ost -ErrorAction SilentlyContinue).Source }

if (Test-Path $ArtifactDir) { Remove-Item -Recurse -Force $ArtifactDir }
New-Item -ItemType Directory -Path $ArtifactDir | Out-Null

# 1) consistent DB snapshot (VACUUM INTO) + manifest. Never fatal: a down
#    daemon (or a fresh node with no DB yet) must still yield a rescue zip,
#    otherwise the next run's restore step fails and the node never starts.
$snap = Join-Path $ArtifactDir "meta.db"
$haveSnap = $false
if ($ost) {
    $env:OST_BASE = $env:STORAGED_BASE
    $env:OST_KEY  = $env:STORAGED_ADMIN_KEY
    try { & $ost checkpoint 2>$null | Out-Null } catch { Write-Warning "ost checkpoint failed: $_" }
    # storaged writes state-artifact\meta-snapshot.db on the hourly tick; if
    # present, copy that (it is already VACUUM INTO output).
    $psnap = Join-Path $dataDir "state-artifact\meta-snapshot.db"
    if (Test-Path $psnap) { Copy-Item $psnap $snap -Force -ErrorAction Continue; $haveSnap = $true }
}
if (-not $haveSnap -and (Test-Path (Join-Path $dataDir "db\meta.db"))) {
    Copy-Item (Join-Path $dataDir "db\meta.db") $snap -Force -ErrorAction Continue
    $haveSnap = $true
}
if (-not $haveSnap) { Write-Warning "no DB snapshot available (daemon down or fresh node); continuing with manifest only" }

# 2) manifest of all blobs (for cross-node sync after restore)
$manifest = @()
Get-ChildItem -Recurse -Path (Join-Path $dataDir "blobs") -Filter * | ForEach-Object {
    if ($_.PSIsContainer) { return }
    if ($_.Name.Length -eq 64) {
        $manifest += @{
            hash = $_.Name.ToLower()
            size = $_.Length
        }
    }
}
$manifest | ConvertTo-Json -Depth 3 | Out-File (Join-Path $ArtifactDir "manifest.json") -Encoding utf8

# 3) full blob dump only when we might be the last node standing
$needsBlobs = $ForceBlobs
if (-not $needsBlobs) {
    try {
        $peer = if ($env:STORAGED_PEER_URL) { $env:STORAGED_PEER_URL } else { "http://127.0.0.1:5000" }
        $h = Invoke-WebRequest -Uri "$peer/v1/health" -UseBasicParsing -TimeoutSec 10
        $needsBlobs = ($h.StatusCode -ne 200)
    } catch { $needsBlobs = $true }
}
if ($needsBlobs) {
    Write-Host "[snapshot] no healthy peer; dumping full blobs"
    $blobOut = Join-Path $ArtifactDir "blobs"
    Copy-Item -Recurse (Join-Path $dataDir "blobs") $blobOut -Force
}

# 4) compress into a single .zip (artifact upload is easier)
$zipDest = Join-Path $runnerTemp "storaged-rescue.zip"
Compress-Archive -Path (Join-Path $ArtifactDir "*") -DestinationPath $zipDest -Force
Write-Host "[snapshot] rescue artifact at $zipDest"
Write-Host "[snapshot] blobs=$(($manifest | Measure-Object).Count)"