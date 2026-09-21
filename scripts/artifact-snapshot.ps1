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

# space check: a 50 GB rescue dump needs the zip volume + data volume to have
# real headroom; log it so a "disk full" failure is diagnosable from the log.
Get-PSDrive -PSProvider FileSystem | ForEach-Object {
    Write-Host ("[snapshot] drive {0}: free {1:N1} GB / total {2:N1} GB" -f $_.Name, ($_.Free / 1GB), (($_.Used + $_.Free) / 1GB))
}

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

# 4) compress into a single .zip. Windows PowerShell 5.1's Compress-Archive
#    corrupts/fails past 2 GB (it buffers whole files into memory and uses
#    Int32 offsets), so a 50 GB rescue MUST NOT go through it. Stream every
#    entry with ZipArchive instead: constant memory, no size limit, and
#    NoCompression so the media bytes are untouched.
# ZipArchive types live in System.IO.Compression; Windows PowerShell 5.1 does
# not resolve them when only the FileSystem assembly is loaded.
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zipDest = Join-Path $runnerTemp "storaged-rescue.zip"
if (Test-Path $zipDest) { Remove-Item $zipDest -Force }
try {
    $zip = [System.IO.Compression.ZipFile]::Open($zipDest, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        $addFile = {
            param($file, $entryName)
            $e = $zip.CreateEntry($entryName, [System.IO.Compression.CompressionLevel]::NoCompression)
            $es = $e.Open()
            try { $r = $file.OpenRead(); try { $r.CopyTo($es) } finally { $r.Dispose() } } finally { $es.Dispose() }
        }
        foreach ($f in Get-ChildItem -File -Recurse $ArtifactDir) {
            & $addFile $f $f.Name
        }
        if ($needsBlobs) {
            Write-Host "[snapshot] no healthy peer; streaming full blobs into the rescue zip"
            $blobsSrc = Join-Path $dataDir "blobs"
            Get-ChildItem -Recurse -File $blobsSrc -ErrorAction SilentlyContinue | ForEach-Object {
                if ($_.Name.Length -ne 64) { return }
                & $addFile $_ (("blobs/{0}/{1}" -f $_.Directory.Name, $_.Name))
            }
        }
    } finally { $zip.Dispose() }
} catch {
    Write-Warning "streaming zip failed ($_); falling back to Compress-Archive of the metadata-only artifact"
    if (Test-Path $zipDest) { Remove-Item $zipDest -Force }
    Compress-Archive -Path (Join-Path $ArtifactDir "*") -DestinationPath $zipDest -Force
}
Write-Host "[snapshot] rescue artifact at $zipDest ($([math]::Round((Get-Item $zipDest).Length / 1GB), 2) GB)"
Write-Host "[snapshot] blobs=$(($manifest | Measure-Object).Count)"