# release-archive.ps1 — lifetime backup of node blobs + meta to a GitHub release.
#
# The GitHub Actions runner disk dies every ~6h, so blobs stored only on the VM
# are lost and the 90-day artifact rescue is not a lifetime store. This script
# mirrors every blob into the node repo's Releases (never expire, no size cap
# beyond 2GB per asset):
#   * blobs are grouped by sha256 prefix (2 hex chars) into stable buckets
#   * each bucket is zipped (NoCompression, so media is untouched bytes) into
#     part-<hh>-<sha256-of-zip>.zip assets (<=ChunkLimitMB)
#   * the zip's own sha256 is in the asset name -> exact-name dedup means we
#     only ever upload buckets whose content actually changed
#   * meta.db snapshot is stored as meta-<sha256>.zip
#   * a single blob larger than the chunk limit is split into raw
#     part-<hash>-<n>-<sha12>.bin pieces (restore concatenates them back)
#   * -Prune removes remote part-*/meta-* assets no longer current
#
# Runs forever (loop mode) or once (-Once) for a single flush. Never exits
# non-zero on data-plane errors: durability must not kill the node.
#
# Env:
#   GH_REPO, GH_TOKEN (workflow job env), STORAGED_DATA_DIR,
#   STORAGED_ARCHIVE_RELEASE (default backups), STORAGED_ARCHIVE_INTERVAL
param(
    [switch]$Once,
    [switch]$Force,
    [switch]$Prune,
    [int]$IntervalSeconds = 0,
    [int]$ChunkLimitMB = 1500,
    [string]$Release      = "",
    [string]$DataDir      = "",
    [string]$StagingDir   = "",
    [string]$StateFile    = ""
)

$ErrorActionPreference = "Continue"

$dataDir = if ($DataDir) { $DataDir } elseif ($env:STORAGED_DATA_DIR) { $env:STORAGED_DATA_DIR } else { "D:\storage" }
if (-not $Release) { $Release = if ($env:STORAGED_ARCHIVE_RELEASE) { $env:STORAGED_ARCHIVE_RELEASE } else { "backups" } }
if (-not $IntervalSeconds) { $IntervalSeconds = if ($env:STORAGED_ARCHIVE_INTERVAL) { [int]$env:STORAGED_ARCHIVE_INTERVAL } else { 600 } }
$chunkBytes = [int64]$ChunkLimitMB * 1024 * 1024

function Log($m) { Write-Host "[$(Get-Date -Format HH:mm:ss)] release-archive: $m" }

$repo  = $env:GH_REPO
$token = $env:GH_TOKEN
if (-not $repo -or -not $token) { Log "GH_REPO/GH_TOKEN unset; archive disabled"; exit 0 }
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) { Log "gh not found; archive disabled"; exit 0 }

$runnerTemp = $env:RUNNER_TEMP; if (-not $runnerTemp) { $runnerTemp = $env:TEMP }
if (-not $StagingDir) { $StagingDir = Join-Path $runnerTemp "release-archive" }
if (-not $StateFile)  { $StateFile  = Join-Path $StagingDir "release-archive-state.json" }
New-Item -ItemType Directory -Force -Path $StagingDir | Out-Null

# Both assemblies are required on Windows PowerShell 5.1: ZipArchiveMode and
# ZipArchiveEntry live in System.IO.Compression, and loading only the FileSystem
# assembly leaves that type unresolvable.
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem

# ---------------------------------------------------------------- helpers
function Get-RemoteAssets {
    # returns @{ name -> asset } for the release (empty if the release is gone)
    $out = (& gh release view $Release --repo $repo --json assets 2>$null) | Out-String
    if (-not $out) { return @() }
    try { $obj = $out | ConvertFrom-Json } catch { return @() }
    $list = @()
    foreach ($a in $obj.assets) { $list += [pscustomobject]@{ name = $a.name; id = $a.id } }
    return $list
}

function Ensure-Release {
    $view = (& gh release view $Release --repo $repo 2>$null) | Out-String
    if (-not $view) {
        Log "creating release $Release"
        (& gh release create $Release --repo $repo --title $Release --notes "storaged lifetime backup archive" 2>&1) | Out-Null
        Start-Sleep -Seconds 2
    }
}

function Get-MetaSnapshotPath {
    $ost = Join-Path $env:GITHUB_WORKSPACE "cmd\ost\ost.exe"
    if (Test-Path "$PSScriptRoot\..\cmd\ost\ost.exe") { $ost = (Resolve-Path "$PSScriptRoot\..\cmd\ost\ost.exe").Path }
    if ($ost -and (Test-Path $ost)) {
        $env:OST_BASE = if ($env:STORAGED_BASE) { $env:STORAGED_BASE } else { "http://127.0.0.1:5000" }
        $env:OST_KEY  = $env:STORAGED_ADMIN_KEY
        try { & $ost checkpoint 2>$null | Out-Null } catch { }
    }
    $psnap = Join-Path $dataDir "state-artifact\meta-snapshot.db"
    if (Test-Path $psnap) { return $psnap }
    $live = Join-Path $dataDir "db\meta.db"
    if (Test-Path $live) { return $live }
    return $null
}

function Get-BlobFiles {
    $out = New-Object System.Collections.ArrayList
    $blobsDir = Join-Path $dataDir "blobs"
    if (-not (Test-Path $blobsDir)) { return $out }
    Get-ChildItem -Recurse -Path $blobsDir -File -ErrorAction SilentlyContinue | ForEach-Object {
        if ($_.Name.Length -eq 64) { [void]$out.Add($_) }
    }
    return $out
}

function Get-BucketSig([array]$files, [long]$sum) {
    return "$($files.Count):$sum"
}

function Copy-FileToStream([string]$file, [System.IO.Stream]$s) {
    $r = [System.IO.File]::OpenRead($file)
    try { $r.CopyTo($s) } finally { $r.Dispose() }
}

function New-ZipPart([object[]]$files, [string]$prefix) {
    $tmp = Join-Path $StagingDir "$prefix-tmp.zip"
    if (Test-Path $tmp) { Remove-Item $tmp -Force }
    $zip = [System.IO.Compression.ZipFile]::Open($tmp, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        # Deterministic build: the zip's sha256 lives in the asset name and is
        # what makes "upload only what changed" work. CreateEntry stamps the
        # current time into every entry by default, so identical content would
        # hash differently on every flush and re-upload the whole bucket each
        # time the runner (and with it the state file) dies. Pin the timestamp
        # and the entry order so identical blobs always zip to identical bytes.
        $fixedTime = [DateTimeOffset]::new(1980, 1, 1, 0, 0, 0, [TimeSpan]::Zero)
        foreach ($f in ($files | Sort-Object FullName)) {
            $rel = $f.FullName.Substring($dataDir.Length).TrimStart('\','/')
            $e = $zip.CreateEntry($rel, [System.IO.Compression.CompressionLevel]::NoCompression)
            $e.LastWriteTime = $fixedTime
            $es = $e.Open()
            try { Copy-FileToStream $f.FullName $es } finally { $es.Dispose() }
        }
    } finally { $zip.Dispose() }

    $sha = (Get-FileHash $tmp -Algorithm SHA256).Hash.ToLower()
    $name = "$prefix-$sha.zip"
    Move-Item $tmp (Join-Path $StagingDir $name) -Force
    Log "built $name"
    return $name
}

function New-MetaZip([string]$snapPath) {
    $tmp = Join-Path $StagingDir "meta-tmp.zip"
    if (Test-Path $tmp) { Remove-Item $tmp -Force }
    $zip = [System.IO.Compression.ZipFile]::Open($tmp, [System.IO.Compression.ZipArchiveMode]::Create)
    try {
        $e = $zip.CreateEntry("meta.db", [System.IO.Compression.CompressionLevel]::NoCompression)
        # fixed timestamp for a deterministic hash-in-name (see New-ZipPart)
        $e.LastWriteTime = [DateTimeOffset]::new(1980, 1, 1, 0, 0, 0, [TimeSpan]::Zero)
        $es = $e.Open()
        try { Copy-FileToStream $snapPath $es } finally { $es.Dispose() }
    } finally { $zip.Dispose() }

    $sha = (Get-FileHash $tmp -Algorithm SHA256).Hash.ToLower()
    $name = "meta-$sha.zip"
    Move-Item $tmp (Join-Path $StagingDir $name) -Force
    return $name
}

function New-BinParts([System.IO.FileInfo]$file, [string]$prefix) {
    # one blob bigger than the chunk limit -> raw pieces, named so restore can
    # reconstruct: part-<hash>-<n>-<sha12>.bin (sorted by <n>)
    $names = @()
    $hash = $file.Name.ToLower()
    $pieceSize = [int64]($chunkBytes - (64 * 1024 * 1024))
    $fs = [System.IO.File]::OpenRead($file.FullName)
    try {
        $remaining = $file.Length
        $n = 0
        $buf = New-Object byte[] (4 * 1024 * 1024)
        while ($remaining -gt 0) {
            $n++
            $tmp = Join-Path $StagingDir "tmp-$hash-$n.bin"
            if (Test-Path $tmp) { Remove-Item $tmp -Force }
            $os = [System.IO.File]::Open($tmp, [System.IO.FileMode]::Create)
            try {
                $written = 0
                while ($written -lt $pieceSize -and $remaining -gt 0) {
                    $chunk = [int][Math]::Min($buf.Length, [Math]::Min($pieceSize - $written, $remaining))
                    $read = $fs.Read($buf, 0, $chunk)
                    if ($read -le 0) { break }
                    $os.Write($buf, 0, $read)
                    $written += $read
                    $remaining -= $read
                }
            } finally { $os.Dispose() }
            $sha = (Get-FileHash $tmp -Algorithm SHA256).Hash.Substring(0,12).ToLower()
            $name = "$prefix-$hash-$n-$sha.bin"
            Move-Item $tmp (Join-Path $StagingDir $name) -Force
            $names += $name
        }
    } finally { $fs.Dispose() }
    return $names
}

function Split-IntoBatches([array]$files, [long]$limit) {
    $batches = New-Object System.Collections.ArrayList
    $cur = New-Object System.Collections.ArrayList
    $curSize = [int64]0
    foreach ($f in ($files | Sort-Object Name)) {
        if ($f.Length -gt $limit) {
            if ($cur.Count -gt 0) { [void]$batches.Add(@($cur)); $cur = New-Object System.Collections.ArrayList; $curSize = [int64]0 }
            [void]$batches.Add(@($f))
        }
        else {
            if (($cur.Count -gt 0) -and (($curSize + $f.Length) -gt $limit)) {
                [void]$batches.Add(@($cur))
                $cur = New-Object System.Collections.ArrayList
                $curSize = [int64]0
            }
            [void]$cur.Add($f)
            $curSize += $f.Length
        }
    }
    if ($cur.Count -gt 0) { [void]$batches.Add(@($cur)) }
    return $batches
}

function Upload-IfNew([string]$path, [string]$name, [hashtable]$remoteByName) {
    if (-not (Test-Path $path)) { return $false }
    if ($remoteByName.ContainsKey($name)) {
        Log "skip (already archived) $name"
        Remove-Item $path -Force -ErrorAction Continue
        return $true
    }
    Log "uploading $name ($([math]::Round((Get-Item $path).Length / 1MB)) MB)"
    $out = & gh release upload $Release $path --repo $repo --clobber 2>&1
    $ok = ($LASTEXITCODE -eq 0)
    if ($ok) {
        Remove-Item $path -Force -ErrorAction Continue
    } else {
        # A failed upload must NOT delete the staged part and must surface here:
        # the caller leaves the bucket un-marked so the next flush retries it.
        # (50 GB blobs ship as ~35 pieces; a single silent failure would corrupt
        # the restore invisibly.) GitHub rate limits / 5xx make retries normal.
        Log "UPLOAD FAILED $name : $($out | Out-String)"
    }
    return $ok
}

# ---------------------------------------------------------------- state
function Load-State {
    $s = @{}
    if (Test-Path $StateFile) {
        try {
            $j = Get-Content $StateFile -Raw | ConvertFrom-Json
            foreach ($p in $j.PSObject.Properties) { $s[$p.Name] = $p.Value }
        } catch { }
    }
    return $s
}

function Save-State($s) {
    $s | ConvertTo-Json -Depth 5 | Out-File $StateFile -Encoding utf8
}

# ---------------------------------------------------------------- flush
function Invoke-Flush {
    param([switch]$Frc, [switch]$Pru)
    Log "flush start (force=$Frc prune=$Pru)"
    $metaFailed = $false
    if (-not (Test-Path (Join-Path $dataDir "blobs"))) { Log "no blobs dir yet; nothing to archive"; return }

    Ensure-Release
    $remote = Get-RemoteAssets
    $remoteByName = @{}
    foreach ($r in $remote) { if (-not $remoteByName.ContainsKey($r.name)) { $remoteByName[$r.name] = $r } }
    $keep = @{}

    # 1) meta.db snapshot
    $snap = Get-MetaSnapshotPath
    if ($snap) {
        $mName = New-MetaZip $snap
        if ($mName) {
            $keep[$mName] = $true
            if (-not (Upload-IfNew (Join-Path $StagingDir $mName) $mName $remoteByName)) { $metaFailed = $true }
        }
    } else { Log "no meta.db snapshot available" }

    # 2) blobs, bucketised by sha256 prefix
    $state = Load-State
    $blobs = Get-BlobFiles
    $groups = @($blobs | Group-Object { $_.Name.Substring(0,2).ToLower() })
    Log "blob count=$($blobs.Count) buckets=$($groups.Count)"
    $anyFailed = $false

    foreach ($g in $groups) {
        $hh = $g.Name
        $files = @($g.Group)
        $sum = [int64](($files | Measure-Object -Property Length -Sum).Sum)
        $sig = Get-BucketSig $files $sum
        $partsDone = @()
        $bucketFailed = $false

        $prev = $state[$hh]
        if ((-not $Frc) -and $prev -and ($prev.sig -eq "$sig") -and (-not $prev.failed)) {
            $partsDone = @($prev.parts)
            foreach ($p in $partsDone) { $keep[$p] = $true }
            continue
        }

        $batches = Split-IntoBatches $files $chunkBytes
        $idx = 0
        foreach ($batch in @($batches)) {
            $batchFiles = @($batch)
            if ($batchFiles.Count -eq 0) { continue }
            $prefixBase = "part-$hh"
            if ($batches.Count -gt 1) { $idx++; $prefixBase = "part-$hh-$idx" }

            $newNames = @()
            if (($batchFiles.Count -eq 1) -and ($batchFiles[0].Length -gt $chunkBytes)) {
                $newNames = @(New-BinParts $batchFiles[0] $prefixBase)
            } else {
                $zn = New-ZipPart $batchFiles $prefixBase
                if ($zn) { $newNames = @($zn) }
            }
            foreach ($nm in $newNames) {
                $keep[$nm] = $true
                if (Upload-IfNew (Join-Path $StagingDir $nm) $nm $remoteByName) {
                    $partsDone += $nm
                } else {
                    $bucketFailed = $true
                    $anyFailed = $true
                }
            }
        }
        # Mark the bucket archived ONLY when every part uploaded. A bucket with
        # failed parts is recorded as failed so the next flush retries it even
        # though its sig is unchanged.
        $state[$hh] = @{ sig = "$sig"; parts = @($partsDone); failed = [bool]$bucketFailed }
    }
    Save-State $state
    if ($metaFailed) { $anyFailed = $true }

    # 3) prune stale remote assets (only when asked: handoff flush)
    if ($Pru) {
        foreach ($r in $remote) {
            $n = $r.name
            if (($n -like 'part-*' -or $n -like 'meta-*') -and (-not $keep.ContainsKey($n))) {
                Log "pruning orphan asset $n"
                (& gh api --method DELETE "/repos/$repo/releases/assets/$($r.id)" 2>&1) | Out-Null
            }
        }
    }
    Log "flush done: kept=$(($keep.Keys | Measure-Object).Count) parts, remote=$(($remote | Measure-Object).Count) assets"
    if ($anyFailed) { Log "WARNING: some uploads failed; they will be retried on the next flush" }
    return
}

# ---------------------------------------------------------------- entry
if ($Once) {
    Invoke-Flush -Frc:$Force -Pru:$Prune
    exit 0
}

Log "background loop starting (interval=$IntervalSeconds s)"
$lastCount = -1
$lastFlush = Get-Date
while ($true) {
    Start-Sleep -Seconds 30
    $count = (Get-BlobFiles).Count
    $changed = ($count -ne $lastCount)
    $lastCount = $count
    $elapsed = ((Get-Date) - $lastFlush).TotalSeconds
    if ($changed -and ($elapsed -ge $IntervalSeconds -or $Force)) {
        Invoke-Flush
        $lastFlush = Get-Date
    }
}