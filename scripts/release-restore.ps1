# release-restore.ps1 — restore node state from the lifetime GitHub release
# archive at boot (before the daemon starts).
#
#   * downloads every part-*.zip and expands straight into $STORAGED_DATA_DIR
#     (entries are blobs\<hh>\<hash>, so extraction merges into blobs/)
#   * reassembles split blobs from part-<hash>-<n>-<sha12>.bin pieces
#   * restores meta.db from meta-*.zip only when none exists yet (the rescue
#     snapshot/meta-checkpoint may be fresher)
#
# Safe to run on a fresh node with no archive (empty release) and never fails
# the job: a lost network/GitHub blip must not block the node from starting.
#
# Env: GH_REPO, GH_TOKEN, STORAGED_DATA_DIR, STORAGED_ARCHIVE_RELEASE
param(
    [string]$Release     = "",
    [string]$DataDir     = "",
    [string]$DownloadDir = ""
)

$ErrorActionPreference = "Continue"
function Log($m) { Write-Host "[restore] $m" }

$dataDir = if ($DataDir) { $DataDir } elseif ($env:STORAGED_DATA_DIR) { $env:STORAGED_DATA_DIR } else { "D:\storage" }
if (-not $Release) { $Release = if ($env:STORAGED_ARCHIVE_RELEASE) { $env:STORAGED_ARCHIVE_RELEASE } else { "backups" } }
$repo  = $env:GH_REPO
$token = $env:GH_TOKEN

Log "dataDir=$dataDir release=$Release repo=$repo"
if (-not $repo -or -not $token) { Log "GH_REPO/GH_TOKEN unset; nothing to restore"; exit 0 }
if (-not (Get-Command gh -ErrorAction SilentlyContinue)) { Log "gh not found; nothing to restore"; exit 0 }

$runnerTemp = $env:RUNNER_TEMP; if (-not $runnerTemp) { $runnerTemp = $env:TEMP }
if (-not $DownloadDir) { $DownloadDir = Join-Path $runnerTemp "release-restore" }
New-Item -ItemType Directory -Force -Path $DownloadDir | Out-Null

function Get-RemoteAssets {
    $out = (& gh release view $Release --repo $repo --json assets 2>$null) | Out-String
    if (-not $out) { return @() }
    try { $obj = $out | ConvertFrom-Json } catch { return @() }
    $list = @()
    foreach ($a in $obj.assets) { $list += [pscustomobject]@{ name = $a.name } }
    return $list
}

$assets = Get-RemoteAssets
if (-not $assets) { Log "release $Release has no assets (first deployment?); starting fresh"; exit 0 }

$zips = @($assets | Where-Object { $_.name -like 'part-*.zip' })
$meta = @($assets | Where-Object { $_.name -like 'meta-*.zip' })
$bins = @($assets | Where-Object { $_.name -like 'part-*.bin' })
Log "archive has $($zips.Count) part zips, $($bins.Count) split pieces, $($meta.Count) meta snapshots"

New-Item -ItemType Directory -Force -Path (Join-Path $dataDir "db")    | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $dataDir "blobs") | Out-Null

# 1) part zips -> blobs
foreach ($a in $zips) {
    $t = Join-Path $DownloadDir $a.name
    if (-not (Test-Path $t)) {
        (& gh release download $Release --repo $repo --pattern $a.name --dir $DownloadDir --clobber 2>&1) | Out-Null
    }
    if (Test-Path $t) {
        try { Expand-Archive -Path $t -DestinationPath $dataDir -Force } catch { Log "expand failed $($a.name): $_" }
        Remove-Item $t -Force -ErrorAction Continue
    }
}
Log "expanded $($zips.Count) part zips"

# 2) meta.db (only when the node has none yet -> rescue/checkpoint may be newer)
foreach ($a in $meta) {
    $t = Join-Path $DownloadDir $a.name
    if (-not (Test-Path $t)) {
        (& gh release download $Release --repo $repo --pattern $a.name --dir $DownloadDir --clobber 2>&1) | Out-Null
    }
    if (Test-Path $t) {
        $tmp = Join-Path $DownloadDir "meta-unzip"
        New-Item -ItemType Directory -Force -Path $tmp | Out-Null
        try {
            Expand-Archive -Path $t -DestinationPath $tmp -Force
            $mdb = Join-Path $tmp "meta.db"
            if ((Test-Path $mdb) -and -not (Test-Path (Join-Path $dataDir "db\meta.db"))) {
                Copy-Item $mdb (Join-Path $dataDir "db\meta.db") -Force
                Log "restored meta.db from $($a.name)"
            }
        } catch { Log "meta expand failed: $_" }
        Remove-Item $t -Force -ErrorAction Continue
    }
}

# 3) reassemble split blobs (part-<hash>-<n>-<sha12>.bin, sorted by <n>)
if ($bins.Count -gt 0) {
    $groups = @($bins | Group-Object { ($_.name -split '-')[1] })
    foreach ($g in $groups) {
        $hash = $g.Name.ToLower()
        $hh = $hash.Substring(0,2)
        $pieces = @($g.Group | Sort-Object { [int](($_.name -split '-')[2]) })
        $out = Join-Path $dataDir "blobs\$hh\$hash"
        New-Item -ItemType Directory -Force -Path (Split-Path $out) | Out-Null

        $os = [System.IO.File]::Open($out, [System.IO.FileMode]::Create)
        try {
            foreach ($p in $pieces) {
                $pt = Join-Path $DownloadDir $p.name
                if (-not (Test-Path $pt)) {
                    (& gh release download $Release --repo $repo --pattern $p.name --dir $DownloadDir --clobber 2>&1) | Out-Null
                }
                $r = [System.IO.File]::OpenRead($pt)
                try { $r.CopyTo($os) } finally { $r.Dispose() }
                Remove-Item $pt -Force -ErrorAction Continue
            }
        } finally { $os.Dispose() }
        Log "reassembled $hash from $($pieces.Count) pieces"
    }
}

Log "restore complete"
exit 0