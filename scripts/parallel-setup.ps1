# parallel-setup.ps1 — prepares a GitHub Actions Windows runner as a storaged
# node. Idempotent; run once at workflow start on every node.
#
#   * excludes D:\storage (data dir) from Windows Defender real-time scans
#   * resyncs the Windows clock (signed URLs assume sane time)
#   * creates the persistent storage layout
#   * deploys pre-built binaries so no Go toolchain is needed on the runner
param(
    [string]$DataDir = $env:STORAGED_DATA_DIR,
    [switch]$TakeBackup
)

$ErrorActionPreference = "Continue"
if (-not $DataDir) { $DataDir = "D:\storage" }

Write-Host "[setup] data dir: $DataDir"

# 1) Defender exclusions (blob I/O is write-heavy; scanning kills throughput)
try {
    Add-MpPreference -ExclusionPath $DataDir -ErrorAction Stop
    Add-MpPreference -ExclusionPath "$env:TEMP" -ErrorAction SilentlyContinue
    Write-Host "[setup] Defender exclusions added for $DataDir"
} catch {
    Write-Host "[setup] Defender exclusion failed (may require elevation): $_"
}

# 2) time sync — signed URL windows are +/-60s
try {
    w32tm /query /status | Out-Null
    if (-not $?) { w32tm /config /syncfromflags:manual /manualpeerlist:"time.windows.com" | Out-Null }
    w32tm /resync | Out-Null
    Write-Host "[setup] time resynced"
} catch {
    Write-Host "[setup] NTP tweak skipped: $_"
}

# 3) storage layout
foreach ($d in @($DataDir, "$DataDir\blobs", "$DataDir\tmp", "$DataDir\db", "$DataDir\state-artifact")) {
    New-Item -ItemType Directory -Path $d -Force | Out-Null
}
Write-Host "[setup] storage directories ready"

# 4) if the machine survived the last run, the SQLite WAL may need a clean
#    shutdown; storaged handles recovery via fsck-on-boot markers.
Write-Host "[setup] done"