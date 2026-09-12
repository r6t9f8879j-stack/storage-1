# keep-alive.ps1 — supervises storaged + cloudflared on a GitHub Actions runner
# and handles handoff/re-dispatch before the 6-hour timeout.
#
# Responsibilities:
#   * start storaged if not already running
#   * manage cloudflared so ONLY the current writer publishes the tunnel
#   * failover: when this node is a follower and the leader is unreachable,
#     promote ourselves (becoming the tunnel endpoint)
#   * after ~350 minutes, snapshot + dispatch a fresh run of this workflow so a
#     new runner picks up ownership (staggered cadence prevents both runners
#     from ending at once)
#
# Env (all optional except STORAGED_EXE/BASE):
#   STORAGED_EXE, STORAGED_BASE, STORAGED_NODE_ID, STORAGED_ROLE,
#   STORAGED_ADMIN_KEY, CF_TUNNEL_TOKEN, CF_TUNNEL_ID, CF_TUNNEL_DOMAIN,
#   CLOUDFLARED_EXE, CLOUDFLARED_CONFIG, GH_TOKEN, GH_REPO, WORKFLOW_SELF
param(
    [int]$CheckSeconds   = 15,
    [int]$LeaderTimeout  = 75,   # consecutive failed leader pings before promote
    [int]$MaxUptimeMin   = 350,  # hand off the job before the 420-min timeout
    [string]$LogFile     = ""
)

if (-not $LogFile) {
    if ($env:RUNNER_WORKSPACE) { $LogFile = Join-Path $env:RUNNER_WORKSPACE "keep-alive.log" }
    else                      { $LogFile = Join-Path $env:TEMP "keep-alive.log" }
}

$ErrorActionPreference = "Continue"
function Log($m) { "$(Get-Date -Format o) $m" | Out-File -FilePath $LogFile -Append -Encoding utf8 }

$logDir       = Split-Path $LogFile
$TunnelOutLog = Join-Path $logDir "cloudflared.out.log"
$TunnelErrLog = Join-Path $logDir "cloudflared.err.log"

$exe         = $env:STORAGED_EXE
$base        = $env:STORAGED_BASE
$nodeID      = $env:STORAGED_NODE_ID
$role        = $env:STORAGED_ROLE
$adminKey    = $env:STORAGED_ADMIN_KEY

# locate the storaged binary at $GITHUB_WORKSPACE\cmd\storaged if not given
if (-not $exe) {
    $ws = $env:GITHUB_WORKSPACE
    $exe = Join-Path $ws "cmd\storaged\storaged.exe"
}
if (Test-Path "$PSScriptRoot\..\cmd\storaged\storaged.exe") {
    $exe = (Resolve-Path "$PSScriptRoot\..\cmd\storaged\storaged.exe").Path
}
$ost = Join-Path (Split-Path $exe) "ost.exe"

function Invoke-Ost([string]$args) {
    if (-not (Test-Path $ost)) { return $null }
    $env:OST_BASE = $base
    $env:OST_KEY = $adminKey
    try { return & $ost $args 2>$null } catch { return $null }
}

$startedAt = Get-Date

# 1) start storaged if needed
$proc = Get-Process storaged -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $proc) {
    Log "starting storaged from $exe"
    Start-Process -FilePath $exe -WorkingDirectory (Split-Path $exe) -WindowStyle Hidden
}
Start-Sleep -Seconds 3

# 2) cloudflared lifecycle. A follower doesn't run it until promoted; a leader
#    starts it immediately. Keep its stdout/err in a log.
$cfProc = $null
function Start-Tunnel {
    $url = if ($env:STORAGED_LISTEN_URL) { $env:STORAGED_LISTEN_URL } else { "http://127.0.0.1:5000" }
    $cfExe = $null
    if ($env:CLOUDFLARED_EXE) { $cfExe = $env:CLOUDFLARED_EXE }
    if (-not $cfExe) { $cfExe = (Get-Command cloudflared -ErrorAction SilentlyContinue).Source }
    if (-not $cfExe) { Log "cloudflared not found; tunnel skipped"; return }

    # cloudflared resolves a relative `credentials-file` against its CWD, so
    # always start it from the directory that holds the config + credentials.
    $workDir = $null
    $argList = @()
    if ($env:CF_TUNNEL_TOKEN) {
        Log "starting cloudflared (token) tunnel -> $url"
        $argList = @("tunnel","run","--token",$env:CF_TUNNEL_TOKEN)
        $workDir = if ($env:CLOUDFLARED_DIR) { $env:CLOUDFLARED_DIR } else { $PWD.Path }
    }
    elseif ($env:CLOUDFLARED_CONFIG -and (Test-Path $env:CLOUDFLARED_CONFIG)) {
        Log "starting cloudflared (named tunnel $env:CF_TUNNEL_ID) -> $url via $env:CLOUDFLARED_CONFIG"
        $argList = @("tunnel","--config",$env:CLOUDFLARED_CONFIG,"run")
        $workDir = Split-Path $env:CLOUDFLARED_CONFIG
    }
    else {
        $cfg = Join-Path $PSScriptRoot "..\conf\cloudflared.yml"
        $creds = Join-Path $PSScriptRoot "..\conf\tunnel-credentials.json"
        if ((Test-Path $cfg) -and (Test-Path $creds)) {
            Log "starting cloudflared (named tunnel via conf/) -> $url"
            $argList = @("tunnel","--config",(Resolve-Path $cfg).Path,"run")
            $workDir = Split-Path $cfg
        } else {
            Log "no tunnel config or token; tunnel skipped"
            return
        }
    }
    if (-not $workDir) { $workDir = $PWD.Path }
    $cfProc = Start-Process -FilePath $cfExe -ArgumentList $argList -WorkingDirectory $workDir -WindowStyle Hidden `
            -RedirectStandardOutput $TunnelOutLog -RedirectStandardError $TunnelErrLog
    Log "cloudflared started pid=$($cfProc.Id) cwd=$workDir"
}
function Stop-Tunnel {
    if ($cfProc) { try { Stop-Process -Id $cfProc.Id -Force -ErrorAction SilentlyContinue } catch {} ; $cfProc = $null }
}
function Tunnel-Alive {
    if (-not $cfProc) { return $false }
    if ($cfProc.HasExited) { Log "cloudflared pid=$($cfProc.Id) exited code $($cfProc.ExitCode)"; $cfProc = $null; return $false }
    return $true
}
function Show-Status {
    "--- $(Get-Date -Format HH:mm:ss) keep-alive state ---"
    if (Test-Path $LogFile) { Get-Content $LogFile -Tail 4 }
    if (Test-Path $TunnelOutLog) { Get-Content $TunnelOutLog -Tail 6 }
    if (Test-Path $TunnelErrLog) { "cloudflared err:"; Get-Content $TunnelErrLog -Tail 6 }
}
if ($role -eq "leader") { Start-Tunnel }

# 3) supervision loop
$leaderFailures = 0
$handedOff = $false
$lastStatus = Get-Date
while ($true) {
    Start-Sleep -Seconds $CheckSeconds

    # log liveness to the step console every ~60s so the Actions UI shows state
    if (((Get-Date) - $lastStatus).TotalSeconds -ge 60) {
        Show-Status
        $lastStatus = Get-Date
    }

    # ensure storaged remains up
    if (-not (Get-Process storaged -ErrorAction SilentlyContinue)) {
        Log "storaged died; restarting"
        Start-Process -FilePath $exe -WorkingDirectory (Split-Path $exe) -WindowStyle Hidden
        Start-Sleep -Seconds 5
        continue
    }

    # sanity: if we're the writer, the tunnel must be up (leader starts with it)
    $h = Invoke-Ost "health"
    $amWriter = $false
    if ($h) {
        $amWriter = ($h | Select-String '"writes"\s*:\s*true' -Quiet)
    }

    if (($role -eq "follower") -and (-not $amWriter)) {
        # failover: is the leader healthy?
        $leaderOK = $false
        try {
            $peer = if ($env:STORAGED_PEER_URL) { $env:STORAGED_PEER_URL } else { "http://127.0.0.1:5000" }
            $r = Invoke-WebRequest -Uri "$peer/v1/health" -UseBasicParsing -TimeoutSec 10
            $leaderOK = ($r.StatusCode -eq 200)
        } catch { $leaderOK = $false }

        if (-not $leaderOK) {
            $leaderFailures++
            Log "leader unreachable ($leaderFailures/$LeaderTimeout)"
            if ($leaderFailures -ge $LeaderTimeout) {
                Log "promoting $nodeID to writer"
                Invoke-Ost "promote" | Out-Null
                if (-not (Tunnel-Alive)) { Start-Tunnel }
                $leaderFailures = 0
            }
        } else {
            $leaderFailures = 0
        }
    } else {
        # we are (or should be) the writer
        if (-not (Tunnel-Alive)) { Start-Tunnel }
    }

    # 4) handoff before the 6h timeout: snapshot then re-dispatch this workflow
    $uptimeMin = ((Get-Date) - $startedAt).TotalMinutes
    if ((-not $handedOff) -and ($uptimeMin -ge $MaxUptimeMin) -and (-not $h -or $amWriter)) {
        Log "handoff after $([int]$uptimeMin) minutes"
        & (Join-Path $PSScriptRoot "artifact-snapshot.ps1") | Out-Null
        if ($env:GH_TOKEN -and $env:GH_REPO) {
            $env:GH_TOKEN_OUT = $env:GH_TOKEN
            try {
                & gh workflow run $env:WORKFLOW_SELF --repo $env:GH_REPO 2>$null
                Log "dispatched fresh run of $env:WORKFLOW_SELF"
            } catch { Log "re-dispatch failed: $_" }
        }
        Stop-Tunnel
        $handedOff = $true
        Log "keep-alive exiting; fresh run should take over"
        exit 0
    }
}