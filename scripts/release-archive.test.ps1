# release-archive.test.ps1 — regression test for the failed-upload retry path.
#
# Proves that when `gh release upload` fails (rate limit, 5xx, network blip):
#   1. the flush does NOT mark the bucket as archived (state records failed=true)
#   2. the staged part is kept on disk
#   3. the NEXT flush retries the same parts and marks the bucket archived
#      only after a successful upload
#
# The archive script runs twice against a fake `gh` placed first on PATH whose
# upload subcommand fails while a flag file exists. No network, no real repo.
#
# Run:  powershell -NoProfile -ExecutionPolicy Bypass -File scripts\release-archive.test.ps1
# Exit: 0 = all assertions passed, 1 = at least one failed.

$ErrorActionPreference = "Stop"

$script:failures = 0
function Assert-True([bool]$cond, [string]$msg) {
    if ($cond) { Write-Host "  ok: $msg" }
    else { Write-Host "  FAIL: $msg"; $script:failures++ }
}

$archive = Join-Path $PSScriptRoot "release-archive.ps1"
if (-not (Test-Path $archive)) { Write-Host "FAIL: $archive not found"; exit 1 }

$root = Join-Path ([System.IO.Path]::GetTempPath()) ("ra-test-" + [guid]::NewGuid().ToString("N").Substring(0, 8))
New-Item -ItemType Directory -Force -Path $root | Out-Null

$origPath = $env:PATH
$origRepo = $env:GH_REPO
$origTok  = $env:GH_TOKEN
$origTmp  = $env:RA_TEST_TMP

try {
    # ---- fake gh: view always succeeds with an empty asset list; upload
    #      appends the asset name to upload-log.txt and fails while the
    #      fail-uploads flag file exists.
    $mockDir    = Join-Path $root "mockbin"
    $mockScript = Join-Path $mockDir "gh-mock.ps1"
    New-Item -ItemType Directory -Force -Path $mockDir | Out-Null
    @'
$ErrorActionPreference = "Stop"
$sub = ""
if ($args.Count -gt 1) { $sub = $args[1] }
if ($args[0] -eq "release") {
    if ($sub -eq "view")   { Write-Output '{"assets":[]}'; exit 0 }
    if ($sub -eq "create") { exit 0 }
    if ($sub -eq "upload") {
        $name = Split-Path -Leaf $args[3]
        Add-Content -Path (Join-Path $env:RA_TEST_TMP "upload-log.txt") -Value $name
        if (Test-Path (Join-Path $env:RA_TEST_TMP "fail-uploads")) {
            Write-Output "gh: simulated upload failure (rate limited)"
            exit 1
        }
        exit 0
    }
    exit 0
}
if ($args[0] -eq "api") { exit 0 }
exit 1
'@ | Out-File -FilePath $mockScript -Encoding utf8

    $ghCmd = Join-Path $mockDir "gh.cmd"
    ("@echo off`r`npowershell -NoProfile -ExecutionPolicy Bypass -File `"%~dp0gh-mock.ps1`" %*`r`nexit /b %ERRORLEVEL%`r`n") |
        Out-File -FilePath $ghCmd -Encoding ascii

    # ---- fake storage: two 64-hex-char blobs in two buckets (aa, bb)
    $dataDir  = Join-Path $root "storage"
    $blobsDir = Join-Path $dataDir "blobs"
    New-Item -ItemType Directory -Force -Path (Join-Path $blobsDir "aa"), (Join-Path $blobsDir "bb") | Out-Null
    $payload = New-Object byte[] 4096
    (New-Object Random 42).NextBytes($payload)
    [System.IO.File]::WriteAllBytes((Join-Path $blobsDir ("aa\" + ("a" * 64))), $payload)
    [System.IO.File]::WriteAllBytes((Join-Path $blobsDir ("bb\" + ("b" * 64))), $payload)

    $staging   = Join-Path $root "staging"
    $stateFile = Join-Path $root "state.json"
    $uploadLog = Join-Path $root "upload-log.txt"
    $failFlag  = Join-Path $root "fail-uploads"
    New-Item -ItemType File -Path $failFlag -Force | Out-Null

    $env:GH_REPO = "mock/repo"
    $env:GH_TOKEN = "mock-token"
    $env:RA_TEST_TMP = $root
    # the archive script probes $env:GITHUB_WORKSPACE for the ost binary; give
    # it a benign value so no null-arg noise comes back over stderr
    $env:GITHUB_WORKSPACE = $root
    $env:PATH = "$mockDir;$origPath"

    function Invoke-Flush {
        # capture the child's full output to a file: piping stderr through the
        # parent turns harmless child stderr lines into error records, which
        # would trip this test's Stop preference.
        $log = Join-Path $root "flush-log.txt"
        & powershell -NoProfile -ExecutionPolicy Bypass -File $archive `
            -Once -DataDir $dataDir -StagingDir $staging -StateFile $stateFile *> $log
        ,@($LASTEXITCODE, (Get-Content $log -Raw))
    }
    function Get-UploadLog {
        if (Test-Path $uploadLog) { @(Get-Content $uploadLog) } else { @() }
    }

    # ================= RUN 1: uploads fail =================
    Write-Host "run 1: gh release upload fails"
    $r1 = Invoke-Flush
    $code1 = $r1[0]
    $out1 = $r1[1]
    Assert-True ($code1 -eq 0) "flush exits 0 even when uploads fail (durability must not kill the node)"
    Assert-True ($out1 -match "UPLOAD FAILED") "failed upload is reported (UPLOAD FAILED)"

    $st1 = Get-Content $stateFile -Raw | ConvertFrom-Json
    Assert-True ($st1.aa.failed -eq $true)  "bucket aa marked failed in state"
    Assert-True ($st1.bb.failed -eq $true)  "bucket bb marked failed in state"
    Assert-True (@($st1.aa.parts).Count -eq 0) "bucket aa has no archived parts"
    Assert-True (@($st1.bb.parts).Count -eq 0) "bucket bb has no archived parts"

    $kept = @(Get-ChildItem $staging -Filter "part-*.zip" -ErrorAction SilentlyContinue)
    Assert-True ($kept.Count -eq 2) "both staged parts were kept on disk after the failed upload (got $($kept.Count))"

    $log1 = Get-UploadLog
    Assert-True ($log1.Count -eq 2) "both parts were upload-attempted once (got $($log1.Count))"

    # ================= RUN 2: uploads succeed =================
    Remove-Item $failFlag -Force
    Write-Host "run 2: gh release upload succeeds (retry)"
    $r2 = Invoke-Flush
    $code2 = $r2[0]
    $out2 = $r2[1]
    Assert-True ($code2 -eq 0) "retry flush exits 0"
    Assert-True ($out2 -notmatch "UPLOAD FAILED") "retry flush has no upload failures"
    Assert-True ($out2 -match "uploading part-")  "retry flush re-attempted the parts"

    $st2 = Get-Content $stateFile -Raw | ConvertFrom-Json
    Assert-True ($st2.aa.failed -eq $false) "bucket aa no longer marked failed"
    Assert-True ($st2.bb.failed -eq $false) "bucket bb no longer marked failed"
    Assert-True (@($st2.aa.parts).Count -eq 1) "bucket aa recorded its archived part"
    Assert-True (@($st2.bb.parts).Count -eq 1) "bucket bb recorded its archived part"

    $log2 = Get-UploadLog
    Assert-True ($log2.Count -eq ($log1.Count * 2)) "each part was retried exactly once (log $($log1.Count) -> $($log2.Count))"
    $first = @($log1 | Sort-Object)
    $again = @($log2 | Select-Object -Skip $log1.Count | Sort-Object)
    $same = (@(Compare-Object -ReferenceObject $first -DifferenceObject $again).Count -eq 0)
    if (-not $same) {
        Write-Host "    first attempt: $($first -join ', ')"
        Write-Host "    retried as:    $($again -join ', ')"
    }
    Assert-True $same "the retried parts are exactly the ones that failed (same content-hash names)"
}
finally {
    $env:PATH = $origPath
    $env:GH_REPO = $origRepo
    $env:GH_TOKEN = $origTok
    if ($null -eq $origTmp) { Remove-Item Env:\RA_TEST_TMP -ErrorAction SilentlyContinue }
    else { $env:RA_TEST_TMP = $origTmp }
    Remove-Item -Recurse -Force $root -ErrorAction SilentlyContinue
}

if ($script:failures -gt 0) {
    Write-Host ""
    Write-Host "release-archive retry regression: FAILED ($($script:failures) assertion(s))"
    exit 1
}
Write-Host ""
Write-Host "release-archive retry regression: OK"
exit 0
