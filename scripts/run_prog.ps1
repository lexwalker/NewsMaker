# run_prog.ps1 -- one-command daily run: fetch+classify -> cluster -> push.
#
# The cluster/push steps auto-pick the newest "TEST articles vN" tab, so no
# tab name is passed (also dodges the Cyrillic-argv pitfall). Each stage is
# logged and the chain ABORTS if a stage fails, so a broken fetch never pushes.
#
# Manual launch (recommended -- detached, survives closing the terminal; the
# fetch takes ~1.5-2h):
#   Start-Process powershell -WindowStyle Hidden -ArgumentList '-ExecutionPolicy','Bypass','-File','scripts\run_prog.ps1'
# Or foreground:                 powershell -File scripts\run_prog.ps1
# Stop before the editor feed (review first):  powershell -File scripts\run_prog.ps1 -NoPush
#
# Coverage tip: with the 48h look-back, running this DAILY means no day's
# publications fall outside a fetch window (the #1 coverage lever).

param([switch]$NoPush)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot          # repo root (scripts/..)
Set-Location $root
$ts  = Get-Date -Format "yyyyMMdd_HHmmss"
if (-not (Test-Path (Join-Path $root "logs"))) { New-Item -ItemType Directory (Join-Path $root "logs") | Out-Null }
$log = Join-Path $root "logs\run_prog_$ts.log"

function Stage($name, $argList) {
  Write-Output "=== $name  $(Get-Date -Format HH:mm:ss) ===" | Tee-Object -FilePath $log -Append
  & python @argList 2>&1 | Tee-Object -FilePath $log -Append
  if ($LASTEXITCODE -ne 0) {
    Write-Output "ABORT: '$name' exited $LASTEXITCODE -- chain stopped (nothing downstream ran)." | Tee-Object -FilePath $log -Append
    exit $LASTEXITCODE
  }
}

Write-Output "run_prog start $ts  (NoPush=$NoPush)  log=$log"
# --no-playwright: this unattended run must never hang. Playwright (headless
# Chromium) has no hard per-navigation timeout, so a single bad article page
# can stall the whole prog (jun-18: hung 26 min on a thekoreancarblog article
# after that domain became a live impersonate source). httpx + curl_cffi
# (impersonate) cover the vast majority; only a few JS-only SPA sites are skipped.
Stage "1/3 fetch+classify"        @('scripts/batch_fetch_test.py','--no-playwright')
Stage "2/3 cluster (LLM-editor)"  @('scripts/build_news_clusters.py','--use-llm-editor')
if ($NoPush) {
  Write-Output "STOP before push (-NoPush). Review clusters, then run: python scripts\build_news_sheet.py" | Tee-Object -FilePath $log -Append
} else {
  Stage "3/3 push to editor feed" @('scripts/build_news_sheet.py')
}
Write-Output "run_prog DONE $(Get-Date -Format HH:mm:ss)  log=$log"
