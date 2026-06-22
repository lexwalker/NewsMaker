# run_fetch_noai.ps1 -- COVERAGE CHECK only: fetch + heuristics, NO LLM, NO push.
#
# Measures how many articles each source yields (the per-source funnel in the
# new "TEST progon vN" tab) without spending a cent on the LLM. Used to validate
# source fixes (impersonate allowlist, RSS-content fallback). Writes the prog +
# articles tabs but does NOT cluster or push to the editor feed.
#
# Launch detached (survives the terminal/session):
#   Start via the one-shot Scheduled Task, or:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\run_fetch_noai.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$ts  = Get-Date -Format "yyyyMMdd_HHmmss"
if (-not (Test-Path (Join-Path $root "logs"))) { New-Item -ItemType Directory (Join-Path $root "logs") | Out-Null }
$log = Join-Path $root "logs\noai_fetch_$ts.log"

Write-Output "noai fetch start $ts  log=$log"
# Throttle (jun-21 experiment) — OFF. It was a dead end: the real cause of the
# "works isolated, 0 in run" failures was the Brotli decode bug (client asked
# for Content-Encoding: br with no decoder installed -> garbage HTML). Fixed in
# base.py + brotli dependency. Set FETCH_MIN_INTERVAL>0 only to re-test throttling.
$env:FETCH_MIN_INTERVAL = "0"
# stderr-safe (see run_prog.ps1 jun-18): with ErrorActionPreference=Stop a Python
# warning on stderr becomes a fatal NativeCommandError and kills the run. Drop to
# Continue around the call; success is decided by $LASTEXITCODE.
$prev = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& python scripts/batch_fetch_test.py --no-llm --no-playwright --no-published-dedup 2>&1 |
    Tee-Object -FilePath $log -Append
$code = $LASTEXITCODE
$ErrorActionPreference = $prev
Write-Output "noai fetch DONE exit=$code  log=$log"
