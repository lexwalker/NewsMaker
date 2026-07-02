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
$env:PYTHONUNBUFFERED = "1"   # real-time log: a crash/death point is visible immediately, not lost in a block buffer
$ts  = Get-Date -Format "yyyyMMdd_HHmmss"
if (-not (Test-Path (Join-Path $root "logs"))) { New-Item -ItemType Directory (Join-Path $root "logs") | Out-Null }
$log = Join-Path $root "logs\run_prog_$ts.log"

function Stage($name, $argList) {
  Write-Output "=== $name  $(Get-Date -Format HH:mm:ss) ===" | Tee-Object -FilePath $log -Append
  # CRITICAL (jun-18): with $ErrorActionPreference='Stop' (set above), PowerShell
  # 5.1 turns ANY native-command stderr line into a TERMINATING NativeCommandError.
  # Python writes warnings to stderr (e.g. BeautifulSoup XMLParsedAsHTMLWarning the
  # first time an XML feed is parsed -- thekoreancarblog at source 45). That warning
  # aborted the whole prog mid-fetch, BYPASSING the $LASTEXITCODE check below -- so
  # no ABORT line, no traceback: a silent death at source 45. Drop to 'Continue' for
  # the python call so stderr is merely logged; decide success from $LASTEXITCODE.
  $prevEAP = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  & python @argList 2>&1 | Tee-Object -FilePath $log -Append
  $code = $LASTEXITCODE
  $ErrorActionPreference = $prevEAP
  if ($code -ne 0) {
    Write-Output "ABORT: '$name' exited $code -- chain stopped (nothing downstream ran)." | Tee-Object -FilePath $log -Append
    exit $code
  }
}

Write-Output "run_prog start $ts  (NoPush=$NoPush)  log=$log"
# Playwright RE-ENABLED (jul-02): the per-source wall-clock budget
# (SOURCE_BUDGET_S, default 90s) now bounds ANY slow source -- once spent,
# remaining links are skipped and the truncation is recorded in the source's
# error column, so slow sources SURFACE in the report instead of hiding.
# That was the precondition for turning the browser back on: --no-playwright
# had silently zero-yielded the 5 playwright_domains every scheduled run
# (the jun-18 "26 min stall" was an unbounded per-article fallback chain,
# not Playwright itself).
Stage "1/3 fetch+classify"        @('scripts/batch_fetch_test.py')
Stage "2/3 cluster (LLM-editor)"  @('scripts/build_news_clusters.py','--use-llm-editor')
if ($NoPush) {
  Write-Output "STOP before push (-NoPush). Review clusters, then run: python scripts\build_news_sheet.py" | Tee-Object -FilePath $log -Append
} else {
  Stage "3/3 push to editor feed" @('scripts/build_news_sheet.py')
}
Write-Output "run_prog DONE $(Get-Date -Format HH:mm:ss)  log=$log"
