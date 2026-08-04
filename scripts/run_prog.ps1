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

# Single-instance lock (jul-26: THIRD double-run incident -- a WMI-queued
# zombie launch, then the evening scheduled task firing over a manual
# catch-up run. Two parallel chains double the LLM bill and race the tabs).
# Lock = PID file; stale lock (dead PID) is taken over silently.
$lockPath = Join-Path $root "data\run_prog.lock"
if (Test-Path $lockPath) {
  $oldPid = (Get-Content $lockPath -ErrorAction SilentlyContinue | Select-Object -First 1)
  $alive = $false
  if ($oldPid -match '^\d+$') {
    try {
      $p = Get-Process -Id ([int]$oldPid) -ErrorAction Stop
      if ($p.ProcessName -match 'powershell') { $alive = $true }
    } catch {}
  }
  if ($alive) {
    Write-Output "run_prog REFUSED: another run_prog chain is alive (PID $oldPid, lock $lockPath). Exit."
    exit 9
  }
}
Set-Content -Path $lockPath -Value $PID -Encoding ascii

# Yield to a running HOT lane batch (jul-27: hot started 22:00, a manual prog
# 22:04 -- the prog loads its SQLite cache map ONCE at startup, so hot's
# classifications landing at 22:20 were invisible and ~250 candidates were
# paid for TWICE, ~$1.9. Hot already yields to us via its own mutex; this is
# the missing opposite direction). Wait up to 40 min, checking each minute.
for ($w = 0; $w -lt 40; $w++) {
  $hotBusy = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -like "*batch_fetch_test*" }
  if (-not $hotBusy) { break }
  if ($w -eq 0) { Write-Output "run_prog: hot-lane batch is running -- waiting for it to finish (cache reuse)." }
  Start-Sleep -Seconds 60
}
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
    Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
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
# Stage 1 with LLM-abort auto-recovery (jul-10). A transient API 403 used to
# abort the whole chain and cost a ~1.5h full re-fetch on relaunch. When the
# ONLY problem was the aborted LLM pass, batch_fetch_test writes
# data\llm_abort_recovery.json (the fetch itself completed) -- then
# retry_failed_llm finishes the classification on the SAME tab in ~20 min,
# advances the state itself, and the chain continues. Any other exit-3 cause
# (archive floors, stuck rows) has no hint file -> chain aborts as before.
Write-Output "=== 1/3 fetch+classify  $(Get-Date -Format HH:mm:ss) ===" | Tee-Object -FilePath $log -Append
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
& python scripts/batch_fetch_test.py 2>&1 | Tee-Object -FilePath $log -Append
$code = $LASTEXITCODE
$ErrorActionPreference = $prevEAP
if ($code -eq 3 -and (Test-Path (Join-Path $root "data\llm_abort_recovery.json"))) {
  Write-Output "LLM pass aborted mid-run -- auto-recovering via retry_failed_llm (no re-fetch)." | Tee-Object -FilePath $log -Append
  Stage "1R/3 retry LLM (recovery)" @('scripts/retry_failed_llm.py')
} elseif ($code -ne 0) {
  Write-Output "ABORT: '1/3 fetch+classify' exited $code -- chain stopped (nothing downstream ran)." | Tee-Object -FilePath $log -Append
  Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
  exit $code
}
Stage "2/3 cluster (LLM-editor)"  @('scripts/build_news_clusters.py','--use-llm-editor')
if ($NoPush) {
  Write-Output "STOP before push (-NoPush). Review clusters, then run: python scripts\build_news_sheet.py" | Tee-Object -FilePath $log -Append
} else {
  Stage "3/3 push to editor feed" @('scripts/build_news_sheet.py')

  # 4/4 - sample what we REJECTED and put it in front of the editor.
  #
  # We hold 1263 labelled duplicate suspicions and 52 labelled rejections,
  # because this ritual was only ever run by hand. So we have been measuring
  # the side of the funnel that loses ~20 rows a run in fine detail, and the
  # side that loses ~190 almost not at all. On the 52 we do have, 23% of the
  # rejections were wrong (off_topic alone: 43%) - a rate that, applied to the
  # real volume, dwarfs everything dedup could win back.
  #
  # Deliberately NON-FATAL: this is diagnostics, and a failure here must never
  # abort a chain whose delivery has already succeeded. Small batch - the
  # editor answers these by hand, and an unanswerable pile teaches nothing.
  #
  # ASCII only in this file on purpose: PowerShell 5.1 reads a BOM-less .ps1 as
  # ANSI, and Cyrillic here corrupted string terminators into a parse error.
  Write-Output "=== 4/4 sample rejects for labelling  $(Get-Date -Format HH:mm:ss) ===" | Tee-Object -FilePath $log -Append
  $prevEAP = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  & python scripts/sample_rejected.py --total 12 --days 2 2>&1 | Tee-Object -FilePath $log -Append
  if ($LASTEXITCODE -ne 0) {
    Write-Output "  (reject sampling failed, exit $LASTEXITCODE - delivery unaffected)" | Tee-Object -FilePath $log -Append
  }

  # ...and collect the answers. Sampling without ingesting is a loop with the
  # return wire cut: this ran last on 2026-06-18, and by aug-04 the editor had
  # answered 251 rows nobody read - 45 of them recall holes (Hongqi Tiangong
  # 08, the electric Cayenne) that never became precedents. Idempotent by
  # url_hash, so running it every push only ever picks up what is new.
  & python scripts/ingest_rejected_labels.py --write 2>&1 | Tee-Object -FilePath $log -Append
  if ($LASTEXITCODE -ne 0) {
    Write-Output "  (label ingest failed, exit $LASTEXITCODE - delivery unaffected)" | Tee-Object -FilePath $log -Append
  }
  $ErrorActionPreference = $prevEAP
}
Remove-Item $lockPath -Force -ErrorAction SilentlyContinue
# Tee the finish marker INTO the log (aug-04, same defect as run_recover.ps1):
# it used to go only to the task's stdout, so the log ended on the push summary
# and a finished run was indistinguishable from one wedged mid-push.
Write-Output "run_prog DONE $(Get-Date -Format HH:mm:ss)  log=$log" | Tee-Object -FilePath $log -Append
