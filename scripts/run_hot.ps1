# run_hot.ps1 -- HOT lane: poll the ~24 fast-rotating sources
# (config/hot_sources.yaml) every few hours between the two daily FULL runs.
# Whole chain takes ~5-10 min and costs cents: the cross-lane SQLite cache
# already holds everything the full runs classified, so the LLM only sees
# genuinely-new items.
#
# Isolation from the full chain (do not remove):
#   * RUN_STATE_PATH=data\state_hot.json  -- own run-window AND own
#     articles-tab handoff pointer; the full chain's state.json is never
#     touched, so neither lane shrinks the other's window or steals its tab.
#   * batch --hot writes its own tab family "ТЕСТ статьи (гор) vN".
#   * MUTEX: skips silently when any batch_fetch_test.py is already running
#     (the full run has priority; the next hot tick catches up).
#
# Scheduled via the 'NewsMakerHot' task (every 4h). Manual run:
#   powershell -ExecutionPolicy Bypass -File scripts\run_hot.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:PYTHONUNBUFFERED = "1"
$env:RUN_STATE_PATH = Join-Path $root "data\state_hot.json"

# MUTEX: another batch run (full or hot) in progress -> skip this tick.
# retry_failed_llm belongs in this list (aug-03): a recovery pass classifies
# the SAME candidates a hot tick would, and the hot lane loads its SQLite cache
# map once at startup, so verdicts landing mid-tick are invisible to it and the
# overlap is paid for TWICE — the jul-27 ~$1.9 double-pay, one stage over.
$busy = Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -like "*batch_fetch_test*" -or
                       $_.CommandLine -like "*build_news_*" -or
                       $_.CommandLine -like "*retry_failed_llm*" }
if ($busy) {
  Write-Output "HOT skip: another prog run is active (PID $($busy[0].ProcessId))."
  exit 0
}

$ts  = Get-Date -Format "yyyyMMdd_HHmmss"
if (-not (Test-Path (Join-Path $root "logs"))) { New-Item -ItemType Directory (Join-Path $root "logs") | Out-Null }
$log = Join-Path $root "logs\run_hot_$ts.log"

function Stage($name, $argList) {
  Write-Output "=== $name  $(Get-Date -Format HH:mm:ss) ===" | Tee-Object -FilePath $log -Append
  # Same stderr rationale as run_prog.ps1: python warnings on stderr must not
  # become terminating NativeCommandErrors under EAP=Stop.
  $prevEAP = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  & python @argList 2>&1 | Tee-Object -FilePath $log -Append
  $code = $LASTEXITCODE
  $ErrorActionPreference = $prevEAP
  if ($code -ne 0) {
    Write-Output "ABORT: '$name' exited $code -- hot chain stopped." | Tee-Object -FilePath $log -Append
    exit $code
  }
}

Write-Output "run_hot start $ts  log=$log"
Stage "1/3 hot fetch+classify"    @('scripts/batch_fetch_test.py','--hot')
Stage "2/3 cluster (LLM-editor)"  @('scripts/build_news_clusters.py','--use-llm-editor')
Stage "3/3 push to editor feed"   @('scripts/build_news_sheet.py')

# 4/4 - feed the labelling loop from THIS lane too.
#
# The push already sends its dup suspicions to the review tab, so the tab did
# look alive from a hot run - but the REJECT sample was wired into run_prog
# only, and the hot lane rejects ~300 rows a run it never asked about. Half
# the batch size of the full lane: three hot runs a day plus the full one lands
# ~30 rows on the editor, which he can actually answer.
#
# Non-fatal, same as in run_prog: diagnostics must never abort a delivery that
# already succeeded. ASCII only - PowerShell 5.1 reads a BOM-less .ps1 as ANSI.
Write-Output "=== 4/4 sample rejects for labelling  $(Get-Date -Format HH:mm:ss) ===" | Tee-Object -FilePath $log -Append
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
# aug-29: hot lane no longer SAMPLES (compact ritual, ~10 rows/day come from
# the full lane alone) but still INGESTS - colour answers land any time of day.
& python scripts/ingest_rejected_labels.py --write 2>&1 | Tee-Object -FilePath $log -Append
if ($LASTEXITCODE -ne 0) {
  Write-Output "  (label ingest failed, exit $LASTEXITCODE - delivery unaffected)" | Tee-Object -FilePath $log -Append
}
$ErrorActionPreference = $prevEAP

# The two housekeeping steps below MUST run with 'Continue', for the same
# reason Stage() does and the labelling block above does: with 'Stop',
# PowerShell 5.1 turns any native-command stderr line into a TERMINATING
# NativeCommandError. Both write to stderr in normal operation.
#
# Added aug-13 outside this guard, and the aug-14 10:16 run showed exactly
# what that costs: the log stops mid-sync, the tab cleanup never ran, the DONE
# marker was never written, the task returned 1, and an orphaned shell was left
# behind. Delivery was already complete by then -- 903 articles, 9 stories
# pushed -- so nothing was lost except the housekeeping and any way to tell
# from the log that the run had finished. This is the jun-18 defect, third
# occurrence, and the first one I introduced myself.
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = 'Continue'

# Pull the editor's comments off the feed -- see the note in run_prog.ps1.
# Idempotent by comment hash, so running it every tick only picks up what is
# new. Non-fatal: it feeds measurement, not delivery.
& python scripts/sync_editor_feedback.py 2>&1 | Tee-Object -FilePath $log -Append
if ($LASTEXITCODE -ne 0) {
  Write-Output "  (editor-feedback sync failed, exit $LASTEXITCODE - delivery unaffected)" | Tee-Object -FilePath $log -Append
}

# Reclaim the workbook -- see the long note in run_prog.ps1. The hot lane is
# the one that actually hit the 10M-cell ceiling on aug-07 (exit 1 at tab
# allocation, no news for an hour), and it allocates a pair every tick, so it
# is the lane that most needs this. Last step, non-fatal.
& python scripts/cleanup_old_tabs.py --keep 20 --apply 2>&1 | Tee-Object -FilePath $log -Append
if ($LASTEXITCODE -ne 0) {
  Write-Output "  (tab cleanup failed, exit $LASTEXITCODE - delivery unaffected)" | Tee-Object -FilePath $log -Append
}

$ErrorActionPreference = $prevEAP

# Tee the finish marker INTO the log (aug-04). Third copy of the same defect
# (run_recover, run_prog, here): it went to the task's stdout only, so the log
# ended on the push summary and a finished run was indistinguishable from one
# wedged mid-push — anything watching the file waits forever.
Write-Output "run_hot DONE $(Get-Date -Format HH:mm:ss)  log=$log" | Tee-Object -FilePath $log -Append
