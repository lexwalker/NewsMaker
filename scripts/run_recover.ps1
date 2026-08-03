# run_recover.ps1 -- finish an INTERRUPTED prog run without re-fetching.
#
# Scenario: stage-1 fetch+classify completed and wrote a "ТЕСТ статьи vN" tab,
# but the LLM stage failed (e.g. API 403/usage-cap mid-run). The fetched 900+
# articles are safe in the tab; this only re-runs the LLM stages:
#   1) retry_failed_llm.py  -- re-classify+translate the broken rows IN PLACE
#      (auto-picks the newest "ТЕСТ статьи vN"; no Cyrillic argv)
#   2) build_news_clusters.py --use-llm-editor  -- cluster the fixed tab
#   3) build_news_sheet.py                       -- push to "Новости (новые)"
#
# Launch via the one-shot Scheduled Task (survives session reaping), or:
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\run_recover.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Single-instance lock, SHARED with run_prog (aug-03). A recovery pass spends
# on exactly the candidates a full chain would, so the evening scheduled task
# firing over a running recovery is a double bill — the same failure run_prog
# already guards against, which recovery was simply missing.
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
    Write-Output "run_recover REFUSED: a prog/recovery chain is alive (PID $oldPid). Exit."
    exit 9
  }
}
Set-Content -Path $lockPath -Value $PID -Encoding ascii

$env:PYTHONUNBUFFERED = "1"   # real-time log; a crash point is visible immediately
$ts  = Get-Date -Format "yyyyMMdd_HHmmss"
if (-not (Test-Path (Join-Path $root "logs"))) { New-Item -ItemType Directory (Join-Path $root "logs") | Out-Null }
$log = Join-Path $root "logs\run_recover_$ts.log"

function Stage($name, $argList) {
  Write-Output "=== $name  $(Get-Date -Format HH:mm:ss) ===" | Tee-Object -FilePath $log -Append
  # stderr-safe (see run_prog.ps1): with EAP=Stop a Python stderr warning becomes
  # a fatal NativeCommandError. Drop to Continue; decide success from $LASTEXITCODE.
  $prev = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  & python @argList 2>&1 | Tee-Object -FilePath $log -Append
  $code = $LASTEXITCODE
  $ErrorActionPreference = $prev
  if ($code -ne 0) {
    Write-Output "ABORT: '$name' exited $code -- chain stopped." | Tee-Object -FilePath $log -Append
    exit $code
  }
}

Write-Output "run_recover start $ts  log=$log"
Stage "1/3 retry LLM (newest tab)" @('scripts/retry_failed_llm.py')
Stage "2/3 cluster (LLM-editor)"   @('scripts/build_news_clusters.py','--use-llm-editor')
Stage "3/3 push to editor feed"    @('scripts/build_news_sheet.py')
# Tee the finish marker INTO the log (aug-03): it used to go only to the task's
# stdout, so the log's last line was the push summary and a completed recovery
# was indistinguishable from one hung mid-push. Anything watching the file for
# completion waits forever.
Write-Output "run_recover DONE $(Get-Date -Format HH:mm:ss)  log=$log" | Tee-Object -FilePath $log -Append
Remove-Item -Path $lockPath -ErrorAction SilentlyContinue
