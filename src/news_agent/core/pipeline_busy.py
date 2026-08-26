"""Is the pipeline running right now?

The runs guard each other — run_hot skips when a batch or a recovery is in
flight — but ad-hoc work does not go through that guard. On aug-25 two
8-worker backfills and a multi-gigabyte install ran on top of a full run and
the machine locked up hard enough to need a power cycle.

The obvious probe is the trap: `wmic` was removed in recent Windows 11 builds
and returns an EMPTY list instead of an error, so a check built on it reports
"idle" on a busy machine — which is exactly the answer that gets a heavy job
launched. This asks PowerShell's CIM instead and treats an unreadable process
list as BUSY, because refusing to run is the recoverable mistake.
"""

from __future__ import annotations

import subprocess

# The stages that must not share a machine with a heavy ad-hoc job: the fetch
# itself, the clustering/push that follows it, and the recovery pass.
PIPELINE_MARKERS = ("batch_fetch_test", "build_news_", "retry_failed_llm")

_PS_COMMAND = (
    "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" "
    "-ErrorAction Stop | Select-Object -ExpandProperty CommandLine"
)


def _command_lines(timeout: float = 20.0) -> list[str] | None:
    """Every running python command line, or None if the list is unreadable."""
    try:
        r = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", _PS_COMMAND],
            capture_output=True, text=True, timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if r.returncode != 0:
        return None
    return [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]


def busy_stages(lines: list[str] | None = None) -> list[str]:
    """Which pipeline stages are running. Empty means the machine is free.

    An unreadable process list yields ``["<не удалось проверить>"]`` — busy —
    rather than an empty list. A probe that cannot see is not a probe that
    saw nothing.
    """
    if lines is None:
        lines = _command_lines()
    if lines is None:
        return ["<не удалось проверить>"]
    found = []
    for ln in lines:
        for m in PIPELINE_MARKERS:
            if m in ln and m not in found:
                found.append(m)
    return found


def require_idle(what: str = "эта задача") -> None:
    """Raise unless the pipeline is idle. For scripts that must not compete."""
    busy = busy_stages()
    if busy:
        raise SystemExit(
            f"ОТКАЗ: {what} не запускается — работает конвейер ({', '.join(busy)}). "
            f"Дождитесь окончания прогона."
        )
