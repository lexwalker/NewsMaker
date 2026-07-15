"""Resumable-fetch checkpoint: survive a mid-fetch crash (e.g. a power outage)
without re-parsing every source.

The fetch phase is the run's slow, network-bound part. It accumulates
``ArticleRow`` objects in memory; a crash there loses the lot, and the restart
re-fetches all ~340 sources from scratch (jul-15: an outage killed a run at
source 173/341, ~40 min of fetching gone). This records each source's produced
rows to an append-only JSONL file the moment that source finishes, so a restart
replays the completed sources from disk and continues from the first unfetched
one.

Safety contract:
  * The checkpoint only influences the IN-MEMORY fetch loop (which sources to
    skip and which rows to restore). Sheets writes and the state-window advance
    still happen ONCE at the end of the run, exactly as without it — there is no
    mid-run external write, hence no double-write path.
  * A checkpoint is adopted ONLY when its fingerprint matches the current run:
    classifier_version + previous_run_at (the state anchor, which does NOT move
    on a crash) + a hash of the exact source list + the window parameters. If
    the editor changed the source list, or a different run advanced the state in
    between, the fingerprint differs and the checkpoint is discarded → a clean
    full fetch. Anchoring on ``previous_run_at`` (not the freshly-computed
    ``now``, which changes every launch) is what lets the resume match at all.
  * The stored window (``since``/``now`` from the crashed run) is ADOPTED on
    resume, so freshness decisions for the remaining sources match the rows
    already fetched — the resume is a faithful continuation of the same window,
    not a new one.
  * Pure module: it speaks dicts/JSON only (callers convert their dataclasses
    with ``dataclasses.asdict`` / ``Cls(**d)``), so it has no dependency on the
    batch script and is unit-testable in isolation.
  * A torn final line (crash mid-append) is skipped on load; that one source is
    simply re-fetched. Any load/append error degrades to "no checkpoint" — the
    feature can never itself break a run.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# Bump if the on-disk record shape changes incompatibly — an older-schema file
# then fails the fingerprint/version gate and is discarded rather than
# misread.
SCHEMA_VERSION = 1


def checkpoint_path(state_path: Path) -> Path:
    """Checkpoint file for a given run-state file. Derived from the state path
    so the full run (state.json) and the hot lane (state_hot.json) get distinct,
    non-colliding checkpoints."""
    state_path = Path(state_path)
    return state_path.with_name(state_path.stem + "_fetch_ckpt.jsonl")


def fingerprint(
    *,
    classifier_version: str,
    previous_run_at: datetime | None,
    urls: list[str],
    max_lookback_hours: float,
    overlap_minutes: float,
    max_articles: int,
    hot: bool,
) -> str:
    """Stable identity of a run for resume matching.

    Deliberately excludes the freshly-computed ``now`` (it changes on every
    launch) and includes ``previous_run_at`` (the state anchor, unchanged until
    a run SUCCEEDS — a crash leaves it put, so crash and resume share it). Any
    change to the source list, the window knobs, the classifier, or the lane
    (hot vs full) changes the fingerprint and forces a fresh fetch."""
    h = hashlib.sha256()
    h.update(f"v{SCHEMA_VERSION}\n".encode())
    h.update((classifier_version or "").encode())
    h.update(b"\n")
    h.update((previous_run_at.isoformat() if previous_run_at else "none").encode())
    h.update(b"\n")
    h.update(f"{max_lookback_hours}|{overlap_minutes}|{max_articles}|{int(hot)}\n".encode())
    # Source list: order matters (we resume by index), so hash it in order.
    for u in urls:
        h.update((u or "").encode())
        h.update(b"\x00")
    return h.hexdigest()[:16]


@dataclass(frozen=True)
class Header:
    fingerprint: str
    run_ts: str            # the crashed run's start timestamp (ISO)
    report_tab: str
    articles_tab: str
    since_iso: str
    now_iso: str
    using_fallback: bool
    previous_run_at_iso: str  # "" when None
    total_sources: int
    saved_at: str          # when the header was written (ISO, for age gating)


@dataclass
class Loaded:
    header: Header
    max_done_idx: int          # resume from source (max_done_idx + 1)
    done_idx: set[int]
    rows: list[dict]           # ArticleRow dicts, in original order
    results: list[dict]        # SourceResult dicts, in source order


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _atomic_write_line0(path: Path, header_obj: Header) -> None:
    """(Re)create the checkpoint with the header as line 0, atomically."""
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    rec = {"kind": "header", **header_obj.__dict__}
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)  # atomic on the same volume (incl. NTFS)


def begin(
    path: Path,
    *,
    fingerprint: str,
    run_ts: str,
    report_tab: str,
    articles_tab: str,
    since: datetime,
    now: datetime,
    using_fallback: bool,
    previous_run_at: datetime | None,
    total_sources: int,
) -> Header:
    """Start a fresh checkpoint (overwrites any prior file). Call once, right
    after the run's tabs are allocated and before the fetch loop."""
    header = Header(
        fingerprint=fingerprint,
        run_ts=run_ts,
        report_tab=report_tab,
        articles_tab=articles_tab,
        since_iso=since.isoformat(),
        now_iso=now.isoformat(),
        using_fallback=bool(using_fallback),
        previous_run_at_iso=previous_run_at.isoformat() if previous_run_at else "",
        total_sources=int(total_sources),
        saved_at=_now_iso(),
    )
    _atomic_write_line0(Path(path), header)
    return header


def append_source(
    path: Path, *, src_idx: int, source_result: dict, rows: list[dict]
) -> None:
    """Append one completed source's record. Append-only + flushed, so a crash
    mid-write can at worst tear the final line (skipped on load)."""
    rec = {"kind": "src", "src_idx": int(src_idx),
           "result": source_result, "rows": rows}
    with open(Path(path), "a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


def clear(path: Path) -> None:
    """Remove the checkpoint (call after the run's Sheets write succeeds — the
    rows are durable there, so resuming would double-write)."""
    try:
        Path(path).unlink(missing_ok=True)
    except OSError:
        pass
    # also drop a stray tmp from an interrupted header write
    try:
        Path(str(path) + ".tmp").unlink(missing_ok=True)
    except OSError:
        pass


def load(
    path: Path, expected_fingerprint: str, *, max_age_hours: float = 12.0
) -> Loaded | None:
    """Return replayable progress if a compatible checkpoint exists, else None.

    Returns None (→ fresh fetch) when: the file is missing/empty; the header is
    absent or mismatches ``expected_fingerprint``; or the header is older than
    ``max_age_hours`` (a very old checkpoint means an abandoned run whose window
    is stale). A torn final ``src`` line is skipped; other malformed lines are
    ignored defensively."""
    path = Path(path)
    if not path.exists():
        return None
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return None
    lines = [ln for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return None
    # Line 0 = header.
    try:
        hrec = json.loads(lines[0])
    except json.JSONDecodeError:
        return None
    if hrec.get("kind") != "header":
        return None
    if hrec.get("fingerprint") != expected_fingerprint:
        return None
    try:
        header = Header(
            fingerprint=hrec["fingerprint"], run_ts=hrec["run_ts"],
            report_tab=hrec["report_tab"], articles_tab=hrec["articles_tab"],
            since_iso=hrec["since_iso"], now_iso=hrec["now_iso"],
            using_fallback=bool(hrec["using_fallback"]),
            previous_run_at_iso=hrec.get("previous_run_at_iso", ""),
            total_sources=int(hrec["total_sources"]),
            saved_at=hrec.get("saved_at", ""),
        )
    except (KeyError, ValueError, TypeError):
        return None
    # Age gate.
    if header.saved_at:
        try:
            saved = datetime.fromisoformat(header.saved_at)
            age_h = (datetime.now(timezone.utc) - saved).total_seconds() / 3600.0
            if age_h > max_age_hours:
                return None
        except ValueError:
            pass
    # Replay source records.
    done_idx: set[int] = set()
    rows: list[dict] = []
    results: list[dict] = []
    for ln in lines[1:]:
        try:
            rec = json.loads(ln)
        except json.JSONDecodeError:
            # Torn final line from a crash mid-append — stop; that source and
            # everything after it will be re-fetched.
            break
        if rec.get("kind") != "src":
            continue
        idx = rec.get("src_idx")
        if not isinstance(idx, int) or idx in done_idx:
            continue
        done_idx.add(idx)
        rr = rec.get("rows")
        if isinstance(rr, list):
            rows.extend(rr)
        res = rec.get("result")
        if isinstance(res, dict):
            results.append(res)
    if not done_idx:
        return None
    return Loaded(
        header=header,
        max_done_idx=max(done_idx),
        done_idx=done_idx,
        rows=rows,
        results=results,
    )
