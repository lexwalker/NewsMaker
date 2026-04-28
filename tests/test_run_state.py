"""Tests for run_state — window computation + state file I/O."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from news_agent.core.run_state import RunState


def _now() -> datetime:
    return datetime(2026, 4, 28, 13, 0, 0, tzinfo=timezone.utc)


# ------------------------------------------------------------- window logic

def test_first_run_falls_back_to_max_lookback(tmp_path: Path) -> None:
    rs = RunState(tmp_path / "state.json")
    win = rs.compute_window(overlap_minutes=20, max_lookback_hours=24, now=_now())
    assert win.using_fallback is True
    assert win.previous_run_at is None
    assert win.since == _now() - timedelta(hours=24)


def test_normal_case_uses_last_run_minus_overlap(tmp_path: Path) -> None:
    rs = RunState(tmp_path / "state.json")
    last = _now() - timedelta(hours=4)  # well within max_lookback
    rs.save({"last_run_at": last.isoformat(timespec="seconds")})

    win = rs.compute_window(overlap_minutes=20, max_lookback_hours=24, now=_now())
    assert win.using_fallback is False
    assert win.previous_run_at == last
    assert win.since == last - timedelta(minutes=20)


def test_overlap_zero_means_strict_continuity(tmp_path: Path) -> None:
    rs = RunState(tmp_path / "state.json")
    last = _now() - timedelta(hours=2)
    rs.save({"last_run_at": last.isoformat(timespec="seconds")})

    win = rs.compute_window(overlap_minutes=0, max_lookback_hours=24, now=_now())
    assert win.since == last


def test_stale_state_clamps_to_max_lookback(tmp_path: Path) -> None:
    rs = RunState(tmp_path / "state.json")
    # Last run was a week ago — far past the 24h ceiling
    last = _now() - timedelta(days=7)
    rs.save({"last_run_at": last.isoformat(timespec="seconds")})

    win = rs.compute_window(overlap_minutes=20, max_lookback_hours=24, now=_now())
    assert win.using_fallback is True
    assert win.previous_run_at == last
    assert win.since == _now() - timedelta(hours=24)


def test_corrupt_state_treated_as_first_run(tmp_path: Path) -> None:
    p = tmp_path / "state.json"
    p.write_text("{not valid json", encoding="utf-8")
    rs = RunState(p)

    win = rs.compute_window(overlap_minutes=20, max_lookback_hours=24, now=_now())
    assert win.using_fallback is True
    assert win.previous_run_at is None


def test_z_suffix_iso_format_accepted(tmp_path: Path) -> None:
    rs = RunState(tmp_path / "state.json")
    last = _now() - timedelta(hours=1)
    # Some serialisers write "Z" instead of "+00:00"
    iso_z = last.isoformat(timespec="seconds").replace("+00:00", "Z")
    rs.save({"last_run_at": iso_z})

    win = rs.compute_window(overlap_minutes=20, max_lookback_hours=24, now=_now())
    assert win.previous_run_at == last


def test_negative_overlap_rejected(tmp_path: Path) -> None:
    rs = RunState(tmp_path / "state.json")
    with pytest.raises(ValueError):
        rs.compute_window(overlap_minutes=-1, max_lookback_hours=24, now=_now())


def test_zero_max_lookback_rejected(tmp_path: Path) -> None:
    rs = RunState(tmp_path / "state.json")
    with pytest.raises(ValueError):
        rs.compute_window(overlap_minutes=20, max_lookback_hours=0, now=_now())


# -------------------------------------------------------------- persistence

def test_update_success_writes_full_snapshot(tmp_path: Path) -> None:
    p = tmp_path / "state.json"
    rs = RunState(p)
    run_at = _now()
    win_start = run_at - timedelta(minutes=20)
    rs.update_success(run_at=run_at, window_start=win_start, articles=87, cost_usd=0.41)

    blob = json.loads(p.read_text(encoding="utf-8"))
    assert blob["last_run_status"] == "ok"
    assert blob["last_run_articles"] == 87
    assert blob["last_run_cost_usd"] == 0.41
    assert blob["last_run_at"].startswith("2026-04-28T13:00:00")
    assert blob["last_run_window_start"].startswith("2026-04-28T12:40:00")


def test_save_is_atomic_no_temp_left_behind(tmp_path: Path) -> None:
    p = tmp_path / "state.json"
    rs = RunState(p)
    rs.save({"last_run_at": _now().isoformat()})
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == [], f"Stray temp files: {leftovers}"


def test_load_missing_returns_empty(tmp_path: Path) -> None:
    rs = RunState(tmp_path / "does-not-exist.json")
    assert rs.load() == {}
