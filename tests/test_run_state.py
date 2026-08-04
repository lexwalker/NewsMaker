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


# --- peer-lane anchoring (aug-04) -----------------------------------------
# The hot lane fetches 24 sources the full lane also covers, but each anchored
# only on its own state: a hot run 21 minutes after a healthy full run opened a
# 20-hour window, paid for 149 candidates and delivered ONE new row.

def _state(tmp_path, name, **fields):
    p = tmp_path / name
    p.write_text(json.dumps(fields), encoding="utf-8")
    return RunState(p)


def test_healthy_peer_pulls_the_window_forward(tmp_path) -> None:
    own = _state(tmp_path, "hot.json", last_run_at="2026-08-03T15:00:00+00:00",
                 last_run_status="ok")
    peer = _state(tmp_path, "main.json", last_run_at="2026-08-04T06:00:00+00:00",
                  last_run_status="ok")
    w = own.compute_window(
        overlap_minutes=0, max_lookback_hours=48, peer=peer, peer_lag_hours=3,
        now=datetime(2026, 8, 4, 8, 30, tzinfo=timezone.utc))
    # 06:00 finish − 3h lag = 03:00, far later than the lane's own 15:00 of the 3rd
    assert w.since == datetime(2026, 8, 4, 3, 0, tzinfo=timezone.utc)


def test_peer_anchor_is_set_back_by_the_lag(tmp_path) -> None:
    # A full run FINISHES at last_run_at but fetched for hours before it; a
    # source polled at 04:30 cannot carry a story published at 05:00. Anchoring
    # at the finish time would open a gap neither lane covers.
    own = _state(tmp_path, "hot.json", last_run_at="2026-08-01T00:00:00+00:00",
                 last_run_status="ok")
    peer = _state(tmp_path, "main.json", last_run_at="2026-08-04T06:00:00+00:00",
                  last_run_status="ok")
    w = own.compute_window(
        overlap_minutes=0, max_lookback_hours=200, peer=peer, peer_lag_hours=3,
        now=datetime(2026, 8, 4, 8, 0, tzinfo=timezone.utc))
    assert w.since < datetime(2026, 8, 4, 6, 0, tzinfo=timezone.utc)


def test_degraded_peer_is_ignored(tmp_path) -> None:
    # A degraded run may have died mid-fetch with most sources untouched.
    own = _state(tmp_path, "hot.json", last_run_at="2026-08-03T15:00:00+00:00",
                 last_run_status="ok")
    peer = _state(tmp_path, "main.json", last_run_at="2026-08-04T06:00:00+00:00",
                  last_run_status="degraded: LLM pass aborted")
    w = own.compute_window(
        overlap_minutes=0, max_lookback_hours=48, peer=peer, peer_lag_hours=3,
        now=datetime(2026, 8, 4, 8, 30, tzinfo=timezone.utc))
    assert w.since == datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)


def test_older_peer_never_narrows_the_window(tmp_path) -> None:
    own = _state(tmp_path, "hot.json", last_run_at="2026-08-04T07:00:00+00:00",
                 last_run_status="ok")
    peer = _state(tmp_path, "main.json", last_run_at="2026-08-04T06:00:00+00:00",
                  last_run_status="ok")
    w = own.compute_window(
        overlap_minutes=0, max_lookback_hours=48, peer=peer, peer_lag_hours=3,
        now=datetime(2026, 8, 4, 8, 30, tzinfo=timezone.utc))
    assert w.since == datetime(2026, 8, 4, 7, 0, tzinfo=timezone.utc)


def test_missing_peer_file_is_harmless(tmp_path) -> None:
    own = _state(tmp_path, "hot.json", last_run_at="2026-08-04T07:00:00+00:00",
                 last_run_status="ok")
    w = own.compute_window(
        overlap_minutes=0, max_lookback_hours=48,
        peer=RunState(tmp_path / "nope.json"),
        now=datetime(2026, 8, 4, 8, 30, tzinfo=timezone.utc))
    assert w.since == datetime(2026, 8, 4, 7, 0, tzinfo=timezone.utc)
