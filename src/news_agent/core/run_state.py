"""Persistent ``last_run_at`` state for scheduled runs.

The pipeline runs three times a day on a schedule. Each run should fetch
articles published since the previous run finished — minus a small overlap
(20 minutes by default) so a publication that crosses the boundary doesn't
slip through.

State lives in a tiny JSON file (``data/state.json``) — no database, no
network. The shape is intentionally minimal:

    {
      "last_run_at":     "2026-04-28T13:00:42+00:00",  # UTC ISO-8601
      "last_run_status": "ok",                          # ok | failed
      "last_run_window_start": "2026-04-28T08:40:00+00:00",
      "last_run_articles":  87,
      "last_run_cost_usd":  0.41
    }

The file is rewritten in full on every successful run. We never delete it
during a failure — the next run will see the previous successful timestamp
and pick up from there, so a single crashed run does not lose news.

If the file is missing (first run ever, or the user wiped it) or older
than ``max_lookback_hours``, the window falls back to ``now − max_lookback``.
That ceiling protects against a months-long backlog if the bot was paused.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RunWindow:
    """Time window of publication dates we want to ingest in the next run."""

    since: datetime          # inclusive lower bound (UTC)
    now: datetime            # the moment the run started (UTC)
    using_fallback: bool     # True iff we hit max_lookback ceiling
    previous_run_at: datetime | None  # what we read from state, if any


def _parse_iso(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class RunState:
    """File-backed state for the scheduled pipeline.

    Use ``load`` / ``save`` for plain reads and writes; use ``compute_window``
    to derive the next run's fetch window honouring the overlap and fallback
    rules.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    # ------------------------------------------------------------------ I/O
    def load(self) -> dict[str, Any]:
        """Return the parsed JSON, or an empty dict if missing / corrupt."""
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}

    def save(self, data: dict[str, Any]) -> None:
        """Atomically rewrite the state file (temp + rename)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(data, indent=2, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        tmp.replace(self.path)

    def update_success(
        self,
        *,
        run_at: datetime,
        window_start: datetime,
        articles: int,
        cost_usd: float,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Persist a successful-run snapshot.

        ``extra`` — additional flat fields merged into the snapshot (e.g.
        health-check baselines like archive index sizes, or the articles-tab
        name for an explicit stage handoff). Core fields always win on key
        collision."""
        run_at_utc = run_at.astimezone(timezone.utc) if run_at.tzinfo else run_at.replace(tzinfo=timezone.utc)
        win_utc = window_start.astimezone(timezone.utc) if window_start.tzinfo else window_start.replace(tzinfo=timezone.utc)
        payload = dict(extra or {})
        payload.update({
            "last_run_at": run_at_utc.isoformat(timespec="seconds"),
            "last_run_status": "ok",
            "last_run_window_start": win_utc.isoformat(timespec="seconds"),
            "last_run_articles": int(articles),
            "last_run_cost_usd": round(float(cost_usd), 4),
        })
        self.save(payload)

    # ------------------------------------------------------ window strategy
    def compute_window(
        self,
        *,
        overlap_minutes: int,
        max_lookback_hours: int,
        now: datetime | None = None,
        peer: "RunState | None" = None,
        peer_lag_hours: float = 3.0,
    ) -> RunWindow:
        """Return the time window the next run should fetch.

        Rules:
          • normal case: window starts at ``last_run_at − overlap_minutes``
          • first run / no state: window starts at ``now − max_lookback_hours``
          • previous run too old: clamp to ``now − max_lookback_hours``

        ``peer`` lets one lane learn that ANOTHER lane already covered these
        sources. The hot lane fetches 24 sources the full lane also fetches,
        but each anchored only on its own state — so on aug-04 a hot run
        launched 21 minutes after a healthy full run still opened a 20-hour
        window, paid for 149 candidates, and delivered ONE new row. Everything
        else was already on the sheet.

        The peer anchor is deliberately set back by ``peer_lag_hours``. A full
        run's ``last_run_at`` is when it FINISHED, and its fetch takes ~2h: a
        source polled at 04:30 cannot carry a story published at 05:00, so
        anchoring straight at the finish time would open a gap neither lane
        ever covers. Backing off by more than the observed fetch duration
        keeps the overlap that closes it, and still cuts the window from 20
        hours to about 3.

        Only a HEALTHY peer run counts — a degraded one may have stopped
        mid-fetch with most sources untouched, and trusting it would silently
        skip whatever it never reached.
        """
        if overlap_minutes < 0:
            raise ValueError(f"overlap_minutes must be >= 0, got {overlap_minutes}")
        if max_lookback_hours <= 0:
            raise ValueError(f"max_lookback_hours must be > 0, got {max_lookback_hours}")
        if peer_lag_hours < 0:
            raise ValueError(f"peer_lag_hours must be >= 0, got {peer_lag_hours}")

        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        ceiling = now_utc - timedelta(hours=max_lookback_hours)

        previous = _parse_iso(self.load().get("last_run_at"))
        peer_anchor = self._peer_anchor(peer, peer_lag_hours)
        if peer_anchor is not None and (previous is None or peer_anchor > previous):
            previous = peer_anchor
        if previous is None:
            return RunWindow(
                since=ceiling,
                now=now_utc,
                using_fallback=True,
                previous_run_at=None,
            )

        candidate = previous - timedelta(minutes=overlap_minutes)
        if candidate < ceiling:
            # Last run is too far in the past — clamp to the lookback ceiling
            # so we don't try to fetch a months-long backlog.
            return RunWindow(
                since=ceiling,
                now=now_utc,
                using_fallback=True,
                previous_run_at=previous,
            )
        return RunWindow(
            since=candidate,
            now=now_utc,
            using_fallback=False,
            previous_run_at=previous,
        )

    @staticmethod
    def _peer_anchor(peer: "RunState | None", lag_hours: float) -> datetime | None:
        """The other lane's coverage point, or None if it cannot be trusted."""
        if peer is None:
            return None
        try:
            data = peer.load()
        except Exception:  # noqa: BLE001 — a peer we cannot read is just absent
            return None
        if str(data.get("last_run_status") or "").strip().lower() != "ok":
            return None
        finished = _parse_iso(data.get("last_run_at"))
        if finished is None:
            return None
        return finished - timedelta(hours=lag_hours)
