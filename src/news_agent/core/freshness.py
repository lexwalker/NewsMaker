"""Freshness filter — pure function over (published_at, now, hours)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone


def is_fresh(published_at: datetime | None, *, hours: int, now: datetime | None = None) -> bool:
    """True iff article should be considered for further processing.

    Articles without a detectable publication time are considered fresh
    (dedup handles reruns — see DECISIONS.md).
    """
    if published_at is None:
        return True
    now_utc = now or datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(hours=hours)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    return published_at >= cutoff


def is_in_window(
    published_at: datetime | None,
    *,
    since: datetime,
    now: datetime | None = None,
) -> bool:
    """True iff ``published_at`` falls inside ``[since, now]``.

    Companion to :func:`is_fresh`, used by the scheduled-run path where the
    lower bound is computed from ``last_run_at - overlap`` (see
    :mod:`news_agent.core.run_state`) instead of a fixed lookback in hours.

    Articles without a publication time are treated as fresh — the dedup
    layer handles re-ingestion across overlapping runs.
    """
    if published_at is None:
        return True
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    if published_at < since:
        return False
    # Articles dated in the future (sloppy site clocks) — accept up to ``now``.
    now_utc = (now or datetime.now(timezone.utc))
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    # Allow a small (5 min) future skew tolerance for clock drift.
    if published_at > now_utc + timedelta(minutes=5):
        return False
    return True
