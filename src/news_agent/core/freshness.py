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
