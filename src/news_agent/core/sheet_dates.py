"""ONE robust parser for the dates the editor's tabs actually contain.

The 'Опубликованные (все)' archive mixes THREE date encodings in the same
column depending on how a row was entered:
  * ISO            '2026-06-04 17:50'
  * US             '05/26/2026 17:06:00'
  * Excel serial    46177.72   (days since 1899-12-30; what UNFORMATTED_VALUE
                                returns for date-typed cells)

A naive ISO-only parser silently drops every serial-dated row — which made
miss_funnel.py report 0 publications in-window on the current archive. Both
weekly_kpi.py and miss_funnel.py import this so they can never disagree.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y",
)


def parse_sheet_date(value) -> datetime | None:
    """Return a tz-aware UTC datetime, or None if unparseable."""
    if value is None:
        return None
    # Excel serial (a number, or a numeric string like '46177.72').
    try:
        n = float(value)
        if 40000 < n < 60000:        # ~2009..2064 — sane date-serial range
            return (datetime(1899, 12, 30, tzinfo=timezone.utc)
                    + timedelta(days=n))
    except (ValueError, TypeError):
        pass
    s = str(value).strip()
    for fmt in _FORMATS:
        try:
            return datetime.strptime(s[: len(fmt) + 4].strip(), fmt).replace(
                tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
