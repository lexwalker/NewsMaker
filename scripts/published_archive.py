"""Load the editor's published archive ("Опубликованные (все)", refreshed
daily by the manager) into dedup sets the prog checks before pushing.

Returns:
  pub_urls          — url_key of EVERY archive Outer Link (a published
                      source URL is a duplicate forever);
  pub_recent_titles — normalised titles of entries published within the
                      recency window (exact-title match is recency-gated so
                      a recurring headline months apart doesn't false-match).

Lives in the same spreadsheet as the source list, so the prog reads it with
its existing client. Resilient: any failure returns empty sets (the gate
simply does nothing — never breaks a prog).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from news_agent.core.primary_source import normalise_title
from news_agent.core.published_dedup import MIN_TITLE_TOKENS, url_key

PUB_TAB = "Опубликованные (все)"

# Freshness telemetry set by load_published_index: the newest «Начало
# активности» seen in the archive. jul-10 incident: the export silently
# stopped on 30.06 — 10 days of portal publications invisible to the anti-dup,
# editors flooded with «было уже» while every count-based floor passed.
# The run's health check surfaces staleness via this value.
LAST_ARCHIVE_MAX_DT: datetime | None = None


def _parse_date(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    # Google Sheets serial number: with valueRenderOption=UNFORMATTED_VALUE a
    # date cell comes back as a float (days since the 1899-12-30 epoch), e.g.
    # 46199.70 -> 2026-06-29. The recent (date-typed) rows arrive this way, so
    # without this branch they fail every string format below and drop out of
    # the recency window -> 0 recent titles -> the paraphrase dedup silently dies.
    try:
        serial = float(s)
        if 30000 < serial < 80000:  # ~1982..2119, a plausible date serial
            return datetime(1899, 12, 30, tzinfo=timezone.utc) + timedelta(days=serial)
    except (ValueError, OverflowError):
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[: len(fmt) + 2].strip(), fmt).replace(
                tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_published_index(
    svc, spreadsheet_id: str, recent_days: int = 21
) -> tuple[set[str], set[str]]:
    """(pub_urls, pub_recent_titles). Columns: D Название(EN) | E
    Заголовок локализованный(RU) | F Начало активности | L Outer Link."""
    try:
        # A:R (NOT A1:R6000): the archive outgrew 6000 rows (jun-2026: 6077),
        # and a hard row cap silently TRUNCATES the most-recent publications —
        # exactly the ones the anti-dup must catch. Open-ended range reads all.
        rows = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"'{PUB_TAB}'!A:R",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute().get("values", [])
    except Exception as e:  # noqa: BLE001 — never break the prog over the gate
        # …but never die SILENTLY either: this exact gate has gone dark three
        # times (empty-tab read, serial-date decay, the A1:R6000 truncation)
        # and each time dups flowed to the editor for days. The caller's
        # health check also floors the sizes; this line makes the cause
        # greppable in the run log.
        import sys
        print(
            f"!!! published-archive load FAILED ({type(e).__name__}: "
            f"{str(e)[:200]}) — archive dedup is EMPTY this run.",
            file=sys.stderr,
        )
        return set(), set()

    cutoff = datetime.now(timezone.utc) - timedelta(days=recent_days)
    pub_urls: set[str] = set()
    pub_titles: set[str] = set()
    max_dt: datetime | None = None

    def cell(r, i):
        return str(r[i]).strip() if len(r) > i and r[i] is not None else ""

    for r in rows[1:]:
        url = cell(r, 11)
        if url.startswith("http"):
            k = url_key(url)
            if k:
                pub_urls.add(k)
        dt = _parse_date(cell(r, 5))
        if dt is not None and (max_dt is None or dt > max_dt):
            max_dt = dt
        if dt is None or dt < cutoff:
            continue
        for ti in (cell(r, 3), cell(r, 4)):
            nt = normalise_title(ti)
            if len([t for t in nt.split() if t]) >= MIN_TITLE_TOKENS:
                pub_titles.add(nt)
    global LAST_ARCHIVE_MAX_DT
    LAST_ARCHIVE_MAX_DT = max_dt
    return pub_urls, pub_titles
