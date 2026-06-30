"""NHTSA US vehicle-recall source adapter.

The editor repeatedly asked for **all US recall campaigns** to be sourced
from NHTSA (https://www.nhtsa.gov/recalls). This adapter pulls recent
campaigns from the NHTSA Office of Defects Investigation (ODI) Recalls
dataset — the Socrata JSON API on data.transportation.gov — and turns each
campaign into a :class:`RawArticle` so it flows through the normal
editorial → dedup → cluster → push pipeline with no downstream changes.

Why this is a *good* source (unlike scraped HTML):
  * structured JSON — no WAF / Brotli / JS-render failures;
  * authoritative primary source (the regulator itself);
  * the campaign URL (``recall_link.url`` / ``nhtsa_id``) is a stable,
    unique dedup key — the same recall never republishes.

Two facts shape the design:
  1. **Volume is low** (~1-2/day) so we can surface every Vehicle-type
     campaign; the editorial constitution makes the final keep/section
     call (a foreign-brand recall → Other news / Global; a trailer / RV /
     bus recall → rejected as off-topic).
  2. **NHTSA lags ~3-5 days** between a recall and its appearance in the
     dataset. So the caller must NOT apply the usual 24-48h freshness
     window — "fresh" for a recall means "not already published / not yet
     cached" (dedup-based), handled on the batch_fetch side.

Pure functions only (no network globals) so the formatting is unit-testable;
the single network call lives in :func:`fetch_recent_recalls`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx

from news_agent.core.models import RawArticle

# Current ODI Recalls dataset (Socrata). This same URL doubles as the
# self-documenting "source" sentinel registered in batch_fetch_test.py so
# the dispatcher can route to this adapter.
RECALLS_ENDPOINT = "https://data.transportation.gov/resource/6axg-epim.json"

# NHTSA also issues Equipment / Tire / Child-Seat recalls — off-topic for
# car news. Pre-filter to Vehicle campaigns to save LLM calls; the editorial
# constitution still rejects non-car Vehicle campaigns (trailers, RVs, buses)
# on its own. Empty set ⇒ no filter (let the LLM decide everything).
RECALL_TYPES = frozenset({"Vehicle"})

SOURCE_LANGUAGE = "en"

# Legal-entity suffixes the headline doesn't need ("Hyundai Motor America,
# Inc." → "Hyundai Motor America"). Conservative — only trailing noise.
_LEGAL_SUFFIXES = (
    ", Inc.", ", Inc", " Inc.", ", LLC", ", L.L.C.", ", Ltd.",
    ", Corporation", ", Corp.", ", Co.", ", Company",
)


def is_recalls_source(url: str) -> bool:
    """True when a source URL should be routed to this adapter."""
    return "data.transportation.gov" in (url or "").lower()


def _parse_dt(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _clean_manufacturer(manufacturer: str) -> str:
    m = (manufacturer or "").strip()
    for suf in _LEGAL_SUFFIXES:
        if m.endswith(suf):
            m = m[: -len(suf)].strip()
            break
    return m


def _clean_subject(subject: str) -> str:
    # "Instrument Panel Display Failure/FMVSS 101" → "Instrument Panel Display
    # Failure" — drop a trailing regulatory citation that reads as noise.
    s = (subject or "").strip()
    for cut in ("/FMVSS", "/ FMVSS", " - FMVSS"):
        if cut in s:
            s = s.split(cut)[0].strip()
            break
    return s


def _units(rec: dict) -> int | None:
    raw = rec.get("potentially_affected")
    if raw in (None, ""):
        return None
    try:
        n = int(float(raw))
    except (ValueError, TypeError):
        return None
    return n if n > 0 else None


def format_title(rec: dict) -> str:
    """EN news headline for a recall campaign. The editorial + translate
    passes consume this; they extract the brand/model themselves, so we keep
    it faithful rather than clever."""
    who = _clean_manufacturer(rec.get("manufacturer") or "") or "Manufacturer"
    subject = _clean_subject(rec.get("subject") or "")
    n = _units(rec)
    head = f"{who} recalls {n:,} vehicles" if n else f"{who} issues recall"
    return f"{head}: {subject}" if subject else head


def format_body(rec: dict) -> str:
    """Lede/body: the regulator's own defect → consequence → remedy prose."""
    parts = []
    for key in ("defect_summary", "consequence_summary", "corrective_action"):
        v = (rec.get(key) or "").strip()
        if v:
            parts.append(v)
    body = " ".join(parts).strip()
    return body or format_title(rec)


def recall_url(rec: dict) -> str:
    """The NHTSA campaign URL — the authoritative primary source AND the
    dedup key. Prefer the dataset's own recall_link, fall back to building
    the canonical campaign URL from the NHTSA id."""
    link = rec.get("recall_link")
    if isinstance(link, dict):
        u = (link.get("url") or "").strip()
        if u:
            return u
    nid = (rec.get("nhtsa_id") or "").strip()
    if nid:
        return f"https://www.nhtsa.gov/recalls?nhtsaId={nid}"
    return ""


def recall_to_article(rec: dict) -> RawArticle | None:
    """Map one Socrata recall row → RawArticle. None when it has no usable
    campaign URL (can't dedup or link it → skip)."""
    url = recall_url(rec)
    if not url:
        return None
    return RawArticle(
        url=url,
        title=format_title(rec),
        body=format_body(rec),
        published_at=_parse_dt(rec.get("report_received_date") or ""),
        source_name="NHTSA",
        source_url=RECALLS_ENDPOINT,
        source_language=SOURCE_LANGUAGE,
    )


def fetch_recent_recalls(
    client: httpx.Client,
    *,
    lookback_days: int = 10,
    limit: int = 50,
    now: datetime | None = None,
) -> list[RawArticle]:
    """Pull recall campaigns received in the last ``lookback_days`` as
    RawArticles (newest first), pre-filtered to :data:`RECALL_TYPES`.

    ``lookback_days`` is deliberately wider than a daily window to absorb
    NHTSA's 3-5 day publishing lag and any back-fill; the batch_fetch
    dedup (published-archive + cache) collapses anything already surfaced,
    so a wide window is safe — it only ever adds genuinely new campaigns.
    """
    now = now or datetime.now(timezone.utc)
    since = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%dT00:00:00")
    params = {
        "$where": f"report_received_date > '{since}'",
        "$order": "report_received_date DESC",
        "$limit": str(int(limit)),
    }
    resp = client.get(
        RECALLS_ENDPOINT, params=params, timeout=30, follow_redirects=True
    )
    resp.raise_for_status()
    rows = resp.json()

    out: list[RawArticle] = []
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        rtype = (rec.get("recall_type") or "").strip()
        if RECALL_TYPES and rtype not in RECALL_TYPES:
            continue
        art = recall_to_article(rec)
        if art is not None:
            out.append(art)
    return out
