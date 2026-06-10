"""URL canonicalisation + hashing helpers."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

TRACKING_PARAMS = frozenset(
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "utm_id",
        "yclid",
        "gclid",
        "fbclid",
        "mc_cid",
        "mc_eid",
        "ref",
        "ref_src",
    }
)


def canonicalise(url: str) -> str:
    """Return a stable canonical form of ``url``.

    - lowercase scheme + host
    - strip default ports
    - strip known tracking params
    - strip fragment
    - preserve path case and remaining query order
    """
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    elif netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]

    query = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() not in TRACKING_PARAMS
    ]
    return urlunparse((scheme, netloc, parsed.path or "/", parsed.params, urlencode(query), ""))


def url_hash(url: str) -> str:
    return hashlib.sha256(canonicalise(url).encode("utf-8")).hexdigest()


def domain_of(url: str) -> str:
    host = urlparse(url).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def normalise_source_entry(raw: str) -> str | None:
    """Normalise one row of the editor's source sheet into a fetchable URL.

    The editor writes sources in mixed forms: full URLs, bare domains
    ("kolesa.ru"), domain+path ("vesti.ru/auto", "t.me/delimobil"). The
    fetch loop used to require an http(s) scheme and SILENTLY skipped the
    bare forms — 33 active sources (drom, motor.ru, zr.ru, lenta, rbc,
    insideevs …) were never crawled, a major hidden coverage hole (found
    by the jun-2026 miss-funnel). Scheme-less entries now get https://.

    Returns None for rows that are not URLs at all (stray words like
    "экономика", notes with spaces) so callers can drop them loudly.
    """
    s = (raw or "").strip()
    if not s or " " in s:
        return None
    if not s.startswith(("http://", "https://")):
        s = "https://" + s.lstrip("/")
    host = urlparse(s).netloc
    # A real host has at least one dot ("kolesa.ru"); bare words don't.
    if "." not in host:
        return None
    return s


# Year segments in URL paths look like:
#   /2022/05/21/...      → 2022
#   /2023-04-30-...      → 2023
#   /news/2024/...       → 2024
#   /article/2024-12-...  → 2024
# We only accept four-digit year segments wrapped in non-digit boundaries —
# avoids false-matching part numbers / model codes ("2024" inside a SKU).
_YEAR_IN_PATH_RE = re.compile(r"(?:^|[^0-9])((?:19|20)\d{2})(?:[^0-9]|$)")


def year_in_url_path(url: str) -> int | None:
    """Return the EARLIEST four-digit year found in the URL path, or None.

    Used as a sanity-check for stale articles whose ``published_at`` cannot
    be extracted from the HTML — e.g. Hyundai newsroom 2022 archive pages.
    Earliest match (rather than first-seen) so a URL like
    ``/news/2024/article-about-2026-tucson`` still flags 2024.
    """
    parsed = urlparse(url)
    path = parsed.path or ""
    found = []
    for m in _YEAR_IN_PATH_RE.finditer(path):
        try:
            y = int(m.group(1))
            if 1990 <= y <= 2100:
                found.append(y)
        except ValueError:
            continue
    return min(found) if found else None
