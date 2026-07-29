"""Article lists embedded in a Next.js __NEXT_DATA__ blob.

Some newsrooms render their listing client-side but ship the SAME list
inside the HTML, in the JSON the framework hydrates from. The bot saw
those pages as "200 but zero links" for weeks (lada.ru — the AvtoVAZ
press room — among them).

This module extracts such lists WITHOUT executing JavaScript.

The lada.ru trap, and why we do not walk the blob blindly (jul-29): the
page carries TWO lists — a fresh one under ``props.pageProps`` and a
build-time cached copy under ``props.initialState`` whose newest item is
from January 2024. A naive "find any array of dicts with a title" walker
picks the stale one about half the time. So we take the deepest match by
FRESHNESS: parse the dates and prefer the branch whose newest item is
newest overall, which is correct whatever the site renames its keys to.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

# Keys that plausibly hold a headline / a date / an id, in the order we
# probe them. Deliberately generic — the freshness ranking below is what
# guards against picking a wrong list, not a hardcoded key path.
_TITLE_KEYS = ("name", "title", "header", "caption")
_DATE_KEYS = ("created", "date", "published", "publishedAt", "pubDate",
              "created_at", "datetime")
_ID_KEYS = ("id", "slug", "url", "alias", "code")

_DATE_PATTERNS = (
    ("%d.%m.%Y", re.compile(r"^\d{2}\.\d{2}\.\d{4}")),
    ("%Y-%m-%d", re.compile(r"^\d{4}-\d{2}-\d{2}")),
    ("%d/%m/%Y", re.compile(r"^\d{2}/\d{2}/\d{4}")),
)


def _parse_date(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw.strip():
        return None
    s = raw.strip()
    for fmt, rx in _DATE_PATTERNS:
        m = rx.match(s)
        if m:
            try:
                return datetime.strptime(m.group(0), fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _first(d: dict, keys: Iterable[str]) -> Any:
    for k in keys:
        v = d.get(k)
        if v not in (None, "", []):
            return v
    return None


def _looks_like_article_list(value: Any) -> list[dict] | None:
    """A list of >=3 dicts that each carry a title-ish string."""
    if not isinstance(value, list) or len(value) < 3:
        return None
    items = [v for v in value if isinstance(v, dict)]
    if len(items) < 3:
        return None
    titled = [v for v in items
              if isinstance(_first(v, _TITLE_KEYS), str)
              and len(str(_first(v, _TITLE_KEYS)).strip()) >= 12]
    if len(titled) < 3:
        return None
    return titled


def _walk(node: Any, depth: int = 0) -> Iterable[list[dict]]:
    """Yield every article-list-shaped array in the blob."""
    if depth > 12:
        return
    found = _looks_like_article_list(node)
    if found:
        yield found
    if isinstance(node, dict):
        for v in node.values():
            yield from _walk(v, depth + 1)
    elif isinstance(node, list):
        for v in node[:50]:      # bounded: listings are never deeper than this
            yield from _walk(v, depth + 1)


def extract_next_data_articles(
    html: str, page_url: str, *, article_path: str = ""
) -> list[dict]:
    """Return [{url, title, published_at}] from a page's __NEXT_DATA__.

    ``article_path`` builds per-item URLs when the payload carries only an
    id (lada.ru: /press-releases/<id>). Empty → the item must supply its
    own url/slug, else it is skipped.
    Never raises: any parsing problem yields an empty list.
    """
    try:
        soup = BeautifulSoup(html, "lxml")
        tag = soup.find("script", id="__NEXT_DATA__")
        if tag is None or not tag.string:
            return []
        blob = json.loads(tag.string)
    except Exception:  # noqa: BLE001 — malformed page must never break a run
        return []

    best: list[dict] = []
    best_newest: datetime | None = None
    for candidate in _walk(blob):
        dates = [d for d in (_parse_date(_first(it, _DATE_KEYS)) for it in candidate) if d]
        newest = max(dates) if dates else None
        # Prefer the FRESHEST list — this is what avoids the build-time
        # cached copy (see module docstring). A dateless list only wins
        # when nothing dated was found at all.
        if best_newest is None and newest is None:
            if len(candidate) > len(best):
                best = candidate
        elif newest is not None and (best_newest is None or newest > best_newest):
            best, best_newest = candidate, newest

    out: list[dict] = []
    seen: set[str] = set()
    for it in best:
        title = str(_first(it, _TITLE_KEYS) or "").strip()
        if len(title) < 12:
            continue
        ident = _first(it, _ID_KEYS)
        if isinstance(ident, str) and ident.startswith(("http://", "https://")):
            url = ident
        elif ident is not None and article_path:
            url = urljoin(page_url, f"{article_path.rstrip('/')}/{ident}")
        else:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append({
            "url": url,
            "title": title,
            "published_at": _parse_date(_first(it, _DATE_KEYS)),
        })
    return out
