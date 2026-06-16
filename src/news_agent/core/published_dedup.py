"""Deterministic dedup against the editor's published archive
("Опубликованные (все)", refreshed daily by the manager).

This is the SAFE complement to the failed fuzzy archive-judge (which hit
26% catch / 20% false-positive because semantic similarity can't tell
"republished" from "new variant"). Here we match only EXACTLY:

  * canonical URL  — if we collected the very source URL the editor already
    published, it's a duplicate (zero false positives — a URL is a URL);
  * normalised title — only against archive entries published RECENTLY
    (the loader bounds this to a recency window) and only when the title
    is long enough to be distinctive, so a recurring headline months apart
    doesn't false-match.

It will NOT catch cross-source paraphrases (different URL + different
wording) — those still rely on the LLM-editor's soft "возможно дубль" flag
→ review tab. Partial but safe, and free.
"""

from __future__ import annotations

from urllib.parse import urlparse

from news_agent.core.primary_source import normalise_title

# A title must have at least this many tokens (after normalisation) before
# an exact match is trusted — guards against short/generic headlines
# colliding ("Lada обновилась").
MIN_TITLE_TOKENS = 4


def url_key(url: str) -> str:
    """Aggressive URL key for dedup: drop scheme, www, query, fragment and
    trailing slash so "https://moex.com/n/1?utm=x", "http://www.moex.com/n/1/"
    and "moex.com/n/1" all collapse to one key. (The shared `canonicalise`
    is intentionally lighter and keeps query strings, so it's too strict for
    cross-source matching.)"""
    s = (url or "").strip().lower()
    if "//" not in s:
        s = "//" + s
    p = urlparse(s)
    host = p.netloc[4:] if p.netloc.startswith("www.") else p.netloc
    return host + p.path.rstrip("/")


def already_published(
    article_url: str,
    title: str,
    pub_urls: set[str],
    pub_recent_titles: set[str],
) -> str:
    """Return 'url' | 'title' | '' — why (if) this candidate is already in
    the editor's published archive. ``pub_urls`` is ALL archive source URLs
    (as url_key); ``pub_recent_titles`` is normalised titles of only the
    recently-published entries (recency-gated by the loader)."""
    if article_url:
        cu = url_key(article_url)
        if cu and cu in pub_urls:
            return "url"
    nt = normalise_title(title or "")
    if len([t for t in nt.split() if t]) >= MIN_TITLE_TOKENS \
            and nt in pub_recent_titles:
        return "title"
    return ""
