"""Heuristic primary-source detection — pure.

Two levels are implemented here:

  Level 1 — ``detect_primary_source``: look at outbound links in the
  article body against brand-owned / press-release domains plus cue
  phrases ("press release", "сообщает"). Works when the writer
  explicitly linked the source.

  Level 2 — ``detect_earliest_in_corpus``: search for the same headline
  (fuzzy match) across the corpus of articles this run has already
  fetched, and return the one with the EARLIEST publication timestamp.
  Works when nobody cited the source but several sites repeated the
  story: whichever ran it first is the primary.

Level 1 first, fall back to Level 2 on low-confidence Level 1 results.

Output from both levels: (url, domain, confidence).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable, Literal

from rapidfuzz import fuzz

from news_agent.core.config_loader import BrandDomainEntry, PrimarySourceCues
from news_agent.core.urls import domain_of

Confidence = Literal["high", "medium", "low"]

# Trim "www." + known ccTLDs so media.bmwgroup.com matches press.bmwgroup.com
_SUBDOMAIN_STRIP = re.compile(r"^(?:www|m|amp)\.")


def _normalise_domain(d: str) -> str:
    return _SUBDOMAIN_STRIP.sub("", d.lower())


def _matches_brand(link_domain: str, brands: list[BrandDomainEntry]) -> BrandDomainEntry | None:
    n = _normalise_domain(link_domain)
    for b in brands:
        for d in b.domains:
            nd = _normalise_domain(d)
            if n == nd or n.endswith("." + nd):
                return b
    return None


def _mentions_brand(text: str, brands: list[BrandDomainEntry]) -> set[str]:
    t = text.lower()
    hit: set[str] = set()
    for b in brands:
        names = [b.brand.lower(), *(a.lower() for a in b.aliases)]
        if any(n in t for n in names):
            hit.add(b.brand)
    return hit


def _press_release_host(link_domain: str, hosts: list[str]) -> bool:
    n = _normalise_domain(link_domain)
    return any(n == _normalise_domain(h) or n.endswith("." + _normalise_domain(h)) for h in hosts)


def _has_cue_phrase(body: str, cues: PrimarySourceCues) -> bool:
    t = body.lower()
    for phrases in cues.phrases.values():
        if any(p.lower() in t for p in phrases):
            return True
    return False


def _is_mirror(domain: str, mirror_hosts: list[str]) -> bool:
    n = _normalise_domain(domain)
    for h in mirror_hosts:
        nh = _normalise_domain(h)
        if n == nh or n.endswith("." + nh):
            return True
    return False


def detect_primary_source(
    *,
    article_url: str,
    body: str,
    title: str,
    outbound_links: list[str],
    brands: list[BrandDomainEntry],
    cues: PrimarySourceCues,
) -> tuple[str, str, Confidence]:
    """Return (primary_url, primary_domain, confidence).

    Mirror hosts (t.me, max.ru, vk.com, telegra.ph …) are filtered out of
    outbound-link candidates: they usually are the same author cross-posting
    their own content, not a primary source.
    """
    article_domain = _normalise_domain(domain_of(article_url))
    mentioned = _mentions_brand(title + "\n" + body, brands)
    # Keep only outbound links that are NOT mirrors of the current post.
    outbound_links = [
        link for link in outbound_links
        if not _is_mirror(domain_of(link), cues.mirror_hosts)
    ]

    # Tier 1 — press-release host.
    for link in outbound_links:
        d = domain_of(link)
        if _normalise_domain(d) == article_domain:
            continue
        if _press_release_host(d, cues.press_release_hosts):
            return link, d, "high"

    # Tier 2 — brand-owned domain, brand mentioned in article.
    if mentioned:
        for link in outbound_links:
            d = domain_of(link)
            if _normalise_domain(d) == article_domain:
                continue
            b = _matches_brand(d, brands)
            if b and b.brand in mentioned:
                return link, d, "high"

    # Tier 3 — brand-owned domain, brand not textually confirmed.
    for link in outbound_links:
        d = domain_of(link)
        if _normalise_domain(d) == article_domain:
            continue
        if _matches_brand(d, brands):
            return link, d, "medium"

    # Tier 4 — cue phrase present + any external link present.
    if _has_cue_phrase(body, cues):
        for link in outbound_links:
            d = domain_of(link)
            if _normalise_domain(d) == article_domain:
                continue
            return link, d, "medium"

    # Fallback — the article itself is the primary source.
    return article_url, domain_of(article_url), "low"


# --- Level 2: earliest appearance in our own corpus ------------------------

# Common title suffix noise we want to strip before fuzzy-matching:
#   "… - SMMT"          "… | Geely Russia"
#   "… - Korean Car Blog"  "… — CarNewsChina.com"
_TITLE_SUFFIX_RE = re.compile(r"\s*[–—\-|]\s+[^|–—\-]+$")


class CorpusEntry:
    """One record in the in-run corpus used for earliest-appearance search."""

    __slots__ = ("url", "title", "published_at", "domain")

    def __init__(
        self, *, url: str, title: str, published_at: datetime | None, domain: str
    ) -> None:
        self.url = url
        self.title = title
        self.published_at = published_at
        self.domain = domain


def normalise_title(title: str) -> str:
    """Strip common site-name suffixes and normalise for fuzzy match."""
    t = (title or "").strip()
    # Drop up to 2 trailing "site name" segments ("— SMMT - UK").
    for _ in range(2):
        nt = _TITLE_SUFFIX_RE.sub("", t).strip()
        if nt == t or len(nt) < 20:
            break
        t = nt
    return t.lower()


def detect_earliest_in_corpus(
    *,
    article_url: str,
    article_title: str,
    article_published_at: datetime | None,
    corpus: Iterable[CorpusEntry],
    whitelist_domains: set[str] | None = None,
    press_release_hosts: list[str] | None = None,
    mirror_hosts: list[str] | None = None,
    similarity_threshold: float = 0.72,
) -> tuple[str, str, Confidence] | None:
    """Find the earliest article in ``corpus`` whose title fuzzy-matches.

    Returns ``(url, domain, confidence)`` or ``None`` if no plausible
    earlier twin was found.

    Priority when several candidates tie on timestamp:
      1. press-release hosts (always first — these are authoritative)
      2. whitelist domains (editor-trusted)
      3. alphabetical — stable fallback

    Confidence is ``high`` if the winner is a press-release host, else
    ``medium``. We never return ``low`` from this level — the caller
    should already have the article URL itself as the low-confidence
    fallback.
    """
    target_norm = normalise_title(article_title)
    if len(target_norm) < 20:
        return None  # title too short — false matches are likely
    target_domain = domain_of(article_url)
    threshold = similarity_threshold * 100.0
    whitelist = whitelist_domains or set()
    press_hosts = set((press_release_hosts or []))

    mirror_set = mirror_hosts or []
    candidates: list[tuple[CorpusEntry, float]] = []
    for entry in corpus:
        if entry.url == article_url or entry.domain == target_domain:
            continue
        if _is_mirror(entry.domain, mirror_set):
            # e.g. t.me / max.ru / vk.com — not a primary source
            continue
        if entry.published_at is None or article_published_at is None:
            # can't compare ordering without both timestamps
            continue
        if entry.published_at >= article_published_at:
            # candidate was not earlier, so it's not a primary source
            continue
        ratio = fuzz.token_set_ratio(target_norm, normalise_title(entry.title))
        if ratio >= threshold:
            candidates.append((entry, ratio))

    if not candidates:
        return None

    # Sort: earliest first; among ties, press-release > whitelist > others
    def _tier(e: CorpusEntry) -> int:
        d = e.domain
        for h in press_hosts:
            hn = h.lower().lstrip(".")
            if d == hn or d.endswith("." + hn):
                return 0
        if d in whitelist:
            return 1
        return 2

    candidates.sort(
        key=lambda c: (c[0].published_at, _tier(c[0]), c[0].domain)
    )
    winner = candidates[0][0]
    confidence: Confidence = "high" if _tier(winner) == 0 else "medium"
    return winner.url, winner.domain, confidence
