"""Heuristic primary-source detection — pure.

Inputs: article title, body, outbound links, plus configs (brand domains,
press-release hosts, cue phrases per language).

Output: (url, domain, confidence). Falls back to the article URL itself
with ``low`` confidence when nothing better is found.
"""

from __future__ import annotations

import re
from typing import Literal

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


def detect_primary_source(
    *,
    article_url: str,
    body: str,
    title: str,
    outbound_links: list[str],
    brands: list[BrandDomainEntry],
    cues: PrimarySourceCues,
) -> tuple[str, str, Confidence]:
    """Return (primary_url, primary_domain, confidence)."""
    article_domain = _normalise_domain(domain_of(article_url))
    mentioned = _mentions_brand(title + "\n" + body, brands)

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
