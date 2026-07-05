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
from urllib.parse import urlparse

from rapidfuzz import fuzz

from news_agent.core.config_loader import BrandDomainEntry, PrimarySourceCues
from news_agent.core.urls import domain_of

Confidence = Literal["high", "medium", "low"]

# Trim "www." + known ccTLDs so media.bmwgroup.com matches press.bmwgroup.com
_SUBDOMAIN_STRIP = re.compile(r"^(?:www|m|amp)\.")

# Plan P2-A (may-2026, 248-comment editor audit). The editor repeatedly
# insisted the bot surface the *original journalistic* source when an
# article is hosted on a Russian redistribution portal that merely
# reposts. Example editor flags:
#   row  92 "первоисточником должен быть Carscoops, на него ссылается Автомейл"
#   row 141 "первоисточник - thekoreancarblog.com"
#   row  98 "первоисточником должен быть ANCAP"
#
# When the article domain is one of these redistributors AND the body
# links out to a recognised primary journalistic/official host, that
# outbound link is promoted to the primary source (high confidence).
# This tier is GATED on the redistribution-host check, so articles on
# any other domain keep the exact pre-existing tier behaviour — no
# regression risk to the 9 existing detect_primary_source tests.
_REDISTRIBUTION_HOSTS: frozenset[str] = frozenset({
    "auto.mail.ru", "mail.ru",
    "asroad.org",
    "1prime.ru", "ria.ru", "tass.ru", "iz.ru",
    "interfax.ru", "finmarket.ru",
    "autonews.ru", "motorpage.ru",
    "quto.ru", "kolesa.ru",
    # may-2026 editor note: «спидми очень редко используем как
    # первоисточник — обычно это перепост англ новостей».
    "speedme.ru",
    # jul-02: the editor's #1 re-attribution target («искать на NHTSA уже
    # писала неоднократно», «первоисточник autonews.ru» — both on
    # naavtotrasse reposts). It names its source in TEXT/slug only (no
    # outbound href), so it must never self-certify as the primary.
    "naavtotrasse.ru",
    "ixbt.com", "media.ixbt.com",
})
_PREFERRED_PRIMARY_HOSTS: frozenset[str] = frozenset({
    # journalistic primaries the editor named explicitly
    "carscoops.com", "motor1.com",
    "thekoreancarblog.com",
    "carnewschina.com", "cnevpost.com",
    "autoevolution.com", "electrek.co",
    "carsdirect.com", "autocarindia.com",
    # official / industry bodies
    "ancap.com.au", "euroncap.com",
    "aebrus.ru", "nhtsa.gov",
})

# jul-03: redistribution portals often name their source in TEXT ONLY
# («сообщает Autonews.ru», «по данным Автостата») or in the URL SLUG
# (naavtotrasse: /auto-news/autonews-ru-shtrafy…) with NO outbound href —
# the link-based tiers can't see it. Known publication names → domain.
_TEXT_SOURCE_MENTIONS: dict[str, str] = {
    "autonews.ru": "autonews.ru", "autonews": "autonews.ru",
    "автостат": "autostat.ru", "autostat": "autostat.ru",
    "коммерсант": "kommersant.ru", "kommersant": "kommersant.ru",
    "ведомост": "vedomosti.ru",
    "тасс": "tass.ru",
    "риа новости": "ria.ru",
    "за рулём": "zr.ru", "за рулем": "zr.ru",
    "известия": "iz.ru",
    "интерфакс": "interfax.ru",
    "нбки": "nbki.ru",
    "carscoops": "carscoops.com",
    "carnewschina": "carnewschina.com",
    "cnevpost": "cnevpost.com",
    "reuters": "reuters.com", "рейтер": "reuters.com",
    "bloomberg": "bloomberg.com", "блумберг": "bloomberg.com",
    "automotive news": "autonews.com",
    "autocar india": "autocarindia.com",
}
_SOURCE_CUE_RE = re.compile(
    r"(?:сообщает|сообщил[аио]?|пишет|по данным|по информации|"
    r"со ссылкой на|источник[:\s]|цитирует)\s+[«\"']?"
    r"([a-zа-яё][a-zа-яё0-9 .\-]{2,28})",
    re.I,
)


def _text_mentioned_source(body: str, article_domain_norm: str) -> str:
    """Domain of a publication the body TEXT credits as the source, or ""."""
    for m in _SOURCE_CUE_RE.finditer(body[:4000]):
        cand = m.group(1).strip().lower()
        for name, dom in _TEXT_SOURCE_MENTIONS.items():
            if cand.startswith(name) and not _same_site(dom, article_domain_norm):
                return dom
    return ""


def _slug_named_source(article_url: str, article_domain_norm: str) -> str:
    """Domain encoded at the start of the article's URL slug
    (…/autonews-ru-v-i-polugodii… → autonews.ru), or ""."""
    slug = urlparse(article_url).path.rsplit("/", 1)[-1].lower()
    for dom in set(_TEXT_SOURCE_MENTIONS.values()):
        if slug.startswith(dom.replace(".", "-")) and \
                not _same_site(dom, article_domain_norm):
            return dom
    return ""


def _normalise_domain(d: str) -> str:
    return _SUBDOMAIN_STRIP.sub("", d.lower())


# ---------------------------------- junk-link filters ---------------------
# Outbound links that must NEVER be picked as a primary source. The full
# string is checked case-insensitive against URL.
_JUNK_URL_FRAGMENTS = (
    # Share buttons + share-widget aggregators (addtoany wraps the real
    # article in a ?linkurl= param — never a usable primary itself)
    "facebook.com/sharer/", "facebook.com/share/",
    "twitter.com/intent/", "twitter.com/share",
    "t.me/share", "vk.com/share",
    "linkedin.com/share", "pinterest.com/pin/create",
    "wa.me/", "api.whatsapp.com/send",
    "ok.ru/share", "reddit.com/submit",
    "addtoany.com", "/add_to/", "sharethis.com", "addthis.com",
    # Print / email-this widgets
    "/print/", "?print=", "/print.html",
    "mailto:", "/email-this", "/sendtofriend",
    # Login / auth pages
    "/login", "/signin", "/sign-in", "/auth/", "/oauth",
    "/register", "/signup", "/sign-up",
    # Contact / about pages
    "/contact", "/contacts", "/contact-us", "/contact.html",
    "/feedback", "/support",
    # Tracking redirectors / click-counters (liveinternet wraps the target
    # in /click with NO query string, so the "?"-suffixed forms miss it)
    "doubleclick.net", "googleadservices", "google.com/url?",
    "googletagmanager.com", "ga4-", "liveinternet.ru",
    "/redir?", "/redirect?", "/click?", "/click/", "liveinternet.ru/click",
    "/r.php?",
    # Generic search results pages
    "/search?", "/search/", "?q=",
)


# Bare profiles/roots on these are not articles (e.g. twitter.com/SomeJourno);
# a genuine post carries /status/, /posts/, /permalink or /p/. t.me is NOT
# here — it is a legitimate source in this project.
_SOCIAL_PROFILE_HOSTS = {
    "twitter.com", "x.com", "facebook.com", "instagram.com", "threads.net",
}
_SOCIAL_POST_MARKERS = ("/status/", "/posts/", "/permalink", "/p/")


# Image / CDN-resize URLs are page furniture, not sources. A real case: an
# ixbt article's only "outbound" link was media.ixbt.com/fit-in/1110x/… (an
# image-resize URL) and it was promoted to primary with medium confidence.
_IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg", ".avif")
_IMAGE_PATH_MARKERS = ("/fit-in/", "/resize/", "/thumb/", "/thumbs/", "/img/")


def _is_junk_link(url: str) -> bool:
    """Reject share buttons / share-widget aggregators, bare social-network
    profiles, login pages, tracking redirectors, root-only URLs, image/CDN
    links and similar non-source links."""
    if not url:
        return True
    u = url.lower()
    if any(frag in u for frag in _JUNK_URL_FRAGMENTS):
        return True
    if any(m in u for m in _IMAGE_PATH_MARKERS):
        return True
    if urlparse(u).path.lower().endswith(_IMAGE_EXTENSIONS):
        return True
    # Reject root-only URLs: a press release should have a path. We accept
    # any URL whose path has more than just "/". This filter catches
    # "https://www.gazeta.ru/" or "https://toyota.jp/" homepage links.
    parsed = urlparse(url)
    path = (parsed.path or "").rstrip("/")
    if not path or path == "/":
        # URL has no meaningful path
        return True
    # Reject bare social-network profiles (twitter.com/<handle> with no post
    # marker) — they are not articles and must never be a primary source.
    if (_normalise_domain(parsed.netloc) in _SOCIAL_PROFILE_HOSTS
            and not any(m in u for m in _SOCIAL_POST_MARKERS)):
        return True
    return False


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


def _same_site(candidate_domain: str, article_domain_norm: str) -> bool:
    """True when the candidate is the article's own site INCLUDING its
    subdomains (media.ixbt.com vs ixbt.com). The old exact-equality check let
    a site's own CDN/media subdomain through as an 'external' primary."""
    n = _normalise_domain(candidate_domain)
    return (n == article_domain_norm
            or n.endswith("." + article_domain_norm)
            or article_domain_norm.endswith("." + n))


def detect_primary_source(
    *,
    article_url: str,
    body: str,
    title: str,
    outbound_links: list[str],
    brands: list[BrandDomainEntry],
    cues: PrimarySourceCues,
    whitelist_domains: set[str] | None = None,
    source_hint_url: str = "",
) -> tuple[str, str, Confidence]:
    """Return (primary_url, primary_domain, confidence).

    Mirror hosts (t.me, max.ru, vk.com, telegra.ph …) and junk URLs
    (share buttons, login pages, root-only homepages, tracking redirects)
    are filtered out of outbound-link candidates.

    Tier 0 — if the article URL itself is on a press-release host or in
    the editor-trusted whitelist, the article IS the primary source. This
    avoids picking a random brand-domain link from the body when the
    article we're processing is the canonical release.
    """
    article_domain = _normalise_domain(domain_of(article_url))
    mentioned = _mentions_brand(title + "\n" + body, brands)
    whitelist_norm = {_normalise_domain(d) for d in (whitelist_domains or set())}

    # Tier 0 — article itself is a press release / whitelist source.
    if _press_release_host(article_domain, cues.press_release_hosts):
        return article_url, domain_of(article_url), "high"

    # Tier 0.5 — the article EXPLICITLY marks its source (an «Источник:» link
    # or a source-marker attribute). The editor's recurring «первоисточник
    # указан в статье» — auto.mail credits e.g. «источник: rg.ru». We trust
    # this even when it's a bare domain root (the outbound junk filter would
    # drop it), as long as it's a different site than the article's own.
    if source_hint_url:
        hd = domain_of(source_hint_url)
        if hd and not _same_site(hd, article_domain) and not _is_mirror(hd, cues.mirror_hosts):
            return source_hint_url, hd, "high"
    if (article_domain in whitelist_norm
            and article_domain not in _REDISTRIBUTION_HOSTS):
        # Whitelist here means "editor-trusted PRIMARY" (zr.ru tests, autostat
        # own research). Redistribution hosts can be fetch-whitelisted too,
        # but a repost must never self-certify as the primary source (the
        # naavtotrasse case: primary=self at HIGH masked the real source).
        return article_url, domain_of(article_url), "high"

    # Filter out mirror posts AND junk URLs from candidates.
    outbound_links = [
        link for link in outbound_links
        if link
        and not _is_junk_link(link)
        and not _is_mirror(domain_of(link), cues.mirror_hosts)
    ]

    # Tier 1 — press-release host in outbound links.
    for link in outbound_links:
        d = domain_of(link)
        if _same_site(d, article_domain):
            continue
        if _press_release_host(d, cues.press_release_hosts):
            return link, d, "high"

    # Tier 1.5 — Plan P2-A: redistribution host → promote a recognised
    # journalistic / official primary that the body links to. Gated on
    # article_domain being a known redistributor, so this is a no-op for
    # every other source (zero regression to existing behaviour).
    if _normalise_domain(article_domain) in _REDISTRIBUTION_HOSTS or \
            article_domain in _REDISTRIBUTION_HOSTS:
        for link in outbound_links:
            d = domain_of(link)
            if _same_site(d, article_domain):
                continue
            nd = _normalise_domain(d)
            if nd in _PREFERRED_PRIMARY_HOSTS or d in _PREFERRED_PRIMARY_HOSTS:
                return link, d, "high"

    # Tier 2 — brand-owned domain, brand mentioned in article.
    if mentioned:
        for link in outbound_links:
            d = domain_of(link)
            if _same_site(d, article_domain):
                continue
            b = _matches_brand(d, brands)
            if b and b.brand in mentioned:
                return link, d, "high"

    # Tier 3 — brand-owned domain, brand not textually confirmed.
    for link in outbound_links:
        d = domain_of(link)
        if _same_site(d, article_domain):
            continue
        if _matches_brand(d, brands):
            return link, d, "medium"

    # Tier 4 — cue phrase present + any external link present.
    if _has_cue_phrase(body, cues):
        for link in outbound_links:
            d = domain_of(link)
            if _same_site(d, article_domain):
                continue
            return link, d, "medium"

    # Tier 5 — redistribution portal naming its source in TEXT or URL SLUG
    # (naavtotrasse has NO outbound hrefs at all: «сообщает Autonews.ru» in
    # the body, or the slug itself /autonews-ru-…). Only for known
    # redistributors, so every other domain keeps prior behaviour. The root
    # URL is for the EDITOR (attribution), not for our fetcher.
    if (_normalise_domain(article_domain) in _REDISTRIBUTION_HOSTS
            or article_domain in _REDISTRIBUTION_HOSTS):
        src = (_text_mentioned_source(body, article_domain)
               or _slug_named_source(article_url, article_domain))
        if src:
            return f"https://{src}/", src, "medium"

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
    """Strip site-name suffixes + transliterate for fuzzy match.

    Delegates to :func:`news_agent.core.fuzzy_match.normalise_for_match`
    which handles language tags, diacritics, Cyrillic→Latin transliteration,
    number-words and punctuation.
    """
    from news_agent.core.fuzzy_match import normalise_for_match
    return normalise_for_match(title)


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
