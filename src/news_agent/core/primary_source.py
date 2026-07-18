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
from collections import Counter
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
    # official / industry bodies. RU sales/market statistics: the editor
    # (06.07) wants AvtoVAZ + RU-market figures attributed to АВТОСТАТ / АЕБ,
    # NOT the automaker's own site — so both are preferred primaries here (a
    # redistributor linking to an autostat/aeb article promotes it).
    "ancap.com.au", "euroncap.com",
    "aebrus.ru", "autostat.ru", "nhtsa.gov",
    # jul-06 editor: RU regulatory/official documents must be attributed to the
    # government source itself, not the aggregator that reported it
    # («Первоисточник publication.pravo.gov.ru/document/…» on a news.drom.ru
    # row; «Первоисточник minpromtorg.gov.ru/docs/…» on naavtotrasse). These
    # serve documents off deep subdomains (publication.pravo.gov.ru), so the
    # Tier-1.5 match below is subdomain-aware — see _is_preferred_primary.
    "pravo.gov.ru", "minpromtorg.gov.ru",
    # jul-08 editor: «Первоисточник https://atommedia.online/press-release…»
    # — the official press site of Atom (RU EV brand); their releases must win
    # over the aggregator that reported them.
    "atommedia.online",
})

# The subset of preferred hosts that are OFFICIAL / regulatory bodies (as
# opposed to journalistic outlets). A deep link to one of these IS the source
# no matter who reports it — nobody files a government decree or an NHTSA
# recall report under «читайте также» — so these are promoted from ANY article
# (Tier 1.4), not only from redistribution portals. Journalistic preferreds
# (carscoops, electrek…) stay redistribution-gated because outlets link to each
# other in related-reading blocks that are NOT the source. MUST be a subset of
# _PREFERRED_PRIMARY_HOSTS.
_OFFICIAL_PRIMARY_HOSTS: frozenset[str] = frozenset({
    "nhtsa.gov", "ancap.com.au", "euroncap.com",
    "aebrus.ru", "autostat.ru",
    "pravo.gov.ru", "minpromtorg.gov.ru",
    # Atom's own press site — brand-official, a deep release link IS the source
    # regardless of who reports it (editor 08.07).
    "atommedia.online",
})

# Path-scoped official primaries. Some editor-designated primaries live on a
# host that must NOT be whitelisted wholesale (t.me is a redistribution host,
# but t.me/sergtselikov is Автостат head Sergey Tselikov's channel — editor
# 08.07: RU-market stats «это статистика Автостат или Целикова, только от
# них», «Первоисточник https://t.me/sergtselikov/1747»). Matched as a URL
# prefix; a trailing post-id is required (the bare channel root is not an
# article). Keep this list SHORT and editor-named only.
_OFFICIAL_PRIMARY_URL_PREFIXES: tuple[str, ...] = (
    "t.me/sergtselikov/",
)


def _matches_official_prefix(link: str) -> bool:
    """True for a deep post under a path-scoped official primary
    (https://t.me/sergtselikov/1747 → yes; t.me/sergtselikov → no).
    telegram.me is Telegram's own alias of t.me — normalise it, else an
    editor's «первоисточник telegram.me/sergtselikov/…» is missed (jul-15
    zr.ru: the marked Автостат/Целиков link was extracted but not promoted)."""
    bare = re.sub(r"^https?://(?:www\.)?", "", link.lower())
    bare = re.sub(r"^telegram\.me/", "t.me/", bare)
    return any(
        bare.startswith(pfx) and len(bare) > len(pfx)
        for pfx in _OFFICIAL_PRIMARY_URL_PREFIXES
    )

# (Removed jul-06 cost-audit: the TEXT/SLUG source-mention machinery —
# _TEXT_SOURCE_MENTIONS, _SOURCE_CUE_RE, _text_mentioned_source,
# _slug_named_source — was dead code. It fed the former Tier-5 that attributed
# a redistributor's named source as a DOMAIN ROOT; Tier-5 was dropped 04.07
# because roots are useless, and nothing has referenced these since.)
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
    "wa.me/", "api.whatsapp.com/send", "web.whatsapp.com/send",
    # personal-profile pages are never a primary (jul-10 red-mark audit:
    # a Waymo story shipped with primary=linkedin.com/in/<journalist>)
    "linkedin.com/in/",
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
    # (Legal/policy boilerplate is handled by _LEGAL_LEAF_SEGMENTS below —
    # terminal-anchored, NOT substrings: "/legal/" as a substring junked whole
    # news verticals like reuters.com/legal/litigation/<slug>, and bare
    # "/gdpr" junked slugs like /2026/07/gdpr-fine-hits-volkswagen/.)
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


# Legal / policy boilerplate pages. Never a news source, but very much a DEEP
# url, so the deep-URL check does not catch them (jul-17: akismet.com/privacy/
# — the WordPress comment-form notice — was the ONLY external link on a short
# thesupercarblog post and Tier 4 promoted it to primary at medium). Matched as
# the URL's LAST path segment only: a boilerplate page is a leaf
# (site.com/legal/, site.com/terms-of-use), while a real article continues
# past the word (reuters.com/legal/litigation/<slug> — Reuters' automaker-
# litigation vertical! — or /2026/07/gdpr-fine-hits-volkswagen/). The first
# shipped version substring-matched these and junked both.
_LEGAL_LEAF_SEGMENTS: frozenset[str] = frozenset({
    "privacy", "privacy-policy", "privacy-notice",
    "terms", "terms-of-use", "terms-of-service", "terms-of-sale",
    "terms-and-conditions", "tos",
    "cookie-policy", "cookies", "cookie-notice",
    "legal", "disclaimer", "imprint", "impressum", "gdpr",
})


def _is_legal_leaf(path: str) -> bool:
    """True when the path's FINAL segment is a legal/policy boilerplate page."""
    segs = [s for s in (path or "").lower().split("/") if s]
    if not segs:
        return False
    last = segs[-1]
    for ext in (".html", ".htm", ".php", ".aspx"):
        if last.endswith(ext):
            last = last[: -len(ext)]
            break
    return last in _LEGAL_LEAF_SEGMENTS


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
    if _is_legal_leaf(parsed.path):
        return True
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


# Compiled brand matcher, cached per brand-list. Built as ONE alternation so a
# text is scanned in a single pass (115 brands × aliases × every article would
# otherwise be ~265 substring scans per row).
_BRAND_RE_CACHE: dict[tuple, tuple[re.Pattern[str], dict[str, str]]] = {}

# Russian case endings tolerated AFTER a brand name («новинки АвтоВАЗа»,
# «двигатели АвтоВАЗом», «ВАЗы»). A bare (?!\w) boundary rejected these —
# measured jul-17: inflected mentions (АвтоВАЗа/Форда/Хавейла) are the NORM in
# RU headlines, and the boundary fix silently lost them all. The tail is a
# CLOSED list of case endings, not [а-яё]+: an open tail would resurrect the
# Cyrillic-internal false positives («смартфон» = смарт+фон, «танкер» =
# танк+ер — neither "фон" nor "ер" is a case ending, so both stay blocked).
# Multi-char endings listed first so backtracking finds «ом» after «о» fails.
_RU_INFLECT_TAIL = (
    r"(?:ами|ями|ов|ев|ом|ем|ой|ей|ам|ям|ах|ях|[аяуюеыио])?"
)

# Brand names that are ORDINARY WORDS. On a word boundary alone, "smart
# driving" matches Smart, "mini-SUV" matches Mini, a fuel-tank story matches
# Tank (measured jul-17: Smart ×11, Mini ×15, Seat ×5 live false hits). For
# these, the match must be CAPITALISED in the original text ("Smart #5",
# "Tank 500", «Танк 500») — brand usage is; running prose is not. Known
# accepted limit: a capitalised ordinary word at sentence start still passes.
_AMBIGUOUS_BRAND_NAMES: frozenset[str] = frozenset({
    "smart", "mini", "seat", "ram", "genesis", "tank", "im",
    "смарт", "сеат", "рам", "танк",
    # «ваз» lowercase is the genitive plural of «ваза» (vases); the brand is
    # always written ВАЗ.
    "ваз",
    # «газ» lowercase is FUEL — measured on live rows, every lowercase hit was
    # «цены на газ» / «педаль газа» / «выхлопных газов», zero brand mentions
    # (a pre-existing substring-era false-positive class). The automaker is
    # always written ГАЗ (an acronym).
    "газ",
})


def _has_cjk(s: str) -> bool:
    return any(ord(ch) >= 0x2E80 for ch in s)


def _brand_matcher(
    brands: list[BrandDomainEntry],
) -> tuple[re.Pattern[str] | None, re.Pattern[str] | None, dict[str, str]]:
    """(word_pattern, cjk_pattern, name→brand). Either pattern may be None.

    CJK aliases (智己, 问界…) get their own pattern WITHOUT \\w boundaries:
    Chinese runs unspaced, so in «智己汽车» or "IM智己L6" the neighbouring
    CJK/Latin chars are word chars and (?<!\\w)/(?!\\w) reject the very
    contexts the alias exists for (red-team jul-17). CJK also has no case and
    no Russian declension, so the tail/gate don't apply."""
    key = tuple((b.brand, tuple(b.aliases)) for b in brands)
    cached = _BRAND_RE_CACHE.get(key)
    if cached is None:
        name_to_brand: dict[str, str] = {}
        for b in brands:
            for n in (b.brand, *b.aliases):
                n = (n or "").strip().lower()
                if n:
                    name_to_brand.setdefault(n, b.brand)
        # Longest alternative first: "land rover" must win over a bare "rover"
        # if both are ever registered.
        word_names = sorted(
            (n for n in name_to_brand if not _has_cjk(n)), key=len, reverse=True)
        cjk_names = sorted(
            (n for n in name_to_brand if _has_cjk(n)), key=len, reverse=True)
        word_pat = re.compile(
            r"(?<!\w)(" + "|".join(re.escape(n) for n in word_names) + r")"
            + _RU_INFLECT_TAIL + r"(?!\w)",
            re.IGNORECASE,
        ) if word_names else None
        cjk_pat = re.compile(
            "(" + "|".join(re.escape(n) for n in cjk_names) + ")",
            re.IGNORECASE,
        ) if cjk_names else None
        cached = (word_pat, cjk_pat, name_to_brand)
        _BRAND_RE_CACHE[key] = cached
    return cached


def _mentions_brand(text: str, brands: list[BrandDomainEntry]) -> set[str]:
    """Brands named in the text: word boundaries + RU case-ending tolerance +
    a capitalisation gate for ordinary-word brand names.

    Was a plain substring test, which fired on any brand name buried inside an
    unrelated word — measured jul-17 on live rows: "Lada AzIMut" matched brand
    "IM Motors", "progRAM" matched Ram, Cyrillic «скЛАДА» matched Lada, and
    «дилеРАМ» matched Ram. That feeds Tier 2 (brand-owned link + brand
    mentioned → HIGH), so one bogus hit plus that brand's link anywhere on the
    page promotes a wrong primary — the same "junk wins as the source" class
    as the akismet bug. Matched over the ORIGINAL text (case-insensitively) so
    the ambiguous-name gate can inspect real capitalisation.
    """
    word_pat, cjk_pat, name_to_brand = _brand_matcher(brands)
    hit: set[str] = set()
    if not name_to_brand:
        return hit  # empty brand list → a bare alternation would match ""
    if word_pat is not None:
        for m in word_pat.finditer(text):
            raw = m.group(1)
            name = raw.lower()
            if name in _AMBIGUOUS_BRAND_NAMES and raw == name:
                continue  # ordinary word in running prose, not a brand mention
            hit.add(name_to_brand[name])
    if cjk_pat is not None:
        for m in cjk_pat.finditer(text):
            hit.add(name_to_brand[m.group(1).lower()])
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


def _is_deep_url(url: str) -> bool:
    """True when the URL points at a specific article, not an outlet homepage
    or a shallow section index. «rg.ru/» / «rg.ru/news» → shallow;
    «rg.ru/2026/07/03/some-slug.html» → deep. The editor wants the article."""
    p = urlparse(url)
    path = (p.path or "").rstrip("/")
    if not path or path == "":
        return False
    segs = [s for s in path.split("/") if s]
    if not segs:
        return False
    last = segs[-1]
    # a slug (hyphens) or a date/id path, or ≥2 path segments = article-deep;
    # a single short word ("news", "auto") is a section index, not an article.
    return ("-" in last or last.isdigit() or last.endswith((".html", ".htm"))
            or len(segs) >= 2 or len(last) >= 16)


def _same_site(candidate_domain: str, article_domain_norm: str) -> bool:
    """True when the candidate is the article's own site INCLUDING its
    subdomains (media.ixbt.com vs ixbt.com). The old exact-equality check let
    a site's own CDN/media subdomain through as an 'external' primary."""
    n = _normalise_domain(candidate_domain)
    return (n == article_domain_norm
            or n.endswith("." + article_domain_norm)
            or article_domain_norm.endswith("." + n))


def _is_preferred_primary(candidate_domain: str) -> bool:
    """True when the candidate is an editor-named preferred primary, matching a
    listed host OR any subdomain of it (publication.pravo.gov.ru → the listed
    pravo.gov.ru). gov.ru agencies serve documents off deep subdomains, so an
    exact-set membership check would miss them. The suffix is anchored on '.'
    so a listed 'pravo.gov.ru' never matches 'pravo.gov.ru.evil.com'."""
    n = _normalise_domain(candidate_domain)
    return any(n == h or n.endswith("." + h) for h in _PREFERRED_PRIMARY_HOSTS)


# Pure infrastructure hosts — domain registrars / web hosting. These appear
# in site footers ("сделано на …", "хостинг …") and are NEVER an article's
# primary source. Deliberately minimal + unambiguous: only providers that
# cannot be an automotive news source (no exchanges/rating agencies here —
# those CAN be a real source for a финансовой/статистической новости).
_INFRA_HOSTS: frozenset[str] = frozenset({
    "reg.ru", "nic.ru", "sweb.ru", "reg.cloud", "timeweb.com", "beget.com",
    "hostland.ru", "masterhost.ru",
    # CMS / plugin service hosts. Every WordPress site with comments carries an
    # akismet.com link ("Learn how your comment data is processed") and most
    # carry gravatar/wp.com asset links — page furniture on thousands of blogs,
    # never a source. jul-17: akismet.com/privacy/ was promoted to primary on a
    # thesupercarblog post because it was the only external link on the page.
    "akismet.com", "gravatar.com", "automattic.com", "wordpress.org",
    "wordpress.com", "wp.com", "w.org", "jetpack.com",
})

# Tier-1 Russian outlets. When an aggregator links out to one of these, it is a
# real candidate for "who actually reported this" — the editor asks for exactly
# this attribution («первоисточник gazeta.ru…», «Первоисточник 1prime.ru…»,
# jul-16 feedback rows 20/32), and the jul-17 fire-rate measurement found the
# arbiter silently missing them (finmarket.ru → kommersant/vedomosti/interfax
# never fired because no tier-1 RU outlet was a recognised anchor).
#
# DELIBERATELY NOT in _PREFERRED_PRIMARY_HOSTS: that set drives Tier 1.5, which
# promotes a link deterministically at HIGH with no LLM — an iz.ru piece linking
# tass.ru in a «читайте также» block would then get tass as its source. These
# hosts ONLY make the LLM arbiter FIRE, so it can read the body and decide.
# Every deterministic tier is untouched.
#
# rbc.ru is deliberately ABSENT: autonews.ru is an RBC property and carries
# auth./cash./id.rbc.ru nav on every page — measured jul-17, including rbc.ru
# false-fires the arbiter on autonews' own chrome. (Same-site self-links are
# handled by _same_site. NB: the _NAV_BOILERPLATE_MIN frequency filter does
# NOT protect against tass/interfax/1prime chrome — those are also
# press_release_hosts, which _is_infra_or_nav exempts — so the strong-anchor
# check below additionally requires a DEEP link for tier-1 hosts.)
_RU_TIER1_OUTLETS: frozenset[str] = frozenset({
    "kommersant.ru", "vedomosti.ru", "iz.ru", "interfax.ru", "tass.ru",
    "gazeta.ru", "kp.ru", "rg.ru", "1prime.ru", "life.ru",
})


def _is_ru_tier1_outlet(candidate_domain: str) -> bool:
    """True for a tier-1 RU outlet, matched on the registrable apex so
    subdomains count (www.rg.ru, m.gazeta.ru)."""
    n = _normalise_domain(candidate_domain)
    return any(n == h or n.endswith("." + h) for h in _RU_TIER1_OUTLETS)


# A domain repeated this many times across ONE article's outbound links is the
# site's own ecosystem chrome (autonews.ru, an RBC property, links rbc.ru +
# *.rbc.ru ~40×/page), never the article's source — a real source is cited
# once or twice. Threshold set high so a genuinely multiply-linked source is
# never touched; known-good primary hosts are exempt regardless (below).
_NAV_BOILERPLATE_MIN = 8


def _is_infra_or_nav(
    domain: str,
    freq: Counter[str],
    press_release_hosts: frozenset[str] | set[str],
) -> bool:
    """Drop a candidate that is pure infra, or high-frequency navigation
    boilerplate — but NEVER a known-good primary host (press-release /
    preferred / official), which must survive whatever its frequency."""
    nd = _normalise_domain(domain)
    if not nd:
        return False
    if (_press_release_host(nd, press_release_hosts)
            or _is_preferred_primary(nd) or _is_official_primary(nd)):
        return False  # known-good source — never filter
    if nd in _INFRA_HOSTS or any(nd.endswith("." + h) for h in _INFRA_HOSTS):
        return True
    return freq.get(nd, 0) >= _NAV_BOILERPLATE_MIN


def _is_official_primary(candidate_domain: str) -> bool:
    """True for an official/regulatory body (gov, АЕБ, autostat, NHTSA, NCAP) —
    subdomain-aware, same anti-spoof anchoring as _is_preferred_primary. These
    are promoted from ANY article, so the check is deliberately narrow."""
    n = _normalise_domain(candidate_domain)
    return any(n == h or n.endswith("." + h) for h in _OFFICIAL_PRIMARY_HOSTS)


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
    # or a source-marker attribute) AND that link is DEEP (points at the actual
    # source article, not just the outlet's homepage). Editor (04.07): a bare
    # «rg.ru/» is useless — she needs «rg.ru/2026/…/article». Most RU
    # aggregators (auto.mail) only credit the domain root, so this fires rarely
    # — the honest gate, not a fake root attribution.
    if source_hint_url and _matches_official_prefix(source_hint_url):
        # An editor-designated official channel marked as the source wins even
        # when its host is a generic "mirror/social" (t.me): sergtselikov IS
        # the Автостат primary, not a repost. Must precede the mirror gate.
        return source_hint_url, domain_of(source_hint_url), "high"
    if source_hint_url and _is_deep_url(source_hint_url):
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

    # Filter out mirror posts, junk URLs, infra hosts, and navigation
    # boilerplate (a domain repeated across the page's chrome). Frequency is
    # counted on the RAW list before filtering so the site-ecosystem count is
    # intact; known-good primary hosts are exempt inside _is_infra_or_nav.
    _dom_freq: Counter[str] = Counter(
        _normalise_domain(domain_of(link)) for link in outbound_links if link)
    outbound_links = [
        link for link in outbound_links
        if link
        and not _is_junk_link(link)
        and not _is_mirror(domain_of(link), cues.mirror_hosts)
        and not _is_infra_or_nav(domain_of(link), _dom_freq,
                                 cues.press_release_hosts)
    ]

    # Tier 1 — press-release host in outbound links.
    for link in outbound_links:
        d = domain_of(link)
        if _same_site(d, article_domain):
            continue
        if _press_release_host(d, cues.press_release_hosts):
            return link, d, "high"

    # Tier 1.4 — official/regulatory body linked DEEP from ANY article. A
    # government decree (publication.pravo.gov.ru/document/…), an NHTSA recall
    # report, or an АЕБ/АВТОСТАТ statistics page is the primary regardless of
    # who reports it (editor 06.07: «Первоисточник publication.pravo.gov.ru…»
    # on a news.drom.ru row — drom is a real outlet, not a redistributor, so
    # the redistribution-gated Tier 1.5 below never fires for it). Deep-gated
    # so a bare homepage link (nhtsa.gov, autostat.ru) is not mistaken for an
    # article; narrow official whitelist so this never over-promotes a
    # related-reading link the way the full preferred set could.
    for link in outbound_links:
        d = domain_of(link)
        if _same_site(d, article_domain):
            continue
        if _is_official_primary(d) and _is_deep_url(link):
            return link, d, "high"
        # Path-scoped officials (t.me/sergtselikov/<post>) — the host alone is
        # a redistribution host, so this must match the URL prefix, not d.
        if _matches_official_prefix(link):
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
            if _is_preferred_primary(d):
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

    # (A former Tier 5 attributed a redistributor's TEXT/SLUG-named source as a
    # DOMAIN ROOT — «сообщает Autonews.ru» -> autonews.ru/. Removed 04.07: the
    # editor needs a DEEP link to the actual article, and a bare domain root is
    # useless. A text mention only yields the outlet name, never the article
    # URL, so it cannot satisfy that — the deep link, when it exists, comes from
    # the body-link tiers above or the corpus-earliest pass, i.e. from actually
    # crawling the original outlet ourselves.)

    # Fallback — the article itself is the primary source.
    return article_url, domain_of(article_url), "low"


# --- LLM arbitration for contested "link-soup" primaries -------------------
# The heuristic tiers above pick ONE source deterministically, but on a
# redistribution portal an article often links to several plausible primaries
# at once — e.g. autonews.ru reposts a brand story that links BOTH to the
# brand's own site (jelandrus.ru) AND to a Western outlet's related-reading
# piece (carscoops.com). Tier 1.5 blindly promotes the preferred journalistic
# link; Tier 2 would have picked the brand site. Which is the TRUE primary is a
# judgment call the heuristics can't make (editor: «не видит первоисточник»), so
# we hand the contested candidates to a cheap LLM arbiter that reads the body
# (AnthropicLLMClient.pick_primary_source). This helper returns the URLs worth
# arbitrating, or [] when the pick is unambiguous — so the common case never
# spends an LLM call. detect_primary_source itself is untouched: on every
# non-redistribution source, and whenever this returns [], behaviour is exactly
# as before (zero regression to the tier tests).
def arbitration_candidates(
    *,
    article_url: str,
    body: str,
    title: str,
    outbound_links: list[str],
    brands: list[BrandDomainEntry],
    cues: PrimarySourceCues,
    whitelist_domains: set[str] | None = None,
    max_candidates: int = 6,
) -> list[str]:
    """Distinct external URLs worth LLM primary-source arbitration, or [].

    Fires ONLY for the genuinely-contested case: a redistribution-host article
    that links out to ≥2 distinct *plausible* primaries, at least one of them a
    *strong* signal (a recognised journalistic/official host, or a brand-owned
    site for a brand the article actually discusses). A lone candidate is not a
    contest — the deterministic tiers already pick it correctly — so it returns
    []. Uses the SAME junk/mirror/infra/nav filtering as detect_primary_source,
    so nav chrome and share buttons never reach the LLM. At most
    ``max_candidates`` URLs, one (the deepest) per external domain."""
    article_domain = _normalise_domain(domain_of(article_url))
    # Redistribution-gated: reposts are where the "which link is the source"
    # ambiguity lives. Real outlets keep their deterministic tier behaviour and
    # never trigger a paid arbitration call.
    if article_domain not in _REDISTRIBUTION_HOSTS:
        return []

    mentioned = _mentions_brand(title + "\n" + body, brands)
    _dom_freq: Counter[str] = Counter(
        _normalise_domain(domain_of(link)) for link in outbound_links if link)

    best: dict[str, str] = {}  # domain -> deepest candidate URL for it
    has_strong = False
    for link in outbound_links:
        if not link or _is_junk_link(link):
            continue
        d = domain_of(link)
        nd = _normalise_domain(d)
        if _is_mirror(d, cues.mirror_hosts):
            continue
        if _is_infra_or_nav(d, _dom_freq, cues.press_release_hosts):
            continue
        if _same_site(d, article_domain):
            continue
        brand = _matches_brand(d, brands)
        strong = (
            _is_preferred_primary(nd)
            or _is_official_primary(nd)
            # Tier-1 RU outlets count as strong ONLY via a DEEP article link.
            # tass/interfax/1prime are also press_release_hosts, which
            # _is_infra_or_nav exempts from the nav-frequency filter — so a
            # shallow section link in a sister-site's page chrome
            # (ria → tass.ru/ekonomika) would otherwise set has_strong and
            # burn an arbitration call on pure nav (red-team jul-17). Every
            # measured target case (finmarket→kommersant/vedomosti,
            # auto.mail→gazeta) was a deep article link, so nothing is lost.
            or (_is_ru_tier1_outlet(nd) and _is_deep_url(link))
            or (brand is not None and brand.brand in mentioned)
        )
        # A "plausible primary" is a strong signal OR any deep external article
        # link (the original the tiers may have overlooked). Shallow/homepage
        # links that aren't a known brand/outlet are related-reading noise.
        if not (strong or _is_deep_url(link)):
            continue
        if strong:
            has_strong = True
        prev = best.get(nd)
        if prev is None or (not _is_deep_url(prev) and _is_deep_url(link)):
            best[nd] = link

    # Need a genuine contest: ≥2 distinct plausible primaries AND at least one
    # strong anchor (a source-vs-related tension), not generic link soup.
    if not has_strong or len(best) < 2:
        return []
    return list(best.values())[:max_candidates]


# --- Level 2: earliest appearance in our own corpus ------------------------
# (Removed jul-06 cost-audit: _TITLE_SUFFIX_RE was dead — the real title-suffix
# stripping lives in fuzzy_match._TRAILING_SOURCE_RE.)


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
    source_hint_domain: str = "",
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

    # Credited-source shortcut: the article EXPLICITLY named an outlet
    # («источник: rg.ru») but linked only its homepage. If that outlet's
    # actual article is in our corpus (we crawled it too), use its DEEP url —
    # the credit is authoritative for WHICH outlet, so we ignore the
    # earlier-timestamp gate here and just require a strong title match.
    hint = _normalise_domain(source_hint_domain) if source_hint_domain else ""
    if hint and not _same_site(hint, _normalise_domain(target_domain)):
        best_hint: tuple[CorpusEntry, float] | None = None
        for entry in corpus:
            if entry.url == article_url:
                continue
            if not _same_site(entry.domain, hint):
                continue
            ratio = fuzz.token_set_ratio(target_norm, normalise_title(entry.title))
            if ratio >= threshold and (best_hint is None or ratio > best_hint[1]):
                best_hint = (entry, ratio)
        if best_hint is not None:
            e = best_hint[0]
            return e.url, e.domain, "high"
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
