"""Source-tier classification — deterministic primary-URL selection.

Why this module exists
----------------------
In the may-2026 editor audit, the most-frequent complaint after "дубль"
was "постили пресс" / "нужен англ" — 35 of 197 jalob in one batch (18%).
Both come from the same root cause: when a cluster has the OEM press
release AND a Russian repost, the pipeline used to pick whichever came
into the crawl first, not whichever was the canonical source.

This module gives every URL a deterministic tier number (lower =
higher priority). The cluster builder then picks the lowest-tier
member as canonical, with publication time as a tie-breaker.

Tier ladder (in order of editorial preference)
-----------------------------------------------
  0  OEM-for-brand          Press host belonging to the article's brand
                            (media.mercedes-benz.com when article is
                            about Mercedes). Highest possible signal.
  1  Regulator              NHTSA, Mintrans, DPMA, USPTO, NCAP, etc.
                            Authoritative for recalls / patents / safety.
  2  Press-release host     Any OEM press host even if we can't confirm
                            brand match (e.g. multi-brand groups).
  3  Industry EN primary    Carscoops, Motor1, Autoevolution, CNEVpost,
                            TheKoreanCarBlog — the editor explicitly
                            named these as preferred over RU reposts.
  4  Whitelist              Editor-trusted miscellaneous (config).
  5  Industry RU primary    Kolesa, Autoreview, Motorpage — they often
                            do original reporting but lag the English
                            sources by hours-to-days.
  6  RU aggregator          SpeedMe, NaAvtotrasse, Auto.mail.ru, RIA,
                            TASS — explicitly flagged by the editor as
                            "never use as primary if EN exists".
  7  Mirror / social        t.me, vk.com, telegra.ph — never canonical.

The tier list is intentionally NARROW. Only domains the editor named
explicitly (in 895 audited comments) are in the buckets. Unknown
domains land at tier 4 (the default middle) — not penalised, but not
trusted over the named EN primaries.

Usage
-----
    from news_agent.core.source_priority import domain_tier
    t = domain_tier("media.mercedes-benz.com",
                    brand_canonical="Mercedes-Benz")  # → 0
    t = domain_tier("speedme.ru")                     # → 6
    t = domain_tier("unknown-site.com")               # → 4

The cluster builder calls this on every cluster member and sorts by
``(tier, published_at)`` to choose canonical / primary URL.

Maintenance
-----------
When the editor names a NEW preferred-or-rejected source, add it to
the relevant frozenset below. No code changes required. Run
``tests/test_source_priority.py`` after any addition.
"""

from __future__ import annotations

import re

from news_agent.core.brand_canonical import get_brand_domains

# ── Tier 1: Regulator / official body ─────────────────────────────────
REGULATOR_DOMAINS: frozenset[str] = frozenset({
    "nhtsa.gov", "static.nhtsa.gov",
    "mintrans.gov.ru",
    "dpma.de",
    "uspto.gov",
    "europa.eu", "ec.europa.eu",
    "ancap.com.au", "euroncap.com",
    "iihs.org",
    "government.ru",
    "lada.ru",  # OEM, but Lada press releases are gov-adjacent in RU
})

# ── Tier 2: Generic press-release / OEM-group hosts ──────────────────
# These are OEM-owned but the article may not name a specific brand
# we can match. Example: media.stellantis.com hosts Jeep, Ram, Fiat,
# etc. — when we can't confirm which brand, still trust it as primary.
GENERIC_PRESS_HOSTS: frozenset[str] = frozenset({
    "media.stellantis.com", "stellantis.com",
    "media.gm.com", "gm.com",
    "media.ford.com",
    "press.bmwgroup.com", "bmwgroup.com",
    "media.daimler.com",
    "newsroom.honda.com", "hondanews.com",
    "press.toyota.com", "pressroom.toyota.com",
    "media.nissan-europe.com",
    "media.subaru.com",
    "audi-mediacenter.com",
    "media.mercedes-benz.com", "group.mercedes-benz.com",
    "volkswagen-newsroom.com",
    "skoda-storyboard.com",
    "media.lotuscars.com",
    "media.polestar.com",
    "media.lucidmotors.com",
    "media.astonmartin.com",
    "media.mclaren.com",
    "media.ineosgrenadier.com",
    "bentleymedia.com",
    "press.rolls-roycemotorcars.com",
    "global.honda",
    "global.nissannews.com", "nissannews.com",
    "media.mbusa.com",
})

# ── Tier 3: Industry EN primary ──────────────────────────────────────
INDUSTRY_EN_PRIMARY: frozenset[str] = frozenset({
    "carscoops.com",
    "autoevolution.com",
    "motor1.com",
    "motortrend.com",
    "cnevpost.com",
    "carnewschina.com",
    "thekoreancarblog.com",
    "electrek.co",
    "businesskorea.co.kr",
    "topauto.co.za",
    "carexpert.com.au",
    "drive.com.au",
    "wardsauto.com",
    "automotivenews.com",
    "autocarindia.com",
    "carbuzz.com",
    "thedrive.com",
    "carsdirect.com",
    "cleantechnica.com",
})

# ── Tier 5: Industry RU primary ──────────────────────────────────────
INDUSTRY_RU_PRIMARY: frozenset[str] = frozenset({
    "kolesa.ru",
    "autoreview.ru",
    "motorpage.ru",
    "autonews.ru",
    "behindthewheel.ru",
    "5koleso.ru",
    "drive2.ru",
    "zr.ru",
    "ixbt.com",  # tech-leaning but does original automotive reporting
})

# ── Tier 6: RU aggregators / reposters ──────────────────────────────
RU_AGGREGATORS: frozenset[str] = frozenset({
    "speedme.ru",
    "naavtotrasse.ru",
    "auto.mail.ru", "mail.ru",
    "1prime.ru", "ria.ru", "tass.ru", "iz.ru",
    "interfax.ru", "finmarket.ru",
    "asroad.org",
    "quto.ru",
    "avtonovostidnya.ru",
    "truesharing.ru",
    "napinfo.ru",
})

# ── Tier 7: Mirror / social ──────────────────────────────────────────
MIRROR_HOSTS: frozenset[str] = frozenset({
    "t.me", "telegra.ph",
    "vk.com", "ok.ru",
    "facebook.com", "instagram.com",
    "youtube.com", "youtu.be",
    "twitter.com", "x.com",
    "max.ru",
    "rutube.ru",
})

# Default for unknown domains. Calibrated to land between "industry
# EN primary" and "industry RU primary" so:
#   • Unknown English-language domain wins over RU aggregator (good).
#   • Unknown RU-language site loses to known EN primary (good).
# We DO NOT try to be clever about TLD — TLD-based heuristics produced
# false positives in earlier prototypes (.co.uk for Indian press, etc.).
DEFAULT_UNKNOWN_TIER = 4


_SUBDOMAIN_STRIP = re.compile(r"^(?:www|m|amp)\.")


def _normalise(d: str) -> str:
    """Strip www./m./amp. prefix and lowercase."""
    return _SUBDOMAIN_STRIP.sub("", (d or "").lower().strip())


def _matches_any(d: str, host_set: frozenset[str]) -> bool:
    """True if `d` is exactly any host in the set or a sub-host of one."""
    if not d:
        return False
    for h in host_set:
        if d == h or d.endswith("." + h):
            return True
    return False


def domain_tier(domain: str, *, brand_canonical: str = "") -> int:
    """Return tier 0-7 for ``domain``. Lower = higher primary priority.

    See module docstring for ladder. When ``brand_canonical`` is provided
    and the domain is a press host of THAT brand, we bump to tier 0
    (the strongest possible primary signal).
    """
    d = _normalise(domain)
    if not d:
        return DEFAULT_UNKNOWN_TIER

    # Tier 0 — OEM domain matches article's brand
    if brand_canonical:
        for od in get_brand_domains(brand_canonical):
            nod = _normalise(od)
            if d == nod or d.endswith("." + nod):
                return 0

    # Tier 1 — regulator
    if _matches_any(d, REGULATOR_DOMAINS):
        return 1

    # Tier 2 — generic press host (OEM group, brand unconfirmed)
    if _matches_any(d, GENERIC_PRESS_HOSTS):
        return 2

    # Tier 3 — industry EN primary
    if _matches_any(d, INDUSTRY_EN_PRIMARY):
        return 3

    # Tier 5 — industry RU primary (tier 4 reserved for whitelist below)
    if _matches_any(d, INDUSTRY_RU_PRIMARY):
        return 5

    # Tier 6 — RU aggregator
    if _matches_any(d, RU_AGGREGATORS):
        return 6

    # Tier 7 — mirror
    if _matches_any(d, MIRROR_HOSTS):
        return 7

    return DEFAULT_UNKNOWN_TIER  # 4


def is_aggregator(domain: str) -> bool:
    """Convenience: ``True`` if domain is a known RU repost host.

    Used by callers that want to suppress these domains from being
    selected as primary, independent of the full tier comparison.
    """
    return _matches_any(_normalise(domain), RU_AGGREGATORS)


def is_oem_press(domain: str, brand_canonical: str = "") -> bool:
    """``True`` if domain is an OEM press host (tier 0 or 2)."""
    d = _normalise(domain)
    if not d:
        return False
    if brand_canonical:
        for od in get_brand_domains(brand_canonical):
            nod = _normalise(od)
            if d == nod or d.endswith("." + nod):
                return True
    return _matches_any(d, GENERIC_PRESS_HOSTS)
