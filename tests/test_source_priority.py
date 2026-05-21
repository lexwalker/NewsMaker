"""Coverage for news_agent.core.source_priority — primary-URL tiering.

Each test maps to a real editor complaint pattern. If the editor in a
future audit names a new "wrong primary" pair, add it as a regression
test here (keeping the comment that names the original case).
"""

from news_agent.core.source_priority import (
    DEFAULT_UNKNOWN_TIER,
    domain_tier,
    is_aggregator,
    is_oem_press,
)


# ── Tier 0: OEM-for-brand wins absolutely ────────────────────────────

def test_oem_for_brand_is_tier_zero() -> None:
    """media.mercedes-benz.com on a Mercedes article = best primary."""
    assert domain_tier("media.mercedes-benz.com",
                        brand_canonical="Mercedes-Benz") == 0
    assert domain_tier("audi-mediacenter.com",
                        brand_canonical="Audi") == 0
    assert domain_tier("global.honda",
                        brand_canonical="Honda") == 0


def test_oem_for_brand_subdomain() -> None:
    """www. / amp. / m. prefixes don't break tier detection."""
    assert domain_tier("www.media.mercedes-benz.com",
                        brand_canonical="Mercedes-Benz") == 0


def test_oem_for_wrong_brand_demotes_to_press_host() -> None:
    """Mercedes domain on a BMW article: not tier-0 (wrong brand)
    but still tier-2 (any OEM press host beats aggregators)."""
    t = domain_tier("media.mercedes-benz.com", brand_canonical="BMW")
    assert t == 2


def test_stellantis_for_jeep_is_tier_zero() -> None:
    """Stellantis press for Jeep article: brand_domains has Stellantis
    as a Jeep domain, so this should be tier 0."""
    assert domain_tier("media.stellantis.com",
                        brand_canonical="Jeep") == 0


# ── Tier 1: Regulator ───────────────────────────────────────────────

def test_nhtsa_tier() -> None:
    """The editor's «нужен пресс NHTSA» pattern (4 cases in v41 audit)."""
    assert domain_tier("static.nhtsa.gov") == 1
    assert domain_tier("nhtsa.gov") == 1


def test_dpma_tier() -> None:
    """Patent office (Porsche air-cooled patent case)."""
    assert domain_tier("register.dpma.de") == 1
    assert domain_tier("dpma.de") == 1


def test_mintrans_tier() -> None:
    """Минтранс press releases."""
    assert domain_tier("mintrans.gov.ru") == 1


# ── Tier 2: Generic press host ───────────────────────────────────────

def test_stellantis_no_brand_hint() -> None:
    """Stellantis press without brand confirmation → still tier 2."""
    assert domain_tier("media.stellantis.com") == 2


def test_audi_mediacenter_no_brand_hint() -> None:
    """OEM press host even without brand match."""
    assert domain_tier("audi-mediacenter.com") == 2


# ── Tier 3: Industry EN ──────────────────────────────────────────────

def test_industry_en() -> None:
    assert domain_tier("carscoops.com") == 3
    assert domain_tier("autoevolution.com") == 3
    assert domain_tier("motor1.com") == 3
    assert domain_tier("cnevpost.com") == 3
    assert domain_tier("thekoreancarblog.com") == 3


# ── Tier 5: Industry RU primary ─────────────────────────────────────

def test_industry_ru() -> None:
    assert domain_tier("kolesa.ru") == 5
    assert domain_tier("autoreview.ru") == 5
    assert domain_tier("motorpage.ru") == 5


# ── Tier 6: RU aggregator ───────────────────────────────────────────

def test_aggregators() -> None:
    """Domains the editor explicitly said «не использовать как
    первоисточник, нужен англ»."""
    assert domain_tier("speedme.ru") == 6
    assert domain_tier("naavtotrasse.ru") == 6
    assert domain_tier("auto.mail.ru") == 6
    assert domain_tier("1prime.ru") == 6
    assert domain_tier("asroad.org") == 6


def test_is_aggregator() -> None:
    assert is_aggregator("speedme.ru")
    assert is_aggregator("www.speedme.ru")
    assert not is_aggregator("carscoops.com")
    assert not is_aggregator("")


# ── Tier 7: Mirror / social ─────────────────────────────────────────

def test_mirrors() -> None:
    assert domain_tier("t.me") == 7
    assert domain_tier("vk.com") == 7
    assert domain_tier("youtube.com") == 7


# ── Tier 4: Unknown default ─────────────────────────────────────────

def test_unknown_default() -> None:
    """Unknown domain lands in the middle — not penalised, not trusted."""
    assert domain_tier("some-random-blog.example") == DEFAULT_UNKNOWN_TIER
    assert domain_tier("xn----7sbbeeptbfadjdvm5ab9bqj.xn--p1ai") \
           == DEFAULT_UNKNOWN_TIER


def test_empty_domain_safe() -> None:
    assert domain_tier("") == DEFAULT_UNKNOWN_TIER
    assert domain_tier(None) == DEFAULT_UNKNOWN_TIER  # type: ignore


# ── The whole point: real cluster comparisons ────────────────────────

def test_v41_mercedes_amg_real_case() -> None:
    """r38 Mercedes-AMG: cluster has media.mercedes-benz.com AND kolesa.
    Editor said r23 («постили пресс») was the dup. We want
    media.mercedes-benz.com to win as primary, not kolesa."""
    press_tier = domain_tier("media.mercedes-benz.com",
                              brand_canonical="Mercedes-AMG")
    kolesa_tier = domain_tier("kolesa.ru",
                               brand_canonical="Mercedes-AMG")
    assert press_tier < kolesa_tier, \
        "Mercedes press host MUST beat kolesa for Mercedes article"
    # Numerically: 0 < 5
    assert press_tier == 0
    assert kolesa_tier == 5


def test_v41_jeep_avenger_real_case() -> None:
    """r258 Jeep Avenger: cluster has media.stellantis.com (canonical
    per editor) AND carscoops.com. Both English, both reasonable
    primaries — but stellantis is OEM so wins."""
    stell = domain_tier("media.stellantis.com", brand_canonical="Jeep")
    carscoops = domain_tier("carscoops.com", brand_canonical="Jeep")
    assert stell < carscoops
    assert stell == 0
    assert carscoops == 3


def test_v41_geely_e5_south_africa() -> None:
    """r21 Geely E5: editor said «постили topauto.co.za». In our
    tiering topauto.co.za is industry-EN (3) — wins over t.me telegram
    (7) which was where we'd been picking it up from."""
    topauto = domain_tier("topauto.co.za")
    telegram = domain_tier("t.me")
    assert topauto < telegram
    assert topauto == 3
    assert telegram == 7


def test_v41_xpeng_robotaxi() -> None:
    """r53 Xpeng robotaxi: editor said «постили cnevpost.com».
    cnevpost (industry-EN) beats autoreview.ru (industry-RU)."""
    cnev = domain_tier("cnevpost.com")
    ar = domain_tier("autoreview.ru")
    assert cnev < ar
    assert cnev == 3
    assert ar == 5


# ── is_oem_press helper ─────────────────────────────────────────────

def test_is_oem_press_brand_match() -> None:
    assert is_oem_press("audi-mediacenter.com", "Audi")
    assert is_oem_press("media.mercedes-benz.com", "Mercedes-Benz")


def test_is_oem_press_generic() -> None:
    """Generic press hosts return True even without brand match."""
    assert is_oem_press("media.stellantis.com")
    assert is_oem_press("media.gm.com")


def test_is_oem_press_negative() -> None:
    assert not is_oem_press("carscoops.com")
    assert not is_oem_press("speedme.ru")
    assert not is_oem_press("")
