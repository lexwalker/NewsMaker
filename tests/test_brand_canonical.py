"""Coverage for news_agent.core.brand_canonical — protects against
brand-key drift between pipeline stages.

Test groups:
  • RENAMES (SsangYong → KGM) — the main reason this module exists
  • SUB-BRAND boundaries (BMW Alpina, Mercedes-AMG, Stellantis kids)
  • CYRILLIC / latin synonyms
  • CASE / WHITESPACE robustness
  • FREE-TEXT extraction (brand embedded in headline)
  • NEGATIVE cases (don't false-fire on substrings)
"""

from news_agent.core.brand_canonical import (
    all_canonical_brands,
    canonicalize_brand,
    get_brand_domains,
)


# ── RENAMES — the original motivation ─────────────────────────────────

def test_ssangyong_to_kgm() -> None:
    """The exact gap that broke v41 dedup on KGM Torres."""
    assert canonicalize_brand("ssangyong") == "KGM"
    assert canonicalize_brand("SsangYong") == "KGM"
    assert canonicalize_brand("Ссангйонг") == "KGM"


def test_kgm_stays_kgm() -> None:
    assert canonicalize_brand("kgm") == "KGM"
    assert canonicalize_brand("KGM") == "KGM"


def test_kgm_torres_in_title() -> None:
    """Free-text extraction — model name shouldn't confuse the matcher."""
    assert canonicalize_brand("KGM Torres EVX update") == "KGM"
    assert canonicalize_brand("SsangYong Tivoli updated") == "KGM"


# ── SUB-BRAND boundaries (preserve editor's mental separation) ─────

def test_bmw_alpina_distinct_from_bmw() -> None:
    """Editor groups Alpina articles separately from BMW M / general."""
    assert canonicalize_brand("BMW Alpina") == "BMW Alpina"
    assert canonicalize_brand("Alpina") == "BMW Alpina"
    assert canonicalize_brand("BMW Alpina Vision concept") == "BMW Alpina"
    # Plain BMW should not become Alpina
    assert canonicalize_brand("BMW M5 spy shots") == "BMW"


def test_mercedes_amg_distinct_from_benz() -> None:
    """Mercedes-AMG GT 4-Door vs Mercedes S-Class are different stories."""
    assert canonicalize_brand("Mercedes-AMG") == "Mercedes-AMG"
    assert canonicalize_brand("mercedes-amg") == "Mercedes-AMG"
    assert canonicalize_brand("AMG GT 4-Door reveal") == "Mercedes-AMG"
    assert canonicalize_brand("Mercedes-Benz S-Class") == "Mercedes-Benz"


def test_stellantis_kids_separate() -> None:
    """Stellantis has Jeep/Ram/Dodge — editor treats each as own bucket."""
    assert canonicalize_brand("Jeep Avenger refreshed") == "Jeep"
    assert canonicalize_brand("Ram Rumble Bee SRT") == "Ram"
    assert canonicalize_brand("Dodge Charger Daytona") == "Dodge"
    assert canonicalize_brand("Chrysler new minivan") == "Chrysler"


# ── CYRILLIC / Latin alias matching ──────────────────────────────────

def test_cyrillic_аliases() -> None:
    assert canonicalize_brand("Мерседес") == "Mercedes-Benz"
    assert canonicalize_brand("ауди") == "Audi"
    assert canonicalize_brand("Хёндэ") == "Hyundai"
    assert canonicalize_brand("Сяоми") == "Xiaomi Auto"
    assert canonicalize_brand("Соллерс") == "Sollers"


def test_volkswagen_vw_alias() -> None:
    """All VW variants should collapse to Volkswagen."""
    assert canonicalize_brand("Vw") == "Volkswagen"
    assert canonicalize_brand("VW") == "Volkswagen"
    assert canonicalize_brand("volkswagen") == "Volkswagen"
    assert canonicalize_brand("Фольксваген") == "Volkswagen"


# ── CASE / WHITESPACE ────────────────────────────────────────────────

def test_case_insensitive() -> None:
    assert canonicalize_brand("FORD") == "Ford"
    assert canonicalize_brand("ford") == "Ford"
    assert canonicalize_brand("Ford") == "Ford"


def test_whitespace_tolerant() -> None:
    assert canonicalize_brand("  ford  ") == "Ford"


# ── FREE-TEXT extraction ─────────────────────────────────────────────

def test_brand_in_long_headline() -> None:
    h = ("Cadillac introduced special version of CT5-V Blackwing "
         "in honor of Formula 1")
    assert canonicalize_brand(h) == "Cadillac"


def test_brand_in_russian_lede() -> None:
    h = "Маленький Maextro S800 Grand Design вышел в Китае на старт продаж"
    assert canonicalize_brand(h) == "Maextro"


def test_longest_alias_wins() -> None:
    """'great wall motor' (alias) is matched, not 'great wall' alone,
    when the longer form is present — both map to Great Wall though."""
    assert canonicalize_brand("Great Wall Motor SUV") == "Great Wall"


# ── NEGATIVE / safety ────────────────────────────────────────────────

def test_empty_input() -> None:
    assert canonicalize_brand("") == ""
    assert canonicalize_brand("   ") == ""


def test_no_brand_text() -> None:
    assert canonicalize_brand("EV demand grows in Europe") == ""


def test_word_boundary_avoid_substring() -> None:
    """'fordable' / 'kgmplc' should NOT match 'ford' / 'kgm'."""
    # A made-up brand name containing 'ford' as substring
    assert canonicalize_brand("affordable pickup") == ""


def test_punctuation_safe() -> None:
    """Common cases the v41 audit revealed."""
    assert canonicalize_brand("Mercedes-AMG GT 4-Door") == "Mercedes-AMG"


# ── Schema sanity ────────────────────────────────────────────────────

def test_all_canonical_present() -> None:
    """Sanity: the gap-fill set is loaded from YAML."""
    canonicals = all_canonical_brands()
    # Must contain the gap-fill brands we just added
    must_have = {
        "KGM", "BMW Alpina", "Mercedes-AMG", "VinFast", "Maextro",
        "Jeep", "Ram", "Dodge", "Chrysler", "Volkswagen",
        "Mercedes-Benz", "Ford", "BMW", "Sollers", "Lotus",
    }
    missing = must_have - canonicals
    assert not missing, f"missing canonicals: {missing}"


def test_get_brand_domains() -> None:
    """OEM press domains come back for canonical brand names."""
    kgm_dom = get_brand_domains("KGM")
    assert "kgm.com" in kgm_dom
    mb_dom = get_brand_domains("Mercedes-Benz")
    assert any("mercedes-benz.com" in d for d in mb_dom)


def test_get_brand_domains_unknown() -> None:
    assert get_brand_domains("ThereIsNoSuchBrand") == []
