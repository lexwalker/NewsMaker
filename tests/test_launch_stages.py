"""Tests for launch_stages — Phase 1 (heuristic detection).

Includes the actual Geely Galaxy M7 lifecycle from the apr-2026 conversation
that prompted this feature.
"""

from news_agent.core.config_loader import BrandDomainEntry
from news_agent.core.launch_stages import (
    detect_launch_stages,
    extract_brand_model,
)


# Brand list mimicking config/brand_domains.yaml
BRANDS = [
    BrandDomainEntry(brand="Geely", aliases=["Джили"], domains=["geely.com"]),
    BrandDomainEntry(brand="Hyundai", aliases=["Хёндэ", "Хендай"],
                     domains=["hyundai.com"]),
    BrandDomainEntry(brand="Toyota", aliases=["Тойота"], domains=["toyota.com"]),
    BrandDomainEntry(brand="BMW", domains=["bmw.com"]),
    BrandDomainEntry(brand="AvtoVAZ", aliases=["АвтоВАЗ", "ВАЗ"],
                     domains=["lada.ru"]),
    BrandDomainEntry(brand="Audi", domains=["audi.com"]),
]


# ============================================================================
# Stage detection — Geely Galaxy M7 real timeline
# ============================================================================

def test_certification_stage() -> None:
    assert "certification" in detect_launch_stages(
        "В Китае сертифицирован гибридный кроссовер Geely Galaxy M7 EM-i"
    )
    assert "certification" in detect_launch_stages(
        "Geely Galaxy M7 received certification in China"
    )


def test_images_published_stage() -> None:
    assert "images_published" in detect_launch_stages(
        "В Geely опубликовали изображения кроссовера Galaxy M7"
    )
    assert "images_published" in detect_launch_stages(
        "Geely shared first images of Galaxy M7"
    )


def test_debut_announced_stage() -> None:
    assert "debut_announced" in detect_launch_stages(
        "В Geely анонсировали дебют гибридного кроссовера Galaxy M7 в Китае"
    )
    assert "debut_announced" in detect_launch_stages(
        "Galaxy M7 to debut on March 16"
    )


def test_model_unveiled_stage() -> None:
    assert "model_unveiled" in detect_launch_stages(
        "В Geely представили гибридный Galaxy M7 в Китае"
    )
    assert "model_unveiled" in detect_launch_stages(
        "Geely unveiled Galaxy M7 hybrid SUV"
    )


def test_preorders_announced_stage() -> None:
    assert "preorders_announced" in detect_launch_stages(
        "В Geely анонсировали старт приема предзаказов на Galaxy M7"
    )
    assert "preorders_announced" in detect_launch_stages(
        "Geely Galaxy M7 pre-orders to begin on April 9"
    )


def test_preorders_started_stage() -> None:
    assert "preorders_started" in detect_launch_stages(
        "В Китае стартовал прием предзаказов на Geely Galaxy M7"
    )
    assert "preorders_started" in detect_launch_stages(
        "Geely Galaxy M7 pre-orders started in China"
    )


def test_sales_started_stage() -> None:
    assert "sales_started" in detect_launch_stages(
        "В Geely запустили продажи Galaxy M7"
    )
    assert "sales_started" in detect_launch_stages(
        "Geely Galaxy M7 went on sale in China"
    )
    assert "sales_started" in detect_launch_stages(
        "Galaxy M7 sales started in China"
    )


def test_no_stage_in_unrelated_news() -> None:
    """News without any lifecycle phrase returns empty list."""
    assert detect_launch_stages("Toyota Q1 financial results") == []
    assert detect_launch_stages("BMW recalled 100,000 vehicles") == []
    assert detect_launch_stages("AvtoVAZ enters scheduled corporate vacation") == []


def test_multistage_in_one_article() -> None:
    """Multiple stages in one announcement — both detected."""
    stages = detect_launch_stages(
        "В Geely представили Galaxy M7 и стартовал прием предзаказов"
    )
    assert "model_unveiled" in stages
    assert "preorders_started" in stages


def test_empty_input() -> None:
    assert detect_launch_stages("") == []
    assert detect_launch_stages(None) == []  # type: ignore[arg-type]


# ============================================================================
# Brand + model extraction
# ============================================================================

def test_brand_model_after_brand_in_russian() -> None:
    """В Geely анонсировали ... Galaxy M7."""
    bm = extract_brand_model(
        "В Geely анонсировали старт приема предзаказов на Galaxy M7",
        BRANDS,
    )
    assert bm is not None
    assert bm[0] == "Geely"
    assert "Galaxy" in bm[1] and "M7" in bm[1]


def test_brand_model_brand_first_in_english() -> None:
    bm = extract_brand_model("Geely Galaxy M7 sales started in China", BRANDS)
    assert bm is not None
    assert bm[0] == "Geely"
    assert "Galaxy M7" == bm[1]


def test_brand_model_simple_word_model() -> None:
    bm = extract_brand_model("Hyundai unveiled the new Tucson", BRANDS)
    assert bm == ("Hyundai", "Tucson")


def test_brand_model_alphanumeric_short_model() -> None:
    bm = extract_brand_model("Audi Q9 to debut in summer", BRANDS)
    assert bm == ("Audi", "Q9")


def test_brand_model_three_token_model() -> None:
    """Galaxy M7 EM-i — three meaningful tokens."""
    bm = extract_brand_model(
        "В Китае сертифицирован Geely Galaxy M7 EM-i",
        BRANDS,
    )
    assert bm is not None
    assert bm[0] == "Geely"
    # Either "Galaxy M7 EM" or "Galaxy M7 EM-i" depending on tokenisation
    assert "Galaxy" in bm[1] and "M7" in bm[1]


def test_brand_model_no_model_after_brand() -> None:
    """Toyota запустила завод — no model name → None."""
    bm = extract_brand_model("Toyota запустила завод в Тольятти", BRANDS)
    assert bm is None


def test_brand_model_no_brand_in_title() -> None:
    bm = extract_brand_model("Russian car market grew 10% in April", BRANDS)
    assert bm is None


def test_brand_model_short_role_acronym_rejected() -> None:
    """'Hyundai представили нового CEO' — CEO is noise, not a model."""
    bm = extract_brand_model("Hyundai назначили нового CEO", BRANDS)
    assert bm is None


def test_brand_model_geographic_token_rejected() -> None:
    """Toyota Russia — Russia is geographic noise."""
    bm = extract_brand_model("Toyota expands operations in Russia", BRANDS)
    assert bm is None


def test_brand_model_year_only_rejected() -> None:
    """'Geely 2026' — pure year is not a model."""
    bm = extract_brand_model("Geely 2026 plans revealed", BRANDS)
    assert bm is None


def test_brand_model_alias_match() -> None:
    """Cyrillic alias 'Хёндэ' should still resolve to canonical 'Hyundai'."""
    bm = extract_brand_model(
        "В Хёндэ представили новый Tucson",
        BRANDS,
    )
    assert bm is not None
    assert bm[0] == "Hyundai"
    assert "Tucson" in bm[1]


def test_brand_model_avtovaz_cyrillic() -> None:
    bm = extract_brand_model(
        "АвтоВАЗ начал продажи Lada Iskra",
        BRANDS,
    )
    assert bm is not None
    assert bm[0] == "AvtoVAZ"
    # Model should be Lada Iskra OR Iskra (depending on parsing)
    assert "Iskra" in bm[1]


def test_brand_model_body_type_after_model() -> None:
    """'Hyundai unveiled Tucson SUV in Russia' — model = Tucson, not 'Tucson SUV'."""
    bm = extract_brand_model(
        "Hyundai unveiled Tucson SUV in Russia",
        BRANDS,
    )
    assert bm is not None
    assert bm[0] == "Hyundai"
    assert bm[1] == "Tucson"  # SUV is body-type noise, stops capture


def test_brand_model_empty_input() -> None:
    assert extract_brand_model("", BRANDS) is None
    assert extract_brand_model("Some title", []) is None


# ============================================================================
# Combined: stage + brand_model — the actual Geely Galaxy M7 lifecycle
# ============================================================================

def test_geely_galaxy_m7_full_lifecycle() -> None:
    """Real headlines from the apr-2026 timeline screenshot."""
    cases = [
        ("certification",
         "В Китае сертифицирован гибридный кроссовер Geely Galaxy M7 EM-i"),
        ("images_published",
         "В Geely опубликовали изображения кроссовера Galaxy M7"),
        ("debut_announced",
         "В Geely анонсировали дебют гибридного кроссовера Galaxy M7 в Китае"),
        ("model_unveiled",
         "В Geely представили гибридный Galaxy M7 в Китае"),
        ("preorders_announced",
         "В Geely анонсировали старт приема предзаказов на Galaxy M7"),
        ("preorders_started",
         "В Китае стартовал прием предзаказов на Geely Galaxy M7"),
        ("sales_started",
         "В Geely запустили продажи Galaxy M7"),
    ]
    for expected_stage, title in cases:
        stages = detect_launch_stages(title)
        bm = extract_brand_model(title, BRANDS)
        assert expected_stage in stages, (
            f"Stage {expected_stage!r} not detected in: {title!r} (got {stages})"
        )
        assert bm is not None, f"Brand+model not extracted from: {title!r}"
        assert bm[0] == "Geely", f"Expected Geely, got {bm[0]} for: {title!r}"
        assert "M7" in bm[1], f"Expected M7 in model, got {bm[1]!r} for: {title!r}"
