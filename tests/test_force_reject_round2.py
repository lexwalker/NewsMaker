"""Round-2 force-reject patterns from Лист7 (apr-2026 second editorial pass).

Each test = one editor-flagged headline from Лист7 rows 168+. When a
test fails it means a regression of an editor-flagged routing.
"""

from news_agent.core.config_loader import Blacklist, BrandDomainEntry
from news_agent.core.heuristic_relevance import (
    blacklist_hit,
    is_supplier_abstract_showcase,
)
from news_agent.core.models import RawArticle


BRANDS = [
    BrandDomainEntry(brand="BMW", domains=["bmw.com"]),
    BrandDomainEntry(brand="Toyota", domains=["toyota.com"]),
    BrandDomainEntry(brand="Hyundai", domains=["hyundai.com"]),
]
EMPTY_BL = Blacklist()


def _force_rejects(title: str) -> bool:
    raw = RawArticle(
        url="https://example.com/news",
        title=title,
        body="...",
        source_name="s",
        source_url="https://example.com/",
    )
    return blacklist_hit(raw, EMPTY_BL, brands=BRANDS).hit


# -------------------------------------------------- A. Supplier showcase

def test_supplier_showcase_bosch_at_auto_china_rejected() -> None:
    """Лист7 row 168."""
    assert is_supplier_abstract_showcase(
        "Bosch presents three key technologies for intelligent mobility at Auto China 2026"
    )


def test_supplier_showcase_minieye_at_beijing_rejected() -> None:
    """Лист7 row 172."""
    assert is_supplier_abstract_showcase(
        "MINIEYE showcased full-range evolution at 2026 Beijing Auto Show"
    )


def test_supplier_showcase_hangsheng_rejected() -> None:
    """Лист7 row 176."""
    assert is_supplier_abstract_showcase(
        "Hangsheng revealed three core technology foundations at Beijing Auto Show"
    )


def test_supplier_showcase_aumovio_rejected() -> None:
    """Лист7 row 179 (variant: order news at motorshow)."""
    assert is_supplier_abstract_showcase(
        "AUMOVIO unveiled three solutions for OLED displays at Auto China 2026"
    )


def test_supplier_showcase_eastman_rejected() -> None:
    """Лист7 row 177."""
    assert is_supplier_abstract_showcase(
        "Eastman debuts at Auto China 2026 with advanced automotive solutions"
    )


def test_brand_at_motorshow_NOT_supplier_rejected() -> None:
    """BMW showcasing tech at Beijing IS news — brand override."""
    assert not is_supplier_abstract_showcase(
        "BMW unveiled new electric platform technologies at Beijing Auto Show"
    )


def test_supplier_at_random_event_NOT_rejected() -> None:
    """Supplier news outside motorshow context is NOT auto-rejected."""
    assert not is_supplier_abstract_showcase(
        "Bosch presents three key technologies at private investor day"
    )


def test_specific_product_NOT_supplier_pattern() -> None:
    """Specific product launch (not abstract noun) is NOT rejected."""
    assert not is_supplier_abstract_showcase(
        "Bosch presented new ABS-X1 module at Beijing Auto Show"
    )


# -------------------------------------------------- B. NASCAR / DTM / Rally

def test_nascar_rejected() -> None:
    """Лист7 row 192."""
    assert _force_rejects("Multi-car crash occurs during NASCAR race in the U.S.")


def test_dtm_rejected() -> None:
    """Лист7 row 235."""
    assert _force_rejects("Mercedes-AMG and Maro Engel celebrate DTM season opener")


def test_indy500_rejected() -> None:
    assert _force_rejects("Toyota wins Indy 500 qualifying round")


def test_le_mans_rejected() -> None:
    assert _force_rejects("Toyota Le Mans podium finish announced")


# -------------------------------------------------- C. Recommendations

def test_nhtsa_guidelines_rejected() -> None:
    """Лист7 row 269."""
    assert _force_rejects("NHTSA publishes guidelines for car seats and booster seats")


def test_nhtsa_safety_standards_rejected() -> None:
    """Лист7 row 270."""
    assert _force_rejects(
        "NHTSA publishes safety standards for electric and hybrid vehicles"
    )


def test_avtovaz_named_common_mistakes_rejected() -> None:
    """Лист7 row 263."""
    assert _force_rejects("AvtoVAZ named common braking mistakes made by drivers")


def test_experts_recommend_rejected() -> None:
    assert _force_rejects("Experts recommend changing oil every 10,000 km")


def test_russian_expert_named_rejected() -> None:
    assert _force_rejects("Эксперт назвал лучшие автомобили для зимы")


# -------------------------------------------------- D. Off-topic gov/economy

def test_staff_shortage_rejected() -> None:
    """Лист7 row 180."""
    assert _force_rejects("Staff shortage in Russia increased over five years")


def test_silver_trading_rejected() -> None:
    """Лист7 row 182."""
    assert _force_rejects("Norilsk Nickel completed first silver trading deal")


def test_gas_prices_europe_rejected() -> None:
    """Лист7 row 183."""
    assert _force_rejects("Gas prices in Europe approached USD 550")


def test_rotavirus_rejected() -> None:
    """Лист7 row 190."""
    assert _force_rejects("Rotavirus disguised as food poisoning poses threat")


def test_monastery_rejected() -> None:
    """Лист7 row 224."""
    assert _force_rejects("600th anniversary of Solovetsky monastery founding")


def test_rocket_engine_rejected() -> None:
    """Лист7 row 253."""
    assert _force_rejects("U.S. tested most powerful rotational detonation rocket engine")


def test_credit_rating_rejected() -> None:
    """Лист7 row 266."""
    assert _force_rejects(
        "NKR assigned expected credit ratings to exchange bond issues"
    )


def test_taxi_fares_rejected() -> None:
    """Лист7 row 187."""
    assert _force_rejects("Taxi fares doubled in Moscow due to bad weather")


# -------------------------------------------------- E. Custom builds / DIY

def test_custom_styling_rejected() -> None:
    """Лист7 row 232."""
    assert _force_rejects("BMW M6 on gold HRE wheels features custom styling")


def test_steam_motorcycle_rejected() -> None:
    """Лист7 row 240."""
    assert _force_rejects(
        "Briton built steam-powered motorcycle that became world's second-fastest bike"
    )


# -------------------------------------------------- F. Smartphone tech

def test_dimensity_smartphone_rejected() -> None:
    """Лист7 row 211."""
    assert _force_rejects(
        "MediaTek introduced Dimensity 7450 processors for smartphones"
    )


def test_foldable_phone_rejected() -> None:
    assert _force_rejects("Samsung introduced new foldable smartphone with Snapdragon")


# -------------------------------------------------- G. Carsharing reverted

def test_carsharing_NOT_force_rejected() -> None:
    """Лист7 row 258 / chat feedback: carsharing IS posted (as Local).
    The previous force-reject was wrong; this regression test ensures
    we don't re-add it.
    """
    raw = RawArticle(
        url="https://example.com/news",
        title="Yandex Drive carsharing expanded fleet in Moscow",
        body="...",
        source_name="s",
        source_url="https://example.com/",
    )
    assert not blacklist_hit(raw, EMPTY_BL, brands=BRANDS).hit


def test_ride_sharing_NOT_force_rejected() -> None:
    raw = RawArticle(
        url="https://example.com/news",
        title="Uber expanded ride-sharing service in Moscow",
        body="...",
        source_name="s",
        source_url="https://example.com/",
    )
    assert not blacklist_hit(raw, EMPTY_BL, brands=BRANDS).hit


# -------------------------------------------------- regression: legit news

def test_legit_news_round2_not_rejected() -> None:
    """v22 baseline cases must still pass after round-2 changes."""
    legit = [
        "Hyundai unveiled the new Grandeur with premium finish",
        "AvtoVAZ improves Lada quality",
        "Refreshed Haval H9 launched in China",
        "BMW M2 won Moscow drivers' choice award 2026",   # award still ok subject-wise
        "Volkswagen Unyx 08 debut at Beijing Motor Show",  # single-model debut
        "Tesla Roadster gets manual controls",
        "Chery registered new brand in Russia",
        "BAIC Group and CATL deepen strategic cooperation",  # row 175: «постим»
        "Xiaomi opened research center in Munich",  # row 216: «постим»
    ]
    for t in legit:
        assert not _force_rejects(t), f"False-rejected legit headline: {t!r}"
