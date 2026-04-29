"""Tests for heuristic_section — hard pre-classifier short-circuiting LLM.

Each test reflects a real editor correction from the apr-2026 review of
the persistent 'Новости' sheet. When a test fails, that's a regression
of an editor-flagged routing.
"""

from news_agent.core.heuristic_relevance import heuristic_section


# -------------------------------------------------------- LCV body-type

def test_pickup_in_title_routes_to_lcv() -> None:
    h = heuristic_section(title="New Toyota Hilux pickup unveiled in 2027")
    assert h is not None and h.section == "LCV news"
    assert "body-type" in h.reason


def test_van_in_title_routes_to_lcv() -> None:
    h = heuristic_section(title="Mercedes Sprinter cargo van gets electric variant")
    assert h is not None and h.section == "LCV news"


def test_russian_pickup_routes_to_lcv() -> None:
    h = heuristic_section(title="Соллерс начала производство пикапа в Ульяновске")
    assert h is not None and h.section == "LCV news"


def test_minivan_routes_to_lcv() -> None:
    h = heuristic_section(title="GAC M8 minivan entered service with Moscow firefighters")
    assert h is not None and h.section == "LCV news"


def test_double_decker_bus_routes_to_lcv() -> None:
    # Editor row 153: BYD double-decker bus → reject as adjacent? Actually
    # editor said «не постим, новость от 2024-05-21» — date issue, not topic.
    # But generally LCV. Body-type wins.
    h = heuristic_section(title="BYD unveiled electric double-decker bus BD11 in London")
    assert h is not None and h.section == "LCV news"


def test_passenger_sedan_NOT_lcv() -> None:
    # Sedan / SUV — clearly passenger, must NOT trigger LCV
    assert heuristic_section(title="Lynk & Co 900 sedan launched in China") is None or \
           heuristic_section(title="Lynk & Co 900 sedan launched in China").section != "LCV news"


def test_suv_alone_NOT_lcv() -> None:
    h = heuristic_section(title="Hyundai unveiled the new Tucson SUV")
    assert h is None or h.section != "LCV news"


# ------------------------------------------------- Financial results

def test_financial_results_routes_to_other() -> None:
    h = heuristic_section(title="Nissan reported Q1 financial results for 2026")
    assert h is not None and h.section == "Other news"


def test_operating_loss_routes_to_other() -> None:
    h = heuristic_section(title="Nissan avoided operating loss in fiscal year 2025")
    assert h is not None and h.section == "Other news"


def test_revenue_grew_routes_to_other() -> None:
    h = heuristic_section(title="Toyota revenue grew 12% in Q3 2025")
    assert h is not None and h.section == "Other news"


def test_russian_finrezultat_routes_to_other() -> None:
    h = heuristic_section(title="АвтоВАЗ опубликовал финрезультаты за 2025 год")
    assert h is not None and h.section == "Other news"


# --------------------------------------------------- Awards / премии

def test_award_routes_to_other() -> None:
    h = heuristic_section(title="Škoda Auto wins Fuorisalone Award at Milan Design Week")
    assert h is not None and h.section == "Other news"


def test_russian_award_routes_to_other() -> None:
    h = heuristic_section(title="Hyundai удостоен премии «Внедорожник года»")
    assert h is not None and h.section == "Other news"


# --------------------------------------- RU portal + RU subject = Local

def test_autostat_ru_market_news_routes_to_local() -> None:
    h = heuristic_section(
        title="Прогноз продаж новых легковых автомобилей в России",
        domain="autostat.ru",
    )
    assert h is not None and h.section == "Local specifics"
    assert h.region == "Local"


def test_autostat_global_news_NOT_local() -> None:
    # Even on autostat.ru, a global news (no RU markers) should defer to LLM
    h = heuristic_section(
        title="Tesla Roadster gets manual controls",
        domain="autostat.ru",
    )
    assert h is None  # defer to LLM — no RU subject markers


# ------------------------------------------- Defer cases — LLM still needed

def test_clean_global_model_launch_defers_to_llm() -> None:
    # Real model reveal — must NOT pre-classify; LLM decides Facts vs Other
    h = heuristic_section(title="Hyundai unveiled the new Grandeur with premium finish")
    assert h is None


def test_partnership_defers_to_llm() -> None:
    h = heuristic_section(title="JAC deepens partnership with Huawei in luxury auto segment")
    assert h is None


def test_empty_title_returns_none() -> None:
    assert heuristic_section(title="") is None
    assert heuristic_section(title="   ") is None
