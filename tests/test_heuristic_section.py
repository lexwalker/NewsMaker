"""Tests for heuristic_section — hard pre-classifier short-circuiting LLM.

Each test reflects a real editor correction from the apr-2026 review of
the persistent 'Новости' sheet. When a test fails, that's a regression
of an editor-flagged routing.
"""

from news_agent.core.heuristic_relevance import (
    has_brand_voice,
    has_rumor_signal,
    heuristic_section,
    is_multi_news_title,
)


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


def test_minivan_defers_to_llm() -> None:
    """Editor rule (may-2026): LCV only for ТС with 8+ seats. Passenger
    minivans (5-7 seats like Luxeed V9, Suzuki MPV, GAC M8) are NOT LCV
    by body-type alone. Defer to LLM which knows the seat count rule.
    """
    h = heuristic_section(title="GAC M8 minivan entered service with Moscow firefighters")
    # Should defer to LLM (return None) or route to Local — but NOT auto-LCV
    if h is not None:
        assert h.section != "LCV news"


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


# ---------------------------------------------------- Rumors detection

def test_rumor_signal_in_title() -> None:
    assert has_rumor_signal("Hongqi hybrid SUV may arrive in Russia")
    assert has_rumor_signal("Tesla Roadster reportedly retains manual controls")
    assert has_rumor_signal("Hyundai Creta замечен в новой версии")
    assert has_rumor_signal("Toyota по слухам запустит новый кроссовер")


def test_no_rumor_signal_in_factual_title() -> None:
    assert not has_rumor_signal("Hyundai unveiled the new Tucson")
    assert not has_rumor_signal("BYD reported Q1 sales")


def test_brand_voice_detection() -> None:
    body = "В пресс-службе Toyota сообщили о планах запуска новой модели..."
    assert has_brand_voice(body)

    body2 = "The company announced today that the new model will launch..."
    assert has_brand_voice(body2)


def test_no_brand_voice_in_speculation() -> None:
    body = "По данным неназванных источников, Toyota может представить новую модель..."
    assert not has_brand_voice(body)


def test_rumor_section_when_speculation_no_brand() -> None:
    # Editor case: spotted in colors, no brand voice → Rumor
    h = heuristic_section(
        title="Tesla Cybertruck spotted in new factory paint",
        body_excerpt="The truck was photographed by a passerby near the Gigafactory.",
    )
    assert h is not None and h.section == "Rumors"


def test_NOT_rumor_when_brand_voice_present() -> None:
    # Editor row 43: «Если ссылка на компанию, то это всегда Факты».
    h = heuristic_section(
        title="Hongqi hybrid SUV may arrive in Russia",
        body_excerpt=(
            "Об этом рассказали представители компании Hongqi в "
            "пресс-релизе, опубликованном на официальном сайте."
        ),
    )
    # Brand voice present → defer to LLM (don't force Rumors).
    # heuristic_section returns None — LLM classifies as Confirmed.
    assert h is None


def test_tesla_roadster_per_musk_NOT_rumor_when_body_quotes_him() -> None:
    # Editor row 89: «По словам главы компании Илона Маска - это не слухи»
    h = heuristic_section(
        title="Tesla will keep manual controls only for the Roadster",
        body_excerpt=(
            "По словам главы компании Илона Маска, такая система останется "
            "только в Roadster. Об этом он заявил на квартальном звонке."
        ),
    )
    assert h is None  # brand voice → LLM decides (likely Other news)


# ---------------------------------------------------- Multi-news detector

def test_multi_news_semicolon_split() -> None:
    # Editor row 34: «опять несколько разных новостей по одной ссылке»
    assert is_multi_news_title(
        "Changan integrates DEEPAL and AVATR brands; CATL hosts Super Tech Day"
    )


def test_multi_news_two_substantial_clauses() -> None:
    assert is_multi_news_title(
        "BMW launches new X3 in Europe; Mercedes opens Hamburg plant in 2026"
    )


def test_NOT_multi_news_short_tail_after_semicolon() -> None:
    # "Title; KOR" — short tail after semicolon, just a stripped tag
    assert not is_multi_news_title("Hyundai unveiled the new Tucson; KOR")


def test_NOT_multi_news_single_clause_no_semicolon() -> None:
    assert not is_multi_news_title("Hyundai unveiled the new Tucson")


def test_NOT_multi_news_empty() -> None:
    assert not is_multi_news_title("")
    assert not is_multi_news_title(None)  # type: ignore[arg-type]


# ----------------------------------------- v22 fixes: extended RU subject

def test_avtotor_in_title_routes_to_local_on_ru_portal() -> None:
    h = heuristic_section(
        title="Avtotor started preparing production of Jetour T1/T2",
        domain="auto.mail.ru",
    )
    assert h is not None and h.section == "Local specifics"


def test_podmoskovye_marker_routes_to_local() -> None:
    h = heuristic_section(
        title="Сборку Exeed из Подмосковья перенесут на завод в Шушарах",
        domain="auto.mail.ru",
    )
    assert h is not None and h.section == "Local specifics"


def test_asroad_org_ru_subject_routes_to_local() -> None:
    h = heuristic_section(
        title="АвтоВАЗ стал партнёром Кавказского форума",
        domain="asroad.org",
    )
    assert h is not None and h.section == "Local specifics"


def test_buts_a_catch_force_rejected() -> None:
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://example.com/news",
        title="BMW M3 Lime Rock sells cheap — but there's a catch",
        body="...", source_name="s", source_url="https://example.com/",
    )
    assert blacklist_hit(raw, Blacklist()).hit


def test_single_word_title_rejected_as_placeholder() -> None:
    from news_agent.core.heuristic_relevance import looks_like_article
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://example.com/news",
        title="Sollers",  # single-word brand placeholder from extractor
        body="some body text " * 100,
        source_name="s", source_url="https://example.com/",
        html="<html>...</html>",
    )
    v = looks_like_article(raw)
    assert not v.is_article
    assert "single-word-placeholder-title" in v.reasons
