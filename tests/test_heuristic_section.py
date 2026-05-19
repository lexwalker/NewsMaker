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


def test_russian_finrezultat_routes_to_local() -> None:
    # ROUND2 P2-1 (flipped): a RUSSIAN company's financials → Local
    # specifics, NOT Other (editor row 54 «Delimobil Q1 → Это Local»;
    # resolves the prior contradiction with rule 1 «АвтоВАЗ → Local»).
    h = heuristic_section(title="АвтоВАЗ опубликовал финрезультаты за 2025 год")
    assert h is not None and h.section == "Local specifics"


def test_foreign_finrezultat_still_other() -> None:
    # Foreign-subject financials unaffected — still Other news.
    h = heuristic_section(title="Nissan reported Q1 financial results for 2026")
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

def test_global_model_reveal_routes_to_confirmed() -> None:
    # ROUND2 P1-1 (flipped): a specific-model debut/reveal is ALWAYS
    # Facts (editor universal rule). "unveiled the" → Confirmed,
    # region Global (no RU markers).
    h = heuristic_section(title="Hyundai unveiled the new Grandeur with premium finish")
    assert h is not None and h.section == "Confirmed" and h.region == "Global"


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


def test_spy_shot_preview_titles_are_rumor() -> None:
    """may-2026 editor dup case: spy-shot / pre-reveal previews must be
    Rumors, never Confirmed."""
    # the exact r11 title that wrongly went Confirmed
    assert has_rumor_signal(
        "Jaguar Type 01 appears in new images ahead of imminent debut"
    )
    assert has_rumor_signal(
        "Jaguar Type 01 показался на новых снимках перед скорой премьерой"
    )
    assert has_rumor_signal(
        "BMW M5 Touring prototype spotted without heavy camouflage"
    )
    assert has_rumor_signal("New Audi A6 caught testing ahead of debut")
    assert has_rumor_signal("Прототип электроседана Jaguar без камуфляжа")
    assert has_rumor_signal("Volkswagen Golf засветился перед премьерой")


def test_legit_launch_not_flagged_as_rumor() -> None:
    """Guard: real launches must NOT be swept up by the new spy phrases."""
    assert not has_rumor_signal("Jaguar unveiled the Type 01 electric SUV")
    assert not has_rumor_signal("BMW launched the new M5 in Europe")
    assert not has_rumor_signal("Toyota представила новый Camry")


def test_r11_jaguar_spy_routes_to_rumors_not_confirmed() -> None:
    """End-to-end: the editor-reported misclassification. Body has no
    brand-voice → heuristic_section must return Rumors."""
    h = heuristic_section(
        title="Jaguar Type 01 appears in new images ahead of imminent debut",
        body_excerpt="New spy images surfaced online showing the prototype "
                      "testing on public roads. No official word yet.",
    )
    assert h is not None and h.section == "Rumors"


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


# ----------- Plan P2-B: RU market data → force Local ----------------------

def test_ru_passenger_imports_forces_local() -> None:
    """Editor row 209: «passenger imports from individuals — Local, not Economics»."""
    h = heuristic_section(
        title="Nearly half of new passenger car imports in Q1 2026 came from individuals in Russia"
    )
    assert h is not None and h.section == "Local specifics"
    assert h.reason == "ru-market-data-force-local"


def test_april_sales_forecast_forces_local() -> None:
    """Editor row 239: «прогноз продаж в апреле — Местные»."""
    h = heuristic_section(
        title="April sales forecast: 115-120 thousand new cars in Russia"
    )
    assert h is not None and h.section == "Local specifics"


def test_ru_car_stocks_forces_local() -> None:
    """Editor row 251: «стоки новых ТС — Местные»."""
    h = heuristic_section(
        title="Total stocks of new passenger cars in Russia reached 400 thousand"
    )
    assert h is not None and h.section == "Local specifics"


def test_ru_weekly_sales_forces_local() -> None:
    """Editor row 284: «продажи выросли за неделю — Местные»."""
    h = heuristic_section(
        title="Russia new car sales increased over the week"
    )
    assert h is not None and h.section == "Local specifics"


def test_global_passenger_imports_NOT_forced_local() -> None:
    """No RU subject markers → tier 4b does NOT trigger."""
    h = heuristic_section(
        title="EU passenger car imports forecast for 2027"
    )
    # No RU markers → tier 4b skipped. May return None.
    if h is not None:
        assert h.reason != "ru-market-data-force-local"


# ----------- Plan P1-A: RU TG aggregator + global brand sales = reject ----

def test_ru_tg_aggregator_global_brand_sales_rejected() -> None:
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://t.me/chinamashina_news/13437",
        title="Great Wall Motor sales in April 2026 reached 106,312 units",
        body="...", source_name="tg", source_url="https://t.me/chinamashina_news",
    )
    v = blacklist_hit(raw, Blacklist())
    assert v.hit
    assert "ru-tg-aggregator-single-brand-sales" in (v.reason or "")


def test_sergtselikov_mazda_market_share_rejected() -> None:
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://t.me/sergtselikov/12345",
        title="Mazda market share in Q1 2026 declined",
        body="...", source_name="tg", source_url="https://t.me/sergtselikov",
    )
    assert blacklist_hit(raw, Blacklist()).hit


def test_chinamashina_lada_sales_NOT_rejected() -> None:
    """Domestic brand (Lada) from aggregator is OK — editor allows as Local."""
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://t.me/chinamashina_news/77",
        title="Lada Iskra sales in April 2026 hit 8,000 units",
        body="...", source_name="tg", source_url="https://t.me/chinamashina_news",
    )
    v = blacklist_hit(raw, Blacklist())
    # Lada is domestic → exempt from P1-A rejection.
    assert "ru-tg-aggregator-single-brand-sales" not in (v.reason or "")


def test_aggregator_no_sales_pattern_NOT_rejected() -> None:
    """No sales/month pattern → don't trigger P1-A."""
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://t.me/chinamashina_news/13520",
        title="Li Auto registered refreshed L6 SUV in China",
        body="...", source_name="tg", source_url="https://t.me/chinamashina_news",
    )
    v = blacklist_hit(raw, Blacklist())
    assert "ru-tg-aggregator-single-brand-sales" not in (v.reason or "")


# ----------- P1-D: per-model price phrases via _FORCE_REJECT_PHRASES ------

def test_revised_price_list_force_rejected() -> None:
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://example.com/news",
        title="Brands that revised price lists in April 2026",
        body="...", source_name="s", source_url="https://example.com/",
    )
    assert blacklist_hit(raw, Blacklist()).hit


# ----------- P2-C: off-topic content via _FORCE_REJECT_PHRASES ------------

def test_putin_supported_rejected() -> None:
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://example.com/news",
        title="Putin supported the idea of extending regional debt repayment",
        body="...", source_name="s", source_url="https://example.com/",
    )
    assert blacklist_hit(raw, Blacklist()).hit


def test_vnukovo_airport_rejected() -> None:
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://example.com/news",
        title="Medics assist passengers at Vnukovo Airport",
        body="...", source_name="s", source_url="https://example.com/",
    )
    assert blacklist_hit(raw, Blacklist()).hit


def test_few_off_roadster_lamborghini_rejected() -> None:
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://example.com/news",
        title="Lamborghini Few Off Roadster: emotional, limited and open",
        body="...", source_name="s", source_url="https://example.com/",
    )
    assert blacklist_hit(raw, Blacklist()).hit


# ----------- Plan P1-C: stale-archive URL pattern guard -------------------

def test_bydeurope_pre_2026_article_id_rejected() -> None:
    """Editor flagged rows 313-318: bydeurope.com/article/<460-468> are
    all 2023-2024 articles served as fresh."""
    from news_agent.core.heuristic_relevance import looks_like_article
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://bydeurope.com/article/468",
        title="BYD signs partnership with DLL Europe",
        body="some body text " * 100,
        source_name="s", source_url="https://bydeurope.com/",
        html="<html>...</html>",
    )
    v = looks_like_article(raw)
    assert not v.is_article
    assert "stale-archive-url" in v.reasons


def test_bydeurope_fresh_article_id_passes() -> None:
    """bydeurope.com IDs ≥500 are post-2026 — should NOT trigger stale guard."""
    from news_agent.core.heuristic_relevance import _is_known_stale_archive_url
    assert not _is_known_stale_archive_url("https://bydeurope.com/article/512")
    assert not _is_known_stale_archive_url("https://bydeurope.com/article/500")


def test_hyundai_ru_pre_feb_2022_press_archive_rejected() -> None:
    """Editor flagged rows 320-324: hyundai.ru/news/press-<N> for N≤12700
    are all Jan-Feb 2022 archive articles."""
    from news_agent.core.heuristic_relevance import looks_like_article
    from news_agent.core.models import RawArticle
    raw = RawArticle(
        url="https://www.hyundai.ru/news/press-12591",
        title="Hyundai Motor sng results January 2022",
        body="some body text " * 100,
        source_name="s", source_url="https://hyundai.ru/news/",
        html="<html>...</html>",
    )
    v = looks_like_article(raw)
    assert not v.is_article
    assert "stale-archive-url" in v.reasons


def test_hyundai_ru_fresh_press_id_passes() -> None:
    """hyundai.ru press-13000+ should not trigger the stale guard."""
    from news_agent.core.heuristic_relevance import _is_known_stale_archive_url
    assert not _is_known_stale_archive_url("https://www.hyundai.ru/news/press-13050")


def test_other_domains_unaffected_by_stale_guard() -> None:
    """The stale guard MUST be domain-scoped — random URLs with /article/<N>
    on other domains must not be filtered."""
    from news_agent.core.heuristic_relevance import _is_known_stale_archive_url
    assert not _is_known_stale_archive_url("https://carscoops.com/article/100")
    assert not _is_known_stale_archive_url("https://motor1.com/news/12345/")
    assert not _is_known_stale_archive_url("")


# =================== ROUND 2 (110-comment audit) ========================

def test_p1_1_ru_model_pricing_routes_confirmed_not_local() -> None:
    """Editor r6: RU-market model pricing/debut → Confirmed (Facts),
    region Local — NOT Local specifics."""
    h = heuristic_section(
        title="AvtoVAZ announced pricing for SKM M7 minivan in Russia"
    )
    assert h is not None and h.section == "Confirmed" and h.region == "Local"


def test_p1_1_li_auto_ru_premiere_confirmed() -> None:
    """Editor r60: «Li Auto L9 Russian premiere → это Факты»."""
    h = heuristic_section(
        title="Li Auto announced Russian premiere of L9 Livis"
    )
    assert h is not None and h.section == "Confirmed"


def test_p2_2_launch_delay_routes_confirmed() -> None:
    """Editor r49/r143: VW Golf EV launch delay → Facts."""
    h = heuristic_section(
        title="Volkswagen postponed launch of electric Golf beyond 2028"
    )
    assert h is not None and h.section == "Confirmed"


def test_p1_1_brand_period_stats_stay_local_guard() -> None:
    """Guard: brand period statistics must NOT become Confirmed —
    they stay Local (Tier 4b)."""
    h = heuristic_section(title="Changan sales in April 2026 in Russia grew")
    assert h is None or h.section != "Confirmed"


def test_p1_1_spy_shot_still_rumors_not_confirmed() -> None:
    """Guard: 'ahead of imminent debut' is a spy-shot — rumor signal
    suppresses the debut→Confirmed path (editor: spy never Confirmed)."""
    h = heuristic_section(
        title="Jaguar Type 01 appears in new images ahead of imminent debut",
        body_excerpt="Spy photos surfaced; no official word.",
    )
    assert h is not None and h.section == "Rumors"


def test_p1_1_factory_relocation_not_falsely_confirmed() -> None:
    """Guard: 'перенесут ... завод' (relocate plant) must NOT match the
    launch-delay phrase — stays Local on a RU portal."""
    h = heuristic_section(
        title="Сборку Exeed из Подмосковья перенесут на завод в Шушарах",
        domain="auto.mail.ru",
    )
    assert h is not None and h.section == "Local specifics"


def test_p2_1_ru_company_financial_to_local() -> None:
    """Editor r54: Delimobil Q1 net loss → Local specifics, not Other."""
    h = heuristic_section(
        title="Российский Делимобиль сократил чистый убыток на 18% в Q1"
    )
    assert h is not None and h.section == "Local specifics"


def _bl(title: str):
    from news_agent.core.config_loader import Blacklist
    from news_agent.core.heuristic_relevance import blacklist_hit
    from news_agent.core.models import RawArticle
    raw = RawArticle(url="https://example.com/n", title=title, body="...",
                      source_name="s", source_url="https://example.com/")
    return blacklist_hit(raw, Blacklist())


def test_p1_2_deliveries_to_dealers_rejected() -> None:
    assert _bl("AvtoVAZ started deliveries of SKM M7 vehicles to dealers").hit


def test_p2_4_disaster_relief_funding_rejected() -> None:
    assert _bl("Government allocated 1,8 bln RUB for road repairs after the flood").hit


def test_p3_2_third_party_offroad_test_rejected() -> None:
    assert _bl("Hyundai IONIQ 5 XRT tested off-road by reviewers").hit


def test_p3_1_share_price_plunge_rejected() -> None:
    assert _bl("Leapmotor shares plunge to 2-month low as Q1 loss widens").hit
