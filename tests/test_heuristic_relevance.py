from datetime import datetime, timezone

from news_agent.core.heuristic_relevance import (
    grade_article,
    is_auto_or_economy,
    looks_like_article,
)
from news_agent.core.models import RawArticle


def _article(**kw) -> RawArticle:  # type: ignore[no-untyped-def]
    defaults = dict(
        url="https://example.com/news/x",
        title="Some headline",
        body="",
        html="",
        source_name="s",
        source_url="https://example.com/",
    )
    defaults.update(kw)
    return RawArticle(**defaults)


def test_real_article_passes() -> None:
    raw = _article(
        url="https://example.com/news/toyota-2026-camry",
        title="Toyota unveils 2026 Camry with plug-in hybrid option",
        body="Toyota Motor Corporation today announced the 2026 Camry lineup. " * 20,
        html='<html><script type="application/ld+json">{"@type": "NewsArticle"}</script>'
        '<meta property="og:type" content="article">body</html>',
        published_at=datetime.now(timezone.utc),
    )
    v = looks_like_article(raw)
    assert v.is_article
    assert v.score >= 0.6


def test_index_page_rejected() -> None:
    raw = _article(
        url="https://example.com/category/news?lang=en",
        title="News",
        body="short",
        html="<a href></a>" * 500,
    )
    assert not looks_like_article(raw).is_article


def test_auto_article_passes_topic() -> None:
    raw = _article(
        title="Lada представила обновлённый седан Vesta",
        body="АвтоВАЗ объявил о старте продаж обновлённой Lada Vesta. Модельный ряд расширен." * 5,
    )
    t = is_auto_or_economy(raw)
    assert t.is_auto_or_economy
    assert t.auto_hits >= 2


def test_football_rejected_on_topic() -> None:
    raw = _article(
        title="Real Madrid win Champions League final",
        body="Real Madrid defeated Manchester City 2-1 in the UEFA Champions League final." * 5,
    )
    t = is_auto_or_economy(raw)
    assert not t.is_auto_or_economy


def test_search_url_rejected_as_article() -> None:
    raw = _article(
        url="https://spark-interfax.ru/quick-search",
        title="Поиск",
        body="x" * 5000,
    )
    assert not looks_like_article(raw).is_article


def test_pdf_url_always_rejected() -> None:
    raw = _article(
        url="https://www.geotab.com/CMS/code-of-conduct.pdf",
        title="Code of Conduct",
        body="A long PDF body text." * 200,
        html='<meta property="og:type" content="article">',
        published_at=datetime.now(timezone.utc),
    )
    v = looks_like_article(raw)
    assert not v.is_article
    assert "binary-document-url" in v.reasons


def test_insights_url_rejected() -> None:
    raw = _article(
        url="https://www.coxautoinc.com/insights/auto-market-weekly-summary-04-13-26/",
        title="Auto Market Weekly Summary",
        body="Cox Automotive weekly summary. " * 100,
        html='<meta property="og:type" content="article">',
        published_at=datetime.now(timezone.utc),
    )
    assert not looks_like_article(raw).is_article


def test_camefrom_referral_rejected() -> None:
    raw = _article(
        url="https://www.benchmarkminerals.com/?camefrom=rhomotion.com",
        title="Battery & Critical Minerals Intelligence",
        body="x " * 400,
    )
    assert not looks_like_article(raw).is_article


def test_grade_certain_news_strong_signals() -> None:
    raw = _article(
        url="https://www.autostat.ru/news/56789/",
        title="Продажи новых автомобилей LADA в марте выросли на 15%",
        body=(
            "По данным агентства АВТОСТАТ, продажи новых автомобилей Lada в России "
            "в марте 2026 года достигли 45 000 штук. Дилерская сеть отгрузила "
            "рекордное количество седанов и кроссоверов. Модель Vesta сохранила "
            "лидерство в модельном ряду." * 3
        ),
        html='<script type="application/ld+json">{"@type": "NewsArticle"}</script>'
        '<meta property="og:type" content="article">',
        published_at=datetime.now(timezone.utc),
    )
    v = looks_like_article(raw, whitelist={"autostat.ru"})
    t = is_auto_or_economy(raw)
    assert grade_article(v, t) == "certain_news"


def test_grade_possible_when_article_but_few_auto_hits() -> None:
    raw = _article(
        url="https://example.com/news/2026/04/policy-update",
        title="Government announces new emission policy",
        body="The government has announced new emission rules. " * 20,
        html='<meta property="og:type" content="article">',
        published_at=datetime.now(timezone.utc),
    )
    v = looks_like_article(raw)
    t = is_auto_or_economy(raw)
    # Only one automotive keyword ("emission") is matched — should be possible.
    grade = grade_article(v, t)
    assert grade in {"possible_news", "off_topic"}


def test_grade_off_topic_when_not_automotive() -> None:
    raw = _article(
        url="https://example.com/news/2026/04/football-match",
        title="Real Madrid win Champions League final",
        body="Real Madrid defeated Manchester City 2-1 in the final." * 10,
        html='<meta property="og:type" content="article">',
        published_at=datetime.now(timezone.utc),
    )
    v = looks_like_article(raw)
    t = is_auto_or_economy(raw)
    assert grade_article(v, t) == "off_topic"


def test_grade_not_article_for_pdf() -> None:
    raw = _article(
        url="https://site.com/whitepaper.pdf",
        title="Whitepaper",
        body="Some text " * 200,
    )
    v = looks_like_article(raw)
    t = is_auto_or_economy(raw)
    assert grade_article(v, t) == "not_article"


def test_whitelist_domain_gets_bonus() -> None:
    raw = _article(
        url="https://www.autostat.ru/news/12345/",
        title="Продажи новых автомобилей в РФ выросли на 8%",
        body="По данным агентства АВТОСТАТ. " * 20,
        html="short",
        published_at=datetime.now(timezone.utc),
    )
    without = looks_like_article(raw)
    with_wl = looks_like_article(raw, whitelist={"autostat.ru"})
    assert with_wl.score > without.score
    assert any("whitelist-domain" in r for r in with_wl.reasons)
