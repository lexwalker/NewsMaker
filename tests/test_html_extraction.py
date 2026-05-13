from datetime import datetime, timezone
from pathlib import Path

from news_agent.adapters.fetchers.html import extract_article

FIX = Path(__file__).parent / "fixtures"


def test_english_article_extraction() -> None:
    html = (FIX / "article_en.html").read_text(encoding="utf-8")
    art = extract_article(
        html=html,
        url="https://example.com/news/toyota-2026-camry",
        source_name="Example Auto",
        source_url="https://example.com/news/",
        source_language="en",
    )
    assert art is not None
    assert "Camry" in art.title
    assert "Toyota" in art.body
    assert art.image_url and art.image_url.endswith("camry-2026.jpg")
    assert art.published_at is not None
    assert art.published_at.tzinfo is not None
    assert art.published_at.astimezone(timezone.utc).isoformat().startswith("2026-04-10")
    assert any("pressroom.toyota.com" in link for link in art.outbound_links)


def test_russian_article_extraction() -> None:
    html = (FIX / "article_ru.html").read_text(encoding="utf-8")
    art = extract_article(
        html=html,
        url="https://example.ru/news/haval-kz",
        source_name="Example RU",
        source_url="https://example.ru/",
        source_language="ru",
    )
    assert art is not None
    assert "Haval" in art.title
    assert "Казахстан" in art.body
    assert art.image_url and "haval-kz.jpg" in art.image_url
    assert art.published_at is not None
    assert any("haval.ru" in link for link in art.outbound_links)


def test_preferred_published_overrides_in_body() -> None:
    """RSS-pubDate override: when a feed's authoritative timestamp is passed
    via ``preferred_published``, body-text date heuristics must NOT win.

    Regression: abreview.ru (and similar) emit fresh items in RSS but their
    article bodies quote past events; the body-scanner used to pick those
    older dates, causing the freshness gate to drop fresh posts as stale.
    """
    # Body mentions an older date prominently — the heuristic would normally
    # latch onto it. The RSS-pubDate must take precedence.
    html = """<html><head><title>Forland expands dealer network</title></head>
    <body><h1>Forland expands dealer network</h1>
    <p>Back on 7 May 2026 the company started a programme that now expanded
    across all federal districts. The original announcement on 07.05.2026
    contained a roadmap for 2027.</p>
    <p>The current expansion adds 12 new dealers across 8 cities.</p>
    </body></html>"""
    rss_pub = datetime(2026, 5, 13, 12, 15, 49, tzinfo=timezone.utc)
    art = extract_article(
        html=html,
        url="https://abreview.ru/ab/news/forland_rasshiryaet_dilerskuyu_set/",
        source_name="abreview.ru",
        source_url="https://abreview.ru/",
        source_language="ru",
        preferred_published=rss_pub,
    )
    assert art is not None
    # Must match the RSS pubDate, NOT the older date from the body text.
    assert art.published_at == rss_pub


def test_preferred_published_none_falls_back_to_heuristics() -> None:
    """When no preferred_published is passed, behaviour is unchanged —
    body / meta heuristics still run."""
    html = """<html><head>
    <meta property="article:published_time" content="2026-04-10T08:00:00Z">
    <title>Test article</title></head>
    <body><h1>Test article</h1><p>Hello world.</p></body></html>"""
    art = extract_article(
        html=html,
        url="https://example.com/news/x",
        source_name="ex",
        source_url="https://example.com/",
        source_language="en",
    )
    assert art is not None
    assert art.published_at is not None
    assert art.published_at.astimezone(timezone.utc).isoformat().startswith("2026-04-10")
