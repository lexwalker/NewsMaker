from datetime import timezone
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
