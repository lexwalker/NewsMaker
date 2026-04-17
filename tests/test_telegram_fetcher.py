from pathlib import Path

from news_agent.adapters.fetchers.telegram import (
    is_telegram_url,
    parse_channel_html,
    to_channel_preview_url,
)

FIX = Path(__file__).parent / "fixtures" / "telegram_channel.html"


def test_url_detection() -> None:
    assert is_telegram_url("https://t.me/autonews")
    assert is_telegram_url("https://telegram.me/autonews")
    assert not is_telegram_url("https://autonews.ru/")


def test_preview_url_normalisation() -> None:
    assert to_channel_preview_url("https://t.me/autonews") == "https://t.me/s/autonews"
    assert to_channel_preview_url("https://t.me/s/autonews") == "https://t.me/s/autonews"
    assert to_channel_preview_url("https://t.me/autonews/12345") == "https://t.me/s/autonews"
    # Private invite links return None
    assert to_channel_preview_url("https://t.me/+abcDEF") is None
    assert to_channel_preview_url("https://t.me/") is None


def test_parse_channel_html_returns_posts_newest_first() -> None:
    html = FIX.read_text(encoding="utf-8")
    posts = parse_channel_html(
        html=html,
        channel_preview_url="https://t.me/s/autonews",
        source_name="autonews",
        source_url="https://t.me/autonews",
        source_language="ru",
        max_items=10,
    )
    # Media-only post without caption must be skipped
    assert len(posts) == 2

    # Newest first (12346 → 12345 by our fixture datetimes)
    assert posts[0].url == "https://t.me/autonews/12346"
    assert posts[1].url == "https://t.me/autonews/12345"

    # First post — Haval
    haval = posts[0]
    assert "Haval" in haval.title
    assert haval.published_at is not None
    assert "haval.ru" in " ".join(haval.outbound_links)

    # Second post — Toyota with photo attachment + outbound link
    toyota = posts[1]
    assert "Toyota" in toyota.title
    assert toyota.image_url and "camry.jpg" in toyota.image_url
    assert any("toyota.com" in link for link in toyota.outbound_links)
