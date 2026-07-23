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


def test_emoji_opening_post_keeps_real_title() -> None:
    # jul-24 miss-funnel bug: Telegram wraps emoji in their own tags and
    # get_text("\n") puts them on a separate line — a post OPENING with an
    # emoji got title="🚘" (<3 chars) and was dropped entirely.
    html = """
    <div class="tgme_widget_message_wrap">
      <div class="tgme_widget_message" data-post="chan/777">
        <div class="tgme_widget_message_text">
          <i class="emoji"><b>🚘</b></i><b>Новый кроссовер Tenet A8 встал на конвейер</b>
          <br/>Подробности в статье.
        </div>
        <time datetime="2026-07-23T10:00:00+00:00"></time>
      </div>
    </div>"""
    posts = parse_channel_html(
        html=html,
        channel_preview_url="https://t.me/s/chan",
        source_name="chan",
        source_url="https://t.me/chan",
        source_language="ru",
        max_items=10,
    )
    assert len(posts) == 1
    assert "Tenet A8" in posts[0].title
    assert not posts[0].title.startswith("🚘")


def test_emoji_only_post_still_skipped() -> None:
    html = """
    <div class="tgme_widget_message_wrap">
      <div class="tgme_widget_message" data-post="chan/778">
        <div class="tgme_widget_message_text"><i class="emoji">🔥</i>
        <i class="emoji">🔥</i></div>
        <time datetime="2026-07-23T10:00:00+00:00"></time>
      </div>
    </div>"""
    posts = parse_channel_html(
        html=html,
        channel_preview_url="https://t.me/s/chan",
        source_name="chan",
        source_url="https://t.me/chan",
        source_language="ru",
        max_items=10,
    )
    assert posts == []
