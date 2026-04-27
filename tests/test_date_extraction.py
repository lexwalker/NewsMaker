"""Date-extraction fallbacks for HTML fetcher."""

from datetime import datetime, timezone

from news_agent.adapters.fetchers.html import (
    _pick_published_from_text,
    _pick_published_from_url,
    _parse_dt,
)


def _utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, tzinfo=timezone.utc)


# ---------------- URL pattern --------------------------------------------
def test_url_yyyy_slash_mm_slash_dd() -> None:
    assert _pick_published_from_url(
        "https://carnewschina.com/2026/04/27/byd-launches"
    ) == _utc(2026, 4, 27)


def test_url_iso_dashes() -> None:
    assert _pick_published_from_url(
        "https://example.com/news/2026-04-27/article"
    ) == _utc(2026, 4, 27)


def test_url_compact_yyyymmdd() -> None:
    assert _pick_published_from_url(
        "https://russian.news.cn/20260424/abcd/c.html"
    ) == _utc(2026, 4, 24)


def test_url_no_date() -> None:
    assert _pick_published_from_url("https://example.com/about") is None


def test_url_invalid_month_rejected() -> None:
    # /2026/13/01/ — month 13 must not parse
    assert _pick_published_from_url("https://example.com/2026/13/01/x") is None


def test_url_old_year_rejected() -> None:
    # 2010 outside the [2015, 2030] sanity range
    assert _pick_published_from_url("https://example.com/2010/04/27/x") is None


# ---------------- Text patterns ------------------------------------------
def test_russian_date_in_body() -> None:
    assert _pick_published_from_text(
        "Toyota представила Camry",
        "Москва, 27 апреля 2026 года. Компания Toyota объявила...",
    ) == _utc(2026, 4, 27)


def test_english_month_first_us_style() -> None:
    assert _pick_published_from_text(
        "Toyota unveils Camry",
        "TOKYO, April 27, 2026 - Toyota Motor Corporation announced...",
    ) == _utc(2026, 4, 27)


def test_english_day_first_uk_style() -> None:
    assert _pick_published_from_text(
        "Headline",
        "Posted on 27 April 2026 by ...",
    ) == _utc(2026, 4, 27)


def test_numeric_dotted() -> None:
    assert _pick_published_from_text(
        "Headline", "Опубликовано 27.04.2026 в 14:30. Geely..."
    ) == _utc(2026, 4, 27)


def test_iso_in_body() -> None:
    assert _pick_published_from_text(
        "Headline", "Posted on 2026-04-27. Article body..."
    ) == _utc(2026, 4, 27)


def test_no_date_returns_none() -> None:
    assert _pick_published_from_text(
        "Random title", "This article has nothing date-shaped."
    ) is None


def test_old_date_rejected() -> None:
    # Pre-2015 dates rejected to avoid copyright footers etc.
    assert _pick_published_from_text(
        "Headline", "Founded in 2010. The article body..."
    ) is None


# ---------------- _parse_dt ----------------------------------------------
def test_parse_iso_with_z() -> None:
    assert _parse_dt("2026-04-27T13:45:00Z") == datetime(
        2026, 4, 27, 13, 45, tzinfo=timezone.utc
    )


def test_parse_iso_with_offset() -> None:
    dt = _parse_dt("2026-04-27T13:45:00+03:00")
    assert dt is not None
    assert dt.utcoffset().total_seconds() == 3 * 3600


def test_parse_dotted_ddmmyyyy() -> None:
    assert _parse_dt("27.04.2026") == _utc(2026, 4, 27)


def test_parse_rfc2822_gmt() -> None:
    assert _parse_dt("Sun, 27 Apr 2026 13:45:00 GMT") == datetime(
        2026, 4, 27, 13, 45, tzinfo=timezone.utc
    )
