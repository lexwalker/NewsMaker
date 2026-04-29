from news_agent.core.urls import canonicalise, domain_of, url_hash, year_in_url_path


def test_canonicalise_strips_utm_and_fragment() -> None:
    raw = "HTTPS://Example.COM/path/?utm_source=x&id=1#frag"
    assert canonicalise(raw) == "https://example.com/path/?id=1"


def test_canonicalise_keeps_empty_path() -> None:
    assert canonicalise("https://Example.com") == "https://example.com/"


def test_url_hash_stable_across_tracking() -> None:
    a = url_hash("https://example.com/a?utm_source=fb")
    b = url_hash("https://Example.com/a")
    assert a == b


def test_domain_of_strips_www() -> None:
    assert domain_of("https://www.example.com/x") == "example.com"


# ---------------------------------------------------- year_in_url_path

def test_year_in_path_basic() -> None:
    assert year_in_url_path("https://byd.com/news/2024/05/21/release") == 2024


def test_year_in_path_dash_separated() -> None:
    assert year_in_url_path("https://hyundai.com/ru-news/2022-01-15-grand-launch") == 2022


def test_year_in_path_takes_earliest_when_multiple() -> None:
    # /news/2024/article-about-2026-tucson — both 2024 & 2026, take earliest
    assert year_in_url_path("https://example.com/news/2024/article-about-2026-tucson") == 2024


def test_year_in_path_no_year_returns_none() -> None:
    assert year_in_url_path("https://example.com/article/best-sedans") is None


def test_year_in_path_does_not_match_part_numbers() -> None:
    # SKU-like "2024" buried inside a longer digit run shouldn't match
    assert year_in_url_path("https://example.com/sku/123202404567") is None


def test_year_in_path_out_of_range_ignored() -> None:
    assert year_in_url_path("https://example.com/year/3050/oops") is None


def test_year_in_path_handles_query_string_only() -> None:
    # Year only in query — we only look at path, so should be None
    assert year_in_url_path("https://example.com/article?year=2022") is None
