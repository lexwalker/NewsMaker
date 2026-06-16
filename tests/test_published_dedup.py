"""Deterministic published-archive dedup (safe exact matching)."""

from news_agent.core.published_dedup import already_published, url_key
from news_agent.core.primary_source import normalise_title


def test_exact_url_is_dup() -> None:
    urls = {url_key("https://moex.com/n/123")}
    assert already_published("https://moex.com/n/123?utm=x", "Любой заголовок тут есть",
                             urls, set()) == "url"


def test_url_key_normalises_variants() -> None:
    k = url_key("https://moex.com/n/123")
    assert url_key("http://www.moex.com/n/123/") == k
    assert url_key("https://moex.com/n/123?utm_source=x") == k
    assert url_key("moex.com/n/123") == k


def test_unseen_url_not_dup() -> None:
    urls = {url_key("https://moex.com/n/123")}
    assert already_published("https://drom.ru/other", "Совершенно другая новость про это",
                             urls, set()) == ""


def test_exact_recent_title_is_dup() -> None:
    t = "Geely Coolray facelift revealed with new engine options"
    titles = {normalise_title(t)}
    assert already_published("https://x.ru/a", t, set(), titles) == "title"


def test_short_title_not_matched() -> None:
    # below MIN_TITLE_TOKENS → not trusted even if present
    t = "Lada обновилась"
    titles = {normalise_title(t)}
    assert already_published("https://x.ru/a", t, set(), titles) == ""


def test_title_not_in_recent_set() -> None:
    titles = {normalise_title("Some other published long headline here now")}
    assert already_published("https://x.ru/a",
                             "Brand new unrelated story about a car launch",
                             set(), titles) == ""


def test_url_takes_priority() -> None:
    urls = {url_key("https://x.ru/a")}
    titles = {normalise_title("doesn't matter")}
    assert already_published("https://x.ru/a", "anything", urls, titles) == "url"


def test_empty_inputs_safe() -> None:
    assert already_published("", "", set(), set()) == ""
    assert already_published("https://x.ru/a", "t", set(), set()) == ""
