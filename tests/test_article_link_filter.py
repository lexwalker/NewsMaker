"""Article-link discovery filter (_looks_like_article) — the URL heuristic
that decides which listing-page links are worth fetching. jun-2026: it
silently rejected kommersant.ru's /doc/<id> article URLs (no slug), so the
source yielded 0 — add coverage for that + the existing patterns."""

from news_agent.adapters.fetchers.html import _looks_like_article


def test_doc_id_article_url_passes() -> None:
    # kommersant.ru articles are /doc/<numeric-id> — must be recognised now
    assert _looks_like_article("https://www.kommersant.ru/doc/8511593")


def test_existing_article_patterns_still_pass() -> None:
    assert _looks_like_article("https://auto.ru/mag/article/some-long-slug-here/")
    assert _looks_like_article("https://iz.ru/2117530/2026-06-18/eks-glava-road")
    assert _looks_like_article("https://x.ru/news/foo-bar")
    assert _looks_like_article("https://x.ru/2026/06/some-story.html")


def test_non_article_urls_still_rejected() -> None:
    assert not _looks_like_article("https://www.kommersant.ru/rubric/138")
    assert not _looks_like_article("https://www.kommersant.ru/theme/3219")
    assert not _looks_like_article("https://auto.ru/garage/")
    assert not _looks_like_article("https://x.ru/")
    assert not _looks_like_article("https://x.ru/about")
