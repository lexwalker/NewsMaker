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


# --- jul-20: taxonomy/theme listings must never pass as articles -------------
# kommersant.ru/theme/2099 («Кредитный рынок: последние новости» — a TAG page)
# shipped to the editor as an article: the old bare "/20" date hint matched
# inside "/2099". The year hint is now anchored (20[12]x) and taxonomy first
# segments are blocked outright.

def test_theme_and_taxonomy_paths_are_not_articles() -> None:
    from news_agent.adapters.fetchers.html import _looks_like_article
    for u in ("https://www.kommersant.ru/theme/2099?from=tag",
              "https://www.kommersant.ru/theme/2099/",
              "https://site.ru/tags/electro", "https://site.ru/rubric/auto",
              "https://site.ru/page/2050"):
        assert not _looks_like_article(u), u


def test_date_paths_still_articles() -> None:
    from news_agent.adapters.fetchers.html import _looks_like_article
    for u in ("https://iz.ru/2133059/2026-07-16/za-polgoda-spros",
              "https://1prime.ru/20260715/legkovushki-871521538.html",
              "https://rg.ru/2026/07/16/some-slug.html",
              "https://cnevpost.com/2026/07/20/li-auto-l6/"):
        assert _looks_like_article(u), u


def test_autostat_analytics_and_infographics_ids_pass() -> None:
    # jul-27 zero-yield forensics: numeric-id articles under /analytics/ and
    # /infographics/ matched NO hint (no slug, no date) and both sections
    # yielded 0 across two runs.
    assert _looks_like_article("https://www.autostat.ru/analytics/62781/")
    assert _looks_like_article("https://www.autostat.ru/infographics/62793/")


def test_archive_listing_pages_rejected() -> None:
    # …while the fetch budget burned on date-shaped archive LISTINGS.
    assert not _looks_like_article(
        "https://www.autostat.ru/analytics/archive/2026/7/")
    assert not _looks_like_article(
        "https://www.autostat.ru/infographics/archive/2026/")


def test_locale_switcher_is_not_an_article() -> None:
    # jul-28 europarl: /news/bg, /news/es … matched the "/news/" hint and 24
    # language switchers filled the whole per-source cap.
    assert not _looks_like_article("https://www.europarl.europa.eu/news/bg")
    assert not _looks_like_article("https://www.europarl.europa.eu/news/es")
    # a real press release keeps working
    assert _looks_like_article(
        "https://www.europarl.europa.eu/news/en/press-room/20260716IPR46537/"
        "foreign-affairs-committee-meps-conclude-mission-to-china")
