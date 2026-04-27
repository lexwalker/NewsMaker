from datetime import datetime, timedelta, timezone

from news_agent.core.config_loader import BrandDomainEntry, PrimarySourceCues
from news_agent.core.primary_source import (
    CorpusEntry,
    detect_earliest_in_corpus,
    detect_primary_source,
    normalise_title,
)

BRANDS = [
    BrandDomainEntry(brand="Toyota", aliases=["Тойота"], domains=["toyota.com", "pressroom.toyota.com"]),
    BrandDomainEntry(brand="BMW", domains=["bmw.com", "press.bmwgroup.com"]),
]
CUES = PrimarySourceCues(
    phrases={"en": ["press release", "according to"], "ru": ["пресс-релиз", "сообщает"]},
    press_release_hosts=["prnewswire.com", "pressroom.toyota.com"],
    mirror_hosts=["t.me", "max.ru", "vk.com", "telegra.ph"],
)


def test_facebook_share_link_ignored() -> None:
    """Share buttons must never become primary sources."""
    url, dom, conf = detect_primary_source(
        article_url="https://autoblog.example/news/some-story",
        body="Some article body about cars without explicit source.",
        title="Some auto headline",
        outbound_links=[
            "https://www.facebook.com/sharer/sharer.php?u=https://autoblog.example/news/some-story",
            "https://twitter.com/intent/tweet?text=foo",
            "https://t.me/share/url?url=foo",
        ],
        brands=BRANDS,
        cues=CUES,
    )
    # Should fall through to "self with low confidence" because all
    # outbound candidates are junk share buttons.
    assert url == "https://autoblog.example/news/some-story"
    assert conf == "low"


def test_root_homepage_link_ignored() -> None:
    """Root-only URLs (no path) shouldn't be picked as primary."""
    url, dom, conf = detect_primary_source(
        article_url="https://autoblog.example/news/toyota-x",
        body="According to Toyota, a new model is coming.",
        title="Toyota announces something",
        outbound_links=[
            "https://www.toyota.com/",  # root — junk
            "https://www.toyota.com",   # root — junk
        ],
        brands=BRANDS,
        cues=CUES,
    )
    # No usable primary in outbound → fall back to self
    assert url == "https://autoblog.example/news/toyota-x"
    assert conf == "low"


def test_login_page_link_ignored() -> None:
    """Login / auth URLs shouldn't be picked as primary."""
    url, dom, conf = detect_primary_source(
        article_url="https://example.com/news/x",
        body="Some text mentioning Toyota.",
        title="Toyota story",
        outbound_links=[
            "https://example.com/login",
            "https://example.com/signin",
            "https://www.toyota.com/news/launch-2026",
        ],
        brands=BRANDS,
        cues=CUES,
    )
    # Toyota brand domain match should win — login URLs filtered.
    assert "toyota.com" in dom
    assert "/news/launch-2026" in url


def test_self_is_press_release_host_promoted_to_high() -> None:
    """When the article URL itself is on a press-release host, mark as
    high-confidence self-source instead of returning 'low' fallback."""
    url, dom, conf = detect_primary_source(
        article_url="https://pressroom.toyota.com/2026/04/camry-launch",
        body="Toyota today announced the new Camry.",
        title="Toyota Camry launches in 2026",
        outbound_links=[],  # no outbound at all
        brands=BRANDS,
        cues=CUES,
    )
    assert url == "https://pressroom.toyota.com/2026/04/camry-launch"
    assert conf == "high"


def test_whitelist_article_promoted_to_high() -> None:
    """Editor-trusted whitelist domains also become primary at high
    confidence when no better external source is found."""
    url, dom, conf = detect_primary_source(
        article_url="https://carnewschina.com/2026/04/byd-x",
        body="BYD revealed a new model today.",
        title="BYD reveals new model",
        outbound_links=[],
        brands=BRANDS,
        cues=CUES,
        whitelist_domains={"carnewschina.com"},
    )
    assert url == "https://carnewschina.com/2026/04/byd-x"
    assert conf == "high"


def test_press_release_host_wins() -> None:
    url, dom, conf = detect_primary_source(
        article_url="https://autoblog.example/news/toyota-x",
        body="According to the press release, Toyota announced a new model.",
        title="Toyota announces new model",
        outbound_links=[
            "https://autoblog.example/related",
            "https://pressroom.toyota.com/news/new-model",
            "https://twitter.com/foo",
        ],
        brands=BRANDS,
        cues=CUES,
    )
    assert dom == "pressroom.toyota.com"
    assert conf == "high"
    assert "toyota" in url


def test_brand_domain_with_mention_is_high() -> None:
    url, dom, conf = detect_primary_source(
        article_url="https://auto.example/bmw-m3",
        body="BMW unveiled the new M3. More details at its site.",
        title="BMW unveils new M3",
        outbound_links=["https://www.bmw.com/en/models/m3.html"],
        brands=BRANDS,
        cues=CUES,
    )
    assert dom == "www.bmw.com" or dom == "bmw.com"
    assert conf == "high"
    assert url.startswith("https://www.bmw.com")


def test_fallback_to_article_when_no_signal() -> None:
    url, dom, conf = detect_primary_source(
        article_url="https://autoblog.example/story",
        body="A vague article about cars.",
        title="Cars are popular",
        outbound_links=[],
        brands=BRANDS,
        cues=CUES,
    )
    assert url == "https://autoblog.example/story"
    assert dom == "autoblog.example"
    assert conf == "low"


def test_cue_phrase_with_external_link_is_medium() -> None:
    url, dom, conf = detect_primary_source(
        article_url="https://autoblog.example/story",
        body="Сообщает Рейтер. Подробности ниже.",
        title="Новости",
        outbound_links=["https://www.reuters.com/article-xyz"],
        brands=BRANDS,
        cues=CUES,
    )
    assert dom.endswith("reuters.com")
    assert conf == "medium"


# ----- Level 2: earliest appearance in corpus -------------------------------
def _t(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


def test_normalise_title_strips_suffix() -> None:
    assert normalise_title("Mustang GTD obliterates Corvette — MotorTrend") == \
        "mustang gtd obliterates corvette"
    assert normalise_title("BYD hits 16M NEV milestone | CarNewsChina.com") == \
        "byd hits 16m nev milestone"


def test_earliest_in_corpus_picks_earlier_copy() -> None:
    target_time = _t("2026-04-17T14:00:00")
    corpus = [
        CorpusEntry(
            url="https://pressroom.toyota.com/camry-2026",
            title="Toyota unveils 2026 Camry plug-in hybrid",
            published_at=_t("2026-04-17T08:00:00"),
            domain="pressroom.toyota.com",
        ),
        CorpusEntry(
            url="https://carbuzz.com/toyota-camry-2026-phev",
            title="Toyota Unveils 2026 Camry With Plug-In Hybrid — CarBuzz",
            published_at=_t("2026-04-17T11:00:00"),
            domain="carbuzz.com",
        ),
    ]
    res = detect_earliest_in_corpus(
        article_url="https://motortrend.com/news/toyota-2026-camry",
        article_title="Toyota unveils 2026 Camry with plug-in hybrid option — MotorTrend",
        article_published_at=target_time,
        corpus=corpus,
        whitelist_domains={"carbuzz.com"},
        press_release_hosts=["pressroom.toyota.com"],
    )
    assert res is not None
    url, dom, conf = res
    # press-release host wins even though both are earlier
    assert dom == "pressroom.toyota.com"
    assert conf == "high"


def test_earliest_in_corpus_prefers_whitelist_over_unknown() -> None:
    target_time = _t("2026-04-17T14:00:00")
    corpus = [
        CorpusEntry(
            url="https://example.com/obscure-leak",
            title="BYD reaches 16 million NEV production milestone",
            published_at=_t("2026-04-17T09:00:00"),
            domain="example.com",
        ),
        CorpusEntry(
            url="https://carnewschina.com/2026/04/17/byd-16m",
            title="BYD reaches 16 millionth NEV production milestone",
            published_at=_t("2026-04-17T09:00:00"),
            domain="carnewschina.com",
        ),
    ]
    res = detect_earliest_in_corpus(
        article_url="https://cnevpost.com/byd-16m-milestone",
        article_title="BYD reaches 16 millionth NEV production milestone",
        article_published_at=target_time,
        corpus=corpus,
        whitelist_domains={"carnewschina.com"},
        press_release_hosts=[],
    )
    assert res is not None
    _, dom, conf = res
    assert dom == "carnewschina.com"
    assert conf == "medium"


def test_earliest_in_corpus_ignores_later_and_same_url() -> None:
    target_time = _t("2026-04-17T10:00:00")
    corpus = [
        # Later than target — must be ignored.
        CorpusEntry(
            url="https://other.com/story",
            title="Toyota unveils 2026 Camry plug-in hybrid",
            published_at=_t("2026-04-17T12:00:00"),
            domain="other.com",
        ),
    ]
    res = detect_earliest_in_corpus(
        article_url="https://motortrend.com/toyota-camry-2026",
        article_title="Toyota unveils 2026 Camry plug-in hybrid option",
        article_published_at=target_time,
        corpus=corpus,
        whitelist_domains=set(),
        press_release_hosts=[],
    )
    assert res is None


def test_mirror_links_are_ignored_in_level1() -> None:
    # A t.me post links to the same author's MAX mirror — not a primary source.
    # But it also links to the brand's press room, which IS a primary source.
    url, dom, conf = detect_primary_source(
        article_url="https://t.me/autonews_channel/12345",
        body="Подробности. Автор также выкладывает на https://max.ru/autonews. "
             "Сообщает официальный источник.",
        title="Geely объявила отзыв Polestar 4",
        outbound_links=[
            "https://max.ru/autonews/4567",        # mirror — must be ignored
            "https://t.me/autonews_channel/12344",  # self-link — must be ignored
            "https://vk.com/autonews_page",         # vk mirror — must be ignored
            "https://pressroom.toyota.com/x",       # the real primary source
        ],
        brands=BRANDS,
        cues=CUES,
    )
    assert dom == "pressroom.toyota.com"
    assert conf == "high"


def test_mirror_only_outbound_falls_through_to_self() -> None:
    # If all outbound links are mirrors, Level 1 can't find anything →
    # falls back to the article itself with low confidence.
    url, dom, conf = detect_primary_source(
        article_url="https://t.me/autonews_channel/99",
        body="Обзор рынка.",
        title="Какой-то общий обзор рынка",
        outbound_links=[
            "https://max.ru/autonews/1",
            "https://vk.com/autonews_page",
        ],
        brands=BRANDS,
        cues=CUES,
    )
    assert dom == "t.me"
    assert conf == "low"


def test_mirror_entry_not_picked_from_corpus() -> None:
    target_time = _t("2026-04-17T14:00:00")
    corpus = [
        # MAX mirror was earlier but it is NOT a primary source.
        CorpusEntry(
            url="https://max.ru/autonews/9",
            title="BYD reaches 16 millionth NEV production milestone",
            published_at=_t("2026-04-17T09:00:00"),
            domain="max.ru",
        ),
        # carnewschina — legitimate earlier source.
        CorpusEntry(
            url="https://carnewschina.com/2026/04/17/byd-16m",
            title="BYD reaches 16 millionth NEV production milestone",
            published_at=_t("2026-04-17T10:30:00"),
            domain="carnewschina.com",
        ),
    ]
    res = detect_earliest_in_corpus(
        article_url="https://cnevpost.com/byd-16m-milestone",
        article_title="BYD reaches 16 millionth NEV production milestone",
        article_published_at=target_time,
        corpus=corpus,
        whitelist_domains={"carnewschina.com"},
        press_release_hosts=[],
        mirror_hosts=["max.ru", "t.me", "vk.com"],
    )
    assert res is not None
    _, dom, _ = res
    assert dom == "carnewschina.com"  # mirror skipped despite being earlier


def test_earliest_in_corpus_no_match_returns_none() -> None:
    target_time = _t("2026-04-17T14:00:00")
    corpus = [
        CorpusEntry(
            url="https://carbuzz.com/ford-f150-news",
            title="Ford F-150 recall announced",
            published_at=_t("2026-04-17T11:00:00"),
            domain="carbuzz.com",
        ),
    ]
    res = detect_earliest_in_corpus(
        article_url="https://example.com/byd-milestone",
        article_title="BYD reaches 16 millionth NEV production milestone",
        article_published_at=target_time,
        corpus=corpus,
        whitelist_domains=set(),
        press_release_hosts=[],
    )
    assert res is None
