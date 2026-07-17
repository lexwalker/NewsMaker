from datetime import datetime, timedelta, timezone

from news_agent.core.config_loader import BrandDomainEntry, PrimarySourceCues
from news_agent.core.primary_source import (
    CorpusEntry,
    arbitration_candidates,
    detect_earliest_in_corpus,
    detect_primary_source,
    normalise_title,
    _is_junk_link,
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


# ----- Plan P2-A: redistribution host → promote journalistic primary --------

def test_redistribution_host_promotes_carscoops() -> None:
    """auto.mail.ru reposting a Carscoops story → Carscoops is primary.
    Editor row 92: «первоисточником должен быть Carscoops»."""
    url, dom, conf = detect_primary_source(
        article_url="https://auto.mail.ru/article/12345-nissan-plant/",
        body="По данным Carscoops, завод Nissan переедет. Подробности.",
        title="Nissan переносит европейский завод",
        outbound_links=["https://www.carscoops.com/2026/05/nissan-plant-move/"],
        brands=BRANDS,
        cues=CUES,
    )
    assert dom.endswith("carscoops.com")
    assert conf == "high"


def test_redistribution_host_promotes_ancap() -> None:
    """1prime.ru reposting an ANCAP crash test → ANCAP is primary.
    Editor row 98: «первоисточником должен быть ANCAP»."""
    url, dom, conf = detect_primary_source(
        article_url="https://1prime.ru/20260512/byd-seal-99999.html",
        body="BYD Seal 6 DM-i прошёл краш-тест ANCAP с максимальной оценкой.",
        title="BYD Seal 6 DM-i получил 5 звёзд ANCAP",
        outbound_links=["https://www.ancap.com.au/safety-ratings/byd/seal-6/"],
        brands=BRANDS,
        cues=CUES,
    )
    assert dom.endswith("ancap.com.au")
    assert conf == "high"


def test_non_redistribution_host_unaffected_by_p2a() -> None:
    """A normal blog linking to carscoops must NOT trigger Tier 1.5 —
    it should fall through to existing tiers (low confidence here)."""
    url, dom, conf = detect_primary_source(
        article_url="https://autoblog.example/story",
        body="Some article about a Carscoops piece, no cue phrase.",
        title="Some auto headline",
        outbound_links=["https://www.carscoops.com/2026/05/story/"],
        brands=BRANDS,
        cues=CUES,
    )
    # autoblog.example is NOT a redistribution host → Tier 1.5 skipped.
    # No brand domain, no cue phrase → falls back to self/low.
    assert url == "https://autoblog.example/story"
    assert conf == "low"


def test_redistribution_host_no_preferred_link_falls_through() -> None:
    """auto.mail.ru but the only outbound is a random blog → Tier 1.5
    does not fire; behaviour is the pre-existing fallback."""
    url, dom, conf = detect_primary_source(
        article_url="https://auto.mail.ru/article/777-some-news/",
        body="Просто новость без ссылки на признанный первоисточник.",
        title="Автомобильная новость",
        outbound_links=["https://randomblog.example/post/1"],
        brands=BRANDS,
        cues=CUES,
    )
    # No preferred-primary link → Tier 1.5 inert, falls to self/low.
    assert url == "https://auto.mail.ru/article/777-some-news/"
    assert conf == "low"


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


# --- jun-2026: share-widget aggregators + bare social profiles (prog v2 bugs)

def test_is_junk_link_share_widgets_and_profiles() -> None:
    # addtoany share wrapper (Mazda CX-90 case) — never a primary
    assert _is_junk_link(
        "https://www.addtoany.com/add_to/facebook?linkurl=https%3A%2F%2Ftopclassactions.com%2Fx")
    # bare social profiles (Rivian case: author's Twitter profile)
    assert _is_junk_link("https://twitter.com/MicheleTheodore")
    assert _is_junk_link("https://x.com/SomeJournalist")
    assert _is_junk_link("https://www.facebook.com/SomePage")
    # a genuine post/status is NOT junk
    assert not _is_junk_link("https://twitter.com/Rivian/status/123456789")
    assert not _is_junk_link("https://www.facebook.com/Rivian/posts/123")
    # t.me is a legitimate source here — must NOT be rejected by this filter
    assert not _is_junk_link("https://t.me/autonews_channel/4567")
    # a normal article passes
    assert not _is_junk_link("https://www.cnbc.com/2026/06/16/rivian-layoffs.html")


def test_addtoany_wrapper_falls_back_to_real_article() -> None:
    """Mazda CX-90 case: addtoany share wrapper as the only outbound →
    primary must fall back to the real fetched article, not the wrapper."""
    url, dom, conf = detect_primary_source(
        article_url="https://topclassactions.com/lawsuit-settlements/investigations/mazda-cx-90-steering/",
        body="Investigation into Mazda CX-90 steering. According to the filing...",
        title="Mazda CX-90 class action investigation",
        outbound_links=[
            "https://www.addtoany.com/add_to/facebook?linkurl=https%3A%2F%2Ftopclassactions.com%2Fx",
        ],
        brands=BRANDS, cues=CUES,
    )
    assert "addtoany.com" not in url
    assert url == "https://topclassactions.com/lawsuit-settlements/investigations/mazda-cx-90-steering/"


def test_bare_twitter_profile_falls_back_to_real_article() -> None:
    """Rivian case: a bare Twitter profile must never win over the article."""
    url, dom, conf = detect_primary_source(
        article_url="https://www.cnbc.com/2026/06/16/rivian-layoffs.html",
        body="Rivian cuts jobs. According to the report, the company...",
        title="Rivian cuts hundreds of jobs",
        outbound_links=["https://twitter.com/MicheleTheodore"],
        brands=BRANDS, cues=CUES,
    )
    assert "twitter.com" not in url
    assert url == "https://www.cnbc.com/2026/06/16/rivian-layoffs.html"


# --------- jul-02 primary-source fixes (editor complaints batch) ----------

def _brands_min():
    from news_agent.core.config_loader import BrandDomainEntry
    return [BrandDomainEntry(brand="Voyt", domains=["voyt.ru"], aliases=[])]


def _cues_min():
    from news_agent.core.config_loader import PrimarySourceCues
    return PrimarySourceCues(phrases={"ru": ["сообщает"]}, press_release_hosts=[],
                             mirror_hosts=[])


def test_own_cdn_subdomain_never_primary() -> None:
    """media.ixbt.com (the article site's own CDN subdomain + an image-resize
    URL) was promoted to primary with medium confidence — both filters must
    reject it now."""
    from news_agent.core.primary_source import detect_primary_source
    url, dom, conf = detect_primary_source(
        article_url="https://www.ixbt.com/news/2026/07/01/voyt.html",
        body="Российский электрокроссовер Voyt, сообщает пресс-служба.",
        title="Voyt показан в движении",
        outbound_links=[
            "https://media.ixbt.com/fit-in/1110x/https://www.ixbt.com/img/x.jpg",
        ],
        brands=_brands_min(), cues=_cues_min(), whitelist_domains=set(),
    )
    assert conf == "low" and "ixbt.com" in dom  # fell back to self, honest low


def test_image_links_are_junk() -> None:
    from news_agent.core.primary_source import _is_junk_link
    assert _is_junk_link("https://cdn.site.com/fit-in/1110x/pic")
    assert _is_junk_link("https://site.com/uploads/photo.jpg")
    assert _is_junk_link("https://site.com/img/banner.webp")
    assert not _is_junk_link("https://voyt.ru/press/new-model-2026")


def test_redistribution_host_does_not_self_certify_via_whitelist() -> None:
    """naavtotrasse is fetch-whitelisted, but a repost must never become its
    own primary source at HIGH confidence (it masked the real source)."""
    from news_agent.core.primary_source import detect_primary_source
    url, dom, conf = detect_primary_source(
        article_url="https://naavtotrasse.ru/auto-news/shtrafy.html",
        body="Штрафы выросли, пишет Autonews.ru.",
        title="Штрафы в I полугодии",
        outbound_links=[],
        brands=_brands_min(), cues=_cues_min(),
        whitelist_domains={"naavtotrasse.ru"},
    )
    assert conf == "low"  # self-certification blocked; falls back to self




def test_marked_source_root_link_NOT_used() -> None:
    """A bare domain-root source credit («источник: rg.ru/») is useless — the
    editor needs a DEEP article link, so a root hint must be ignored."""
    from news_agent.core.primary_source import detect_primary_source
    url, dom, conf = detect_primary_source(
        article_url="https://auto.mail.ru/article/126272-expert-said.html",
        body="Эксперт рассказал.", title="Эксперт",
        outbound_links=[], source_hint_url="https://rg.ru/",
        brands=_brands_min(), cues=_cues_min(), whitelist_domains=set(),
    )
    assert dom != "rg.ru"  # root NOT promoted


def test_marked_source_deep_link_used() -> None:
    """A DEEP source credit (points at the actual article) IS used at high."""
    from news_agent.core.primary_source import detect_primary_source
    url, dom, conf = detect_primary_source(
        article_url="https://auto.mail.ru/article/126272-expert-said.html",
        body="Эксперт рассказал.", title="Эксперт",
        outbound_links=[],
        source_hint_url="https://rg.ru/2026/07/03/ekspert-nazval-os.html",
        brands=_brands_min(), cues=_cues_min(), whitelist_domains=set(),
    )
    assert dom == "rg.ru" and conf == "high"


def test_marked_source_same_site_ignored() -> None:
    from news_agent.core.primary_source import detect_primary_source
    url, dom, conf = detect_primary_source(
        article_url="https://auto.mail.ru/article/x.html",
        body="text", title="t", outbound_links=[],
        source_hint_url="https://auto.mail.ru/tag/news",  # same site
        brands=_brands_min(), cues=_cues_min(), whitelist_domains=set(),
    )
    assert dom != "auto.mail.ru" or conf == "low"  # not promoted


def test_pick_marked_source_by_attribute() -> None:
    from bs4 import BeautifulSoup
    from news_agent.adapters.fetchers.html import _pick_marked_source
    html = ('<main><p>текст</p>'
            '<div>источник: <a href="https://rg.ru/" '
            'data-qa-detail="ArticleSourceLink">Российская газета</a></div>'
            '</main>')
    soup = BeautifulSoup(html, "lxml")
    assert _pick_marked_source(soup, "https://auto.mail.ru/a") == "https://rg.ru/"


def test_pick_marked_source_by_label() -> None:
    from bs4 import BeautifulSoup
    from news_agent.adapters.fetchers.html import _pick_marked_source
    html = '<article>Источник: <a href="https://www.gazeta.ru/">Газета</a></article>'
    soup = BeautifulSoup(html, "lxml")
    assert _pick_marked_source(soup, "https://kolesa.ru/x") == "https://www.gazeta.ru/"


def test_pick_marked_source_ignores_share_widgets() -> None:
    from bs4 import BeautifulSoup
    from news_agent.adapters.fetchers.html import _pick_marked_source
    html = ('<article>text <a href="https://vk.com/share?url=x" '
            'class="share-source-btn">Поделиться</a></article>')
    soup = BeautifulSoup(html, "lxml")
    assert _pick_marked_source(soup, "https://site.ru/x") == ""


def test_corpus_hint_prefers_credited_outlet_deep_url() -> None:
    """auto.mail credits «источник: rg.ru» (homepage only). If rg.ru's actual
    article is in our corpus, the corpus pass returns its DEEP url — even
    without an earlier timestamp — because the credit is authoritative."""
    from datetime import datetime, timezone
    from news_agent.core.primary_source import (
        CorpusEntry, detect_earliest_in_corpus)
    t = datetime(2026, 7, 3, 10, tzinfo=timezone.utc)
    corpus = [
        CorpusEntry(url="https://rg.ru/2026/07/03/ekspert-nazval-uyazvimye-os.html",
                    title="Эксперт назвал уязвимые ОС в автомобилях",
                    published_at=t, domain="rg.ru"),
        CorpusEntry(url="https://irrelevant.ru/x", title="Совсем другое",
                    published_at=t, domain="irrelevant.ru"),
    ]
    res = detect_earliest_in_corpus(
        article_url="https://auto.mail.ru/article/126272-ekspert.html",
        article_title="Эксперт назвал уязвимые ОС в машинах",
        article_published_at=t,          # SAME time — earlier-gate would fail
        corpus=corpus, source_hint_domain="rg.ru",
    )
    assert res is not None
    url, dom, conf = res
    assert dom == "rg.ru" and url.endswith(".html") and conf == "high"


def test_corpus_hint_no_match_falls_through() -> None:
    from datetime import datetime, timezone
    from news_agent.core.primary_source import (
        CorpusEntry, detect_earliest_in_corpus)
    t = datetime(2026, 7, 3, 10, tzinfo=timezone.utc)
    # hint domain not in corpus → hint shortcut yields nothing; no earlier
    # entry either → None
    corpus = [CorpusEntry(url="https://other.ru/a", title="Эксперт назвал ОС",
                          published_at=t, domain="other.ru")]
    res = detect_earliest_in_corpus(
        article_url="https://auto.mail.ru/article/x.html",
        article_title="Эксперт назвал уязвимые ОС в машинах",
        article_published_at=t, corpus=corpus, source_hint_domain="rg.ru")
    assert res is None


# --- LLM arbitration trigger (arbitration_candidates) ----------------------
# autonews.ru is a hardcoded redistribution host; carscoops.com a preferred
# journalistic primary. Toyota is a known brand (fixture BRANDS).
_ARB_TITLE = "Toyota объявила о старте продаж нового кроссовера"
_ARB_BODY = "Компания Toyota (Тойота) официально сообщает о начале продаж. " * 8
_ARB_LINKS = [
    "https://www.carscoops.com/2026/07/toyota-new-crossover/",  # preferred (strong)
    "https://www.toyota.com/news/new-crossover-launch",         # brand+mentioned (strong)
    "https://t.me/autonews_ru",                                 # mirror → filtered
    "https://www.autonews.ru/section/market",                   # same-site → skipped
]


def test_arbitration_fires_on_contested_redistribution() -> None:
    # Redistribution host + BOTH a preferred primary AND a mentioned-brand site
    # → genuine contest → both surfaced to the LLM, junk/mirror/self excluded.
    cands = arbitration_candidates(
        article_url="https://www.autonews.ru/news/abc123",
        body=_ARB_BODY, title=_ARB_TITLE, outbound_links=_ARB_LINKS,
        brands=BRANDS, cues=CUES)
    assert set(cands) == {
        "https://www.carscoops.com/2026/07/toyota-new-crossover/",
        "https://www.toyota.com/news/new-crossover-launch",
    }


def test_arbitration_empty_on_non_redistribution_host() -> None:
    # A real outlet (not a redistributor) keeps 100% deterministic behaviour —
    # the arbiter never fires, so no paid call and zero regression risk.
    cands = arbitration_candidates(
        article_url="https://news.drom.ru/toyota-123.html",
        body=_ARB_BODY, title=_ARB_TITLE, outbound_links=_ARB_LINKS,
        brands=BRANDS, cues=CUES)
    assert cands == []


def test_arbitration_empty_on_single_candidate() -> None:
    # One plausible primary is not a contest — Tier 1.5 already picks it right.
    cands = arbitration_candidates(
        article_url="https://www.autonews.ru/news/abc123",
        body=_ARB_BODY, title=_ARB_TITLE,
        outbound_links=["https://www.carscoops.com/2026/07/x/",
                        "https://t.me/x", "https://www.autonews.ru/subscribe"],
        brands=BRANDS, cues=CUES)
    assert cands == []


def test_arbitration_requires_a_strong_anchor() -> None:
    # Two deep external links but NEITHER is a known primary/brand → generic
    # related-reading soup, not a source contest → no call.
    cands = arbitration_candidates(
        article_url="https://www.autonews.ru/news/abc123",
        body="Обычная новость без упоминания брендов. " * 8,
        title="Некое событие на авторынке",
        outbound_links=["https://example.com/some-deep-article-1",
                        "https://another.org/some-deep-article-2"],
        brands=BRANDS, cues=CUES)
    assert cands == []


# --- Legal/service boilerplate must never become a primary ------------------
# jul-17 (editor + live feed): a short thesupercarblog post had exactly ONE
# external link — akismet.com/privacy/, the WordPress comment-form notice — and
# Tier 4 ("cue phrase + any external link") promoted it to primary at medium.
# The correct answer is self@low: the post cites no external source at all.

def test_akismet_privacy_link_is_junk() -> None:
    assert _is_junk_link("https://akismet.com/privacy/")


def test_legal_policy_paths_are_junk() -> None:
    for u in ("https://site.com/privacy/", "https://site.com/privacy-policy",
              "https://site.com/terms/", "https://site.com/terms-of-use",
              "https://site.com/cookie-policy", "https://site.com/legal/",
              "https://site.com/imprint", "https://site.com/gdpr"):
        assert _is_junk_link(u), u


def test_article_about_privacy_law_is_not_junk() -> None:
    # Guard the guard: a real article whose SLUG mentions privacy must survive.
    for u in ("https://rg.ru/2026/07/16/new-privacy-law-cars.html",
              "https://autonews.ru/news/privacy-rules-for-evs"):
        assert not _is_junk_link(u), u


def test_wordpress_only_external_link_falls_back_to_self() -> None:
    """The exact live case: cue phrase present, the only external link is the
    Akismet policy → must NOT be promoted; the article itself is the source."""
    url, dom, conf = detect_primary_source(
        article_url="https://www.thesupercarblog.com/bentley-torcal-fake-sound/",
        body="Bentley has confirmed the Torcal will play a synthetic sound. "
             "According to the brand, the audio was recorded live. " * 3,
        title="Bentley Torcal electric SUV will have a fake engine sound",
        outbound_links=["https://akismet.com/privacy/"],
        brands=BRANDS, cues=CUES,
    )
    assert dom == "www.thesupercarblog.com" or dom.endswith("thesupercarblog.com")
    assert conf == "low"


# --- Tier-1 RU outlets as arbitration anchors (jul-17 measurement + editor) --
# The fire-rate measurement found the arbiter silently missing the cleanest
# cases: finmarket.ru → kommersant/vedomosti/interfax never fired because no
# tier-1 RU outlet counted as a "strong anchor". The editor asked for exactly
# this attribution the same week («первоисточник gazeta.ru…», «Первоисточник
# 1prime.ru…»). These hosts make the ARBITER fire only — they must never change
# a deterministic tier.

def test_arbitration_fires_on_aggregator_linking_ru_tier1() -> None:
    """finmarket.ru → Kommersant + Vedomosti: the measured miss."""
    cands = arbitration_candidates(
        article_url="https://www.finmarket.ru/main/article/6663892",
        body="Как сообщают Коммерсантъ и Ведомости, рынок вырос. " * 8,
        title="Рынок автокредитов вырос",
        outbound_links=[
            "https://www.kommersant.ru/doc/123456",
            "https://www.vedomosti.ru/auto/articles/2026/07/16/x",
        ],
        brands=BRANDS, cues=CUES)
    assert cands, "an aggregator linking two tier-1 RU outlets must be arbitrated"


def test_arbitration_does_not_fire_on_own_corporate_nav() -> None:
    """autonews.ru is an RBC property and carries auth./cash./id.rbc.ru nav on
    every page. rbc.ru is deliberately absent from the tier-1 list — including
    it made the arbiter fire on autonews' own chrome (measured jul-17)."""
    cands = arbitration_candidates(
        article_url="https://www.autonews.ru/news/abc123",
        body="Новость про рынок. " * 20, title="Некая новость",
        outbound_links=["https://auth.rbc.ru/", "https://cash.rbc.ru/",
                        "https://id.rbc.ru/", "https://plus.rbc.ru/"],
        brands=BRANDS, cues=CUES)
    assert cands == []


def test_ru_tier1_does_not_promote_deterministically() -> None:
    """The tier-1 list must NOT leak into Tier 1.5: a redistribution host
    linking a tier-1 outlet in a «читайте также» block must not get it as a
    HIGH-confidence primary with no LLM. (kommersant.ru is not a press-release
    host, so only the tier-1 list could promote it — it must not.)"""
    url, dom, conf = detect_primary_source(
        article_url="https://auto.mail.ru/article/12345-story/",
        body="Новость. Читайте также материал Коммерсанта. " * 8,
        title="Новость",
        outbound_links=["https://www.kommersant.ru/doc/999"],
        brands=BRANDS, cues=CUES)
    assert not (dom.endswith("kommersant.ru") and conf == "high"), (
        f"tier-1 list leaked into a deterministic tier: {dom}@{conf}")
