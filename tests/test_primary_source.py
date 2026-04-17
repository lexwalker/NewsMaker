from news_agent.core.config_loader import BrandDomainEntry, PrimarySourceCues
from news_agent.core.primary_source import detect_primary_source

BRANDS = [
    BrandDomainEntry(brand="Toyota", aliases=["Тойота"], domains=["toyota.com", "pressroom.toyota.com"]),
    BrandDomainEntry(brand="BMW", domains=["bmw.com", "press.bmwgroup.com"]),
]
CUES = PrimarySourceCues(
    phrases={"en": ["press release", "according to"], "ru": ["пресс-релиз", "сообщает"]},
    press_release_hosts=["prnewswire.com", "pressroom.toyota.com"],
)


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
