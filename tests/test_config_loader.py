from news_agent.core.config_loader import (
    SourceQuality,
    load_brand_domains,
    load_primary_source_cues,
    load_sections,
    load_source_quality,
    load_sources_overrides,
    load_sources_schema,
)


def test_sections_are_nonempty() -> None:
    sections = load_sections()
    names = {s.name for s in sections}
    # These are the real canonical names from the editor's 'Разделы новостей' tab.
    assert {
        "Confirmed",
        "Economics",
        "Rumors",
        "Other news",
        "Local specifics",
        "LCV news",
        "Dealer news / Promo",
        "Motorshow",
        "Test-drive",
    }.issubset(names)


def test_schema_has_required_columns() -> None:
    schema = load_sources_schema()
    assert schema.name
    assert schema.url


def test_overrides_returns_list() -> None:
    assert isinstance(load_sources_overrides(), list)


def test_brands_seed_is_large_enough() -> None:
    assert len(load_brand_domains()) >= 30


def test_cues_have_ru_and_en() -> None:
    cues = load_primary_source_cues()
    assert cues.phrases.get("ru")
    assert cues.phrases.get("en")
    assert "prnewswire.com" in cues.press_release_hosts


def test_source_quality_loads() -> None:
    sq = load_source_quality()
    assert isinstance(sq, SourceQuality)
    # Editor flagged these in apr-2026 chat feedback
    assert sq.is_low_quality("https://t.me/sergtselikov")
    assert sq.is_low_quality("https://t.me/sergtselikov/")
    assert sq.is_low_quality("https://t.me/sergtselikov/123")  # post URL too
    assert sq.is_low_quality("https://t.me/autopotoknews")


def test_source_quality_domain_match() -> None:
    sq = SourceQuality(low_quality={"bizneskorea.ru"}, high_quality=set())
    assert sq.is_low_quality("https://bizneskorea.ru/article/123")
    assert sq.is_low_quality("https://www.bizneskorea.ru/article/123")
    # Different domain — must NOT match
    assert not sq.is_low_quality("https://hyundai.com/news")


def test_source_quality_empty_url_is_safe() -> None:
    sq = load_source_quality()
    assert not sq.is_low_quality("")
    assert not sq.is_low_quality(None)


def test_source_quality_unknown_url_not_flagged() -> None:
    sq = load_source_quality()
    assert not sq.is_low_quality("https://autostat.ru/news/12345")
    assert not sq.is_low_quality("https://t.me/chinamashina_news")  # legit channel
