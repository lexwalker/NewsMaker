from news_agent.core.config_loader import (
    load_brand_domains,
    load_primary_source_cues,
    load_sections,
    load_sources_overrides,
    load_sources_schema,
)


def test_sections_are_nonempty() -> None:
    sections = load_sections()
    names = {s.name for s in sections}
    assert {"facts", "economy", "rumors", "other"}.issubset(names)


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
