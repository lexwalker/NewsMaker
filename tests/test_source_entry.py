"""normalise_source_entry — the editor's source sheet mixes full URLs,
bare domains ("kolesa.ru") and domain+path ("vesti.ru/auto", "t.me/x").
Requiring an http scheme used to SILENTLY skip 33 active sources (drom,
motor.ru, zr.ru, lenta, rbc, insideevs …) — the largest single coverage
hole the jun-2026 miss-funnel found. These tests pin the normalisation.
"""

from news_agent.core.urls import normalise_source_entry


def test_full_urls_pass_through() -> None:
    assert normalise_source_entry("https://tass.ru/ekonomika") == \
        "https://tass.ru/ekonomika"
    assert normalise_source_entry("http://example.com/x") == \
        "http://example.com/x"


def test_bare_domain_gets_https() -> None:
    assert normalise_source_entry("kolesa.ru") == "https://kolesa.ru"
    assert normalise_source_entry("drom.ru") == "https://drom.ru"
    assert normalise_source_entry("www.asm-holding.ru") == \
        "https://www.asm-holding.ru"


def test_domain_with_path() -> None:
    assert normalise_source_entry("vesti.ru/auto") == "https://vesti.ru/auto"
    assert normalise_source_entry("t.me/delimobil") == "https://t.me/delimobil"


def test_whitespace_trimmed() -> None:
    assert normalise_source_entry("  kolesa.ru  ") == "https://kolesa.ru"


def test_non_urls_rejected() -> None:
    # stray words / notes in the URL column must be dropped, not fetched
    assert normalise_source_entry("экономика") is None
    assert normalise_source_entry("посмотреть позже kolesa.ru") is None
    assert normalise_source_entry("") is None
    assert normalise_source_entry("   ") is None


def test_leading_slashes_handled() -> None:
    assert normalise_source_entry("//kolesa.ru") == "https://kolesa.ru"
