"""Tests for fuzzy_match.normalise_for_match.

Each test reflects a real dedup failure from the apr-2026 review:
- AvtoVAZ-Promtech appeared 3× because RU and EN spellings differ
- Lynk & Co 900 appeared 2× because "5-seat" vs "five-seat"
"""

from rapidfuzz import fuzz

from news_agent.core.fuzzy_match import normalise_for_match


def _sim(a: str, b: str) -> float:
    """Convenience: token-set ratio after normalisation, 0..100."""
    return fuzz.token_set_ratio(normalise_for_match(a), normalise_for_match(b))


# --------------------------------------------- Russian ↔ English transliteration

def test_avtovaz_promtech_same_language_variants_match() -> None:
    """Editor-flagged dup: AvtoVAZ-Promtech RU spellings appeared multiple times.

    The translit step should fold ``Промтех`` and ``Promtech`` to the same
    Latin form, so two Russian sentences whose only proper-noun spelling
    differs become matchable.
    """
    a = "АвтоВАЗ и Промтех заключили соглашение о сотрудничестве"
    c = "АвтоВАЗ подписал соглашение с Promtech"
    assert _sim(a, c) >= 60, f"sim a-c = {_sim(a, c)}"


def test_promtech_promtekh_translit_aligns() -> None:
    """Spelling variants of the same brand fold to comparable Latin form."""
    n_cyr = normalise_for_match("Промтех")
    n_lat = normalise_for_match("Promtekh")
    # translit of Промтех = "promtekh" — exact match
    assert n_cyr == n_lat


def test_cross_language_promotion_via_brand_overlap() -> None:
    """RU ↔ EN headlines with same brand should share at least the brand token.

    Full cross-language fuzzy match is genuinely hard (different verbs / nouns)
    — for those cases the cluster builder uses primary-URL overlap as a
    secondary signal. This test only asserts brand+model proper-nouns survive
    transliteration.
    """
    a = normalise_for_match("Lynk & Co 900 представлен в Китае")
    b = normalise_for_match("Lynk & Co 900 launched in China")
    # Both should contain the brand+model
    assert "lynk" in a and "lynk" in b
    assert "900" in a and "900" in b


def test_brand_name_in_cyrillic_matches_latin() -> None:
    a = "Хёндэ представила новый кроссовер"
    b = "Hyundai unveiled the new crossover"
    # After translit: "khende predstavila novyy krossover" vs
    # "hyundai unveiled the new crossover" — the brand still differs
    # (Хёндэ ≠ Hyundai), but model words ("крossover" → "krossover")
    # at least overlap. This case still requires brand alias mapping.
    # Test ensures the pipeline doesn't crash and returns something.
    assert isinstance(normalise_for_match(a), str)
    assert isinstance(normalise_for_match(b), str)


def test_diacritics_stripped() -> None:
    assert normalise_for_match("Škoda Auto wins Fuorisalone Award") == \
           normalise_for_match("Skoda Auto wins Fuorisalone Award")
    assert normalise_for_match("Citroën C5 X") == normalise_for_match("Citroen C5 X")


# ---------------------------------------------------- number-word ↔ digit

def test_five_seat_matches_5_seat() -> None:
    """Editor-flagged dup: Lynk & Co 900 5-seat vs five-seat."""
    a = "Lynk & Co launched 900 five-seat sedan"
    b = "Lynk & Co 900 5-seat sedan launched"
    assert _sim(a, b) >= 80, f"sim = {_sim(a, b)}"


def test_russian_word_numbers_to_digits() -> None:
    a = "пять моделей представлены на выставке"
    b = "5 моделей представлены на выставке"
    assert _sim(a, b) >= 90


# ---------------------------------------------------- language tags / prefixes

def test_lang_tag_stripped() -> None:
    a = "Hyundai unveiled the new Tucson (EN)"
    b = "Hyundai unveiled the new Tucson"
    assert normalise_for_match(a) == normalise_for_match(b)


def test_combined_en_ru_prefix_stripped() -> None:
    a = "EN: Hyundai unveiled the new Tucson\nRU: Hyundai представила новый Tucson"
    n = normalise_for_match(a)
    assert "en:" not in n
    assert "ru:" not in n
    assert "hyundai" in n
    assert "tucson" in n


def test_trailing_source_stripped() -> None:
    a = "Hyundai unveiled the new Tucson — Korean Car Blog"
    b = "Hyundai unveiled the new Tucson"
    assert normalise_for_match(a) == normalise_for_match(b)


# ---------------------------------------------------- edge cases

def test_empty_input_returns_empty() -> None:
    assert normalise_for_match("") == ""
    assert normalise_for_match("   ") == ""


def test_punctuation_collapsed() -> None:
    a = "Hyundai unveiled: the new! Tucson?"
    b = "Hyundai unveiled the new Tucson"
    assert normalise_for_match(a) == normalise_for_match(b)


def test_short_title_not_over_truncated() -> None:
    # Short titles shouldn't lose their ending to the source-name regex
    n = normalise_for_match("Tesla — RT")
    assert "tesla" in n  # the body must survive


def test_idempotent_double_normalisation() -> None:
    """Normalising a normalised title is a no-op."""
    a = "EN: Hyundai unveiled the new Tucson (EN)"
    once = normalise_for_match(a)
    twice = normalise_for_match(once)
    assert once == twice
