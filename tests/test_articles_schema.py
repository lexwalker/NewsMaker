"""Canonical articles-tab schema (package 3 of the architecture review).

The 34-column header used to be hand-copied across ~10 scripts and had
already drifted (retry carried COL_LLM_REL=24 → 'Hits темы'). These tests
pin the schema contract: named indices derive from the list, letters match
the historical A1 usage, and check_header catches the deadly mid-insert.
"""

from news_agent.core.articles_schema import (
    ARTICLES_HEADER,
    COL,
    FULL_RANGE_COLS,
    check_header,
    col_letter,
)


def test_letters_match_historical_a1_usage() -> None:
    # These letters are burned into years of sheet formulas and scripts —
    # the schema must reproduce them exactly.
    assert col_letter(COL.TITLE) == "B"
    assert col_letter(COL.SECTION) == "E"
    assert col_letter(COL.REGION) == "F"
    assert col_letter(COL.NOTE) == "M"
    assert col_letter(COL.VERDICT) == "O"
    assert col_letter(COL.LLM_RELEVANCE) == "Z"
    assert col_letter(COL.COST) == "AA"
    assert col_letter(COL.LLM_REASON) == "AE"
    assert col_letter(COL.EVENT_TYPE) == "AH"


def test_full_range_covers_whole_schema() -> None:
    assert len(ARTICLES_HEADER) == 34
    assert FULL_RANGE_COLS == "A:AH"


def test_check_header_catches_mid_insert() -> None:
    bad = ARTICLES_HEADER[:4] + ["ВСТАВКА"] + ARTICLES_HEADER[4:]
    problems = check_header(bad, context="tab")
    assert problems and "ВСТАВКА" in problems[0]


def test_check_header_tolerates_older_short_tabs() -> None:
    # Tabs written before the AC-AH columns were appended have a shorter
    # header — that's legitimate, not a shifted schema.
    assert check_header(ARTICLES_HEADER[:28], context="tab") == []
    assert check_header(list(ARTICLES_HEADER), context="tab") == []


def test_check_header_catches_rename() -> None:
    renamed = list(ARTICLES_HEADER)
    renamed[COL.VERDICT] = "Вердикт 2.0"
    problems = check_header(renamed, context="tab")
    assert problems and "Вердикт 2.0" in problems[0]
