"""Tests for the Sheets 50k-cell guard (regression: v38 crash)."""

from news_agent.core.sheets_util import SHEETS_CELL_MAX, clamp_cells


def test_overlong_string_truncated() -> None:
    big = "x" * 60_000
    out = clamp_cells([[big]])
    assert len(out[0][0]) == SHEETS_CELL_MAX
    assert out[0][0] == "x" * SHEETS_CELL_MAX


def test_short_strings_untouched() -> None:
    rows = [["hello", "мир"], ["a" * 100, ""]]
    assert clamp_cells(rows) == rows


def test_non_string_cells_pass_through() -> None:
    rows = [[1, 2.5, None, True, "ok"]]
    out = clamp_cells(rows)
    assert out == [[1, 2.5, None, True, "ok"]]
    # types preserved (not stringified)
    assert isinstance(out[0][0], int)
    assert isinstance(out[0][1], float)


def test_mixed_row_only_long_cell_clamped() -> None:
    rows = [["short", "y" * 50_000, 42, "tail"]]
    out = clamp_cells(rows)
    assert out[0][0] == "short"
    assert len(out[0][1]) == SHEETS_CELL_MAX
    assert out[0][2] == 42
    assert out[0][3] == "tail"


def test_non_mutating() -> None:
    original = [["z" * 60_000]]
    snapshot = original[0][0]
    clamp_cells(original)
    # input grid unchanged
    assert original[0][0] is snapshot
    assert len(original[0][0]) == 60_000


def test_custom_max_len() -> None:
    out = clamp_cells([["abcdef"]], max_len=3)
    assert out[0][0] == "abc"


def test_empty_grid_safe() -> None:
    assert clamp_cells([]) == []
    assert clamp_cells([[]]) == [[]]


def test_exactly_at_limit_not_truncated() -> None:
    exact = "q" * SHEETS_CELL_MAX
    out = clamp_cells([[exact]])
    assert out[0][0] == exact
    assert len(out[0][0]) == SHEETS_CELL_MAX
