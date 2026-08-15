"""The editor's colour marks on column P, and telling them from our own fill.

On aug-15 the editor started painting the comment column himself: green for a
row that belonged in the feed, red for one that did not. That settles a
question this repo had been losing money on. The text parser could not read 46
of the 218 comments that week and left them out of the score entirely; the
colour has an opinion on every row, including the 4 he painted green without
writing anything at all.

The whole risk is confusing his mark with our own formatting. This sheet is
already painted by the pipeline — a tint per section, a dark band per run
separator, a dark green header — so "the cell is greenish" is not the test.
Every colour the pipeline uses is either PASTEL (no channel below .73) or DARK
(no channel above .5). A mark is saturated. These are the real colours, read
off the live sheet on aug-15.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.core.editor_feedback import (  # noqa: E402
    mark_from_background, precision_from_marks,
)


def rgb(r, g, b):
    return {"red": r, "green": g, "blue": b}


# ------------------------------------------------------- what he actually used

def test_the_three_marks_on_the_live_sheet() -> None:
    assert mark_from_background(rgb(0, 1, 0)) == "green"
    assert mark_from_background(rgb(1, 0, 0)) == "red"
    assert mark_from_background(rgb(1, 1, 0)) == "yellow"


# --------------------------------------------- and everything we paint ourselves

@pytest.mark.parametrize("colour", [
    (0.776, 0.949, 0.776),   # section tint — GREENISH, and the trap: a naive
                             # "is green dominant" test reads 827 of these as
                             # approvals and reports a precision near 100%
    (0.847, 0.918, 0.988),
    (1.0, 0.949, 0.737),
    (0.988, 0.847, 0.918),   # pinkish tint — the same trap on the red side
    (0.918, 0.859, 0.988),
    (0.988, 0.929, 0.847),
    (1.0, 0.878, 0.776),
    (0.839, 0.957, 0.937),
    (0.918, 0.918, 0.918),
    (0.957, 0.8, 0.8),
    (0.2, 0.247, 0.298),     # run separator band
    (0.267, 0.447, 0.298),   # header row — dark GREEN, would otherwise score
    (1.0, 1.0, 1.0),         # plain unpainted cell
])
def test_pipeline_fills_are_never_marks(colour) -> None:
    assert mark_from_background(rgb(*colour)) == ""


def test_an_unpainted_cell_reads_as_no_mark() -> None:
    assert mark_from_background(None) == ""
    assert mark_from_background({}) == ""


def test_missing_channels_default_to_zero() -> None:
    """The API omits a channel that is 0, so pure red arrives as {'red': 1}."""
    assert mark_from_background({"red": 1}) == "red"
    assert mark_from_background({"green": 1}) == "green"


# ------------------------------------------ a neighbouring swatch must still read

def test_a_darker_green_is_still_green() -> None:
    """Matching #00FF00 exactly would score this week and read zero the first
    time he picks the swatch one row down. That is the silent degradation this
    codebase keeps paying for, so the rule is saturation, not equality."""
    assert mark_from_background(rgb(0.2, 0.85, 0.2)) == "green"
    assert mark_from_background(rgb(0.8, 0.15, 0.15)) == "red"


def test_a_pastel_of_the_same_hue_is_still_not_a_mark() -> None:
    """The other half of the same rule: loosening it must not start eating the
    section tints."""
    assert mark_from_background(rgb(0.75, 0.95, 0.75)) == ""


# ----------------------------------------------------------------- the counting

def test_precision_counts_green_over_green_plus_red() -> None:
    p = precision_from_marks(["green"] * 93 + ["red"] * 128
                             + ["yellow"] + [""] * 38)
    assert (p["green"], p["red"], p["yellow"], p["unmarked"]) == (93, 128, 1, 38)
    assert p["total"] == 221                  # the week of aug 8-15
    assert p["rate"] == pytest.approx(93 / 221)


def test_yellow_is_neither_side() -> None:
    """He used it once, on «это для мониторинга» — a story that is real but
    belongs to another desk. Folding it into either side would be inventing an
    answer he declined to give."""
    p = precision_from_marks(["green", "red", "yellow"])
    assert p["total"] == 2 and p["hit"] == 1
    assert p["yellow"] == 1


def test_unmarked_rows_are_not_evidence() -> None:
    """42 rows that week carry no colour. 'Took it silently' and 'never got to
    it' look identical in the data, and counting them either way is a guess."""
    p = precision_from_marks(["green"] + [""] * 100)
    assert p["total"] == 1 and p["rate"] == 1.0
    assert p["unmarked"] == 100


def test_a_week_with_no_marks_does_not_divide_by_zero() -> None:
    p = precision_from_marks([""] * 50)
    assert p["total"] == 0 and p["rate"] == 0.0


def test_an_unknown_mark_string_is_treated_as_unmarked() -> None:
    p = precision_from_marks(["green", "blue", "лиловый"])
    assert p["total"] == 1 and p["unmarked"] == 2


def test_marks_are_not_biased_the_way_comments_are() -> None:
    """precision_from_feedback carries is_biased=True because the editor
    comments mostly on problems. A colour is on both kinds of row, so the
    caveat does not transfer — and must not be copy-pasted along."""
    assert precision_from_marks(["green", "red"])["is_biased"] is False
