"""Colour answers on the razmetka tab (aug-29).

The editor moved to colour marks in the feed on aug-15 and the razmetka
tab went silent — its last text answer is dated aug-05, which starved the
rule scoreboard and froze the dup eval dataset. The same colour gesture
now answers the tab: green cell in D = «да» (нужна), red = «нет». Explicit
text stays the override; yellow and unpainted are not answers.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingest_rejected_labels import resolve_answer  # noqa: E402


def test_green_means_yes() -> None:
    assert resolve_answer("", "green") == "да"


def test_red_means_no() -> None:
    assert resolve_answer("", "red") == "нет"


def test_text_beats_colour() -> None:
    # An explicit word is the editor correcting the colour, not vice versa.
    assert resolve_answer("нет", "green") == "нет"
    assert resolve_answer("да", "red") == "да"


def test_yellow_and_unpainted_are_no_answer() -> None:
    assert resolve_answer("", "yellow") == ""
    assert resolve_answer("", "") == ""


def test_whitespace_text_is_no_text() -> None:
    assert resolve_answer("   ", "red") == "нет"
