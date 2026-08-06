"""Judging a run's fetch failures against its OWN history.

On aug-06 a full run lost 135 of its 345 sources — 39.1% — and reported
status OK. The threshold was an absolute 40%, so it squeaked under; and it was
a warning rather than an alarm, so even crossing it would not have held the run
window. Those 135 sources had produced 77 auto-relevant articles the previous
day, a fifth of a day's harvest, and the window advanced over them. Nothing will
ever ask for that period again.

An absolute threshold cannot win here. Set it low and every ordinary night cries
wolf; set it high and a 2.6x jump hides underneath it. The run's own recent
history is the only scale that makes 39% obviously wrong — the three healthy
runs before it sat at 15-18%.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def bft():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import batch_fetch_test
    return batch_fetch_test


def _check(bft, *, errored: int, total: int, baseline=None):
    """Run the health check over a synthetic source list, using the REAL
    SourceResult so the stub cannot drift from the fields the check reads."""
    results = [
        bft.SourceResult(
            url=f"https://s{i}.example/",
            error="boom" if i < errored else "",
            articles_attempted=0 if i < errored else 3,
            links_precap=0 if i < errored else 3,
        )
        for i in range(total)
    ]
    prev = {"archive_urls_count": 6884, "archive_titles_count": 1689}
    if baseline is not None:
        prev["err_share"] = baseline
    return bft._health_check(
        results, [],
        llm_ran=False, llm_aborted="", llm_candidates=0,
        dedup_enabled=False, prev_state=prev,
    )


# ------------------------------------------------------------- the real case

def test_the_aug06_run_would_now_be_caught(bft) -> None:
    """135/345 = 39.1% against a 15% baseline: 2.6x, and it slid under the old
    absolute 40% by nine tenths of a percentage point."""
    alarms = _check(bft, errored=135, total=345, baseline=0.15)
    assert any("sources errored" in a for a in alarms), alarms


def test_a_normal_night_stays_quiet(bft) -> None:
    """The healthy runs of aug-05: 52, 62 and 63 errors of 345."""
    for errored in (52, 62, 63):
        assert not [a for a in _check(bft, errored=errored, total=345,
                                      baseline=0.15) if "sources errored" in a]


# ------------------------------------------------ both halves of the rule

def test_a_doubling_that_is_still_small_does_not_alarm(bft) -> None:
    """5% -> 11% is a doubling and means nothing. Without the absolute half of
    the rule, a lucky baseline would make the next ordinary run scream."""
    assert not [a for a in _check(bft, errored=38, total=345, baseline=0.05)
                if "sources errored" in a]


def test_a_high_but_normal_rate_does_not_alarm(bft) -> None:
    """A portal that always errors at 20% must not alarm at 30%. Without the
    relative half, the absolute floor would fire every single night."""
    assert not [a for a in _check(bft, errored=104, total=345, baseline=0.20)
                if "sources errored" in a]


def test_both_halves_together_alarm(bft) -> None:
    alarms = _check(bft, errored=140, total=345, baseline=0.18)
    assert any("Mass fetch failure" in a or "the last healthy run" in a
               for a in alarms), alarms


# ---------------------------------------------------------- no history yet

def test_without_a_baseline_the_old_constant_still_guards(bft) -> None:
    """First run, lost state file, or a baseline of zero — fall back to the
    absolute threshold that has always been here, but as an ALARM now."""
    alarms = _check(bft, errored=200, total=345, baseline=None)
    assert any("no usable baseline" in a for a in alarms), alarms


def test_without_a_baseline_a_normal_rate_is_still_fine(bft) -> None:
    assert not [a for a in _check(bft, errored=52, total=345, baseline=None)
                if "sources errored" in a]


def test_a_zero_baseline_is_not_used_as_a_divisor(bft) -> None:
    """A stored 0.0 would make every run infinitely worse than baseline."""
    alarms = _check(bft, errored=52, total=345, baseline=0.0)
    assert not [a for a in alarms if "sources errored" in a]


# --------------------------------------------------------------- the middle

def test_a_rise_worth_seeing_but_not_stopping_is_a_warning(bft) -> None:
    """26% on a 20% baseline: worth printing, not worth holding the window."""
    alarms = _check(bft, errored=92, total=345, baseline=0.20)
    assert not [a for a in alarms if "sources errored" in a]


def test_an_empty_source_list_is_not_a_division_by_zero(bft) -> None:
    bft._health_check([], [], llm_ran=False, llm_aborted="", llm_candidates=0,
                      dedup_enabled=False, prev_state={})
