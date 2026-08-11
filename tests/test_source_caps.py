"""Per-source link caps — how many articles we take off one index.

aug-04: 127 sources finished a full run pinned exactly at their cap, i.e. we
walked away from indexes that still had fresh stories on them. Of the editor's
publications that week, 88 (28%) sat on a domain we polled and did not take,
and this was the largest single reason. Caps were raised only where the run
report showed saturated AND fast AND productive; these tests pin the ones that
were raised and, just as importantly, the ones that were not.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from batch_fetch_test import _items_cap_for  # noqa: E402


def test_the_best_yielding_sources_are_not_throttled() -> None:
    """aug-11 re-measure off the v88 report: these three convert a quarter to
    a half of every link they are allowed into an auto-relevant row, and each
    costs under a second a link. Throttling them was the most expensive thing
    the cap list did."""
    assert _items_cap_for("https://110km.ru") == 150          # 23 auto of 45
    assert _items_cap_for("https://www.dp.ru/a/2026/08/04/x") == 120  # 10 of 30
    assert _items_cap_for("https://3dnews.ru/news") == 60     # 4 of 15


def test_the_largest_single_cut_was_addressed() -> None:
    """kommersant left 385 links on the table in one run — more than any other
    source, at a 16% yield."""
    assert _items_cap_for("https://www.kommersant.ru/RSS/news.xml") == 200
    assert _items_cap_for("https://kommersant.ru/doc/1") == 200


def test_general_portals_scale_with_what_they_yield() -> None:
    """Their extra links are mostly non-auto, but the heuristic drops those
    before any LLM call — the cost is fetch seconds, not tokens."""
    assert _items_cap_for("https://lenta.ru") == 100          # 26% yield
    assert _items_cap_for("https://ria.ru/lenta/") == 100     # 23%
    assert _items_cap_for("https://www.vedomosti.ru") == 150  # 7%, 0.4s a link
    assert _items_cap_for("https://rg.ru/tema/ekonomika") == 70


def test_a_source_that_yields_nothing_is_not_raised() -> None:
    """77% of the 6283 cut links belong to 97 sources that produced ZERO auto
    rows from what we already took — globalsuzuki offers 1050 links and yields
    nothing. Those are navigation and archive furniture; a bigger cap there
    buys fetch seconds and no news."""
    assert _items_cap_for("https://www.globalsuzuki.com/globalnews/") == 15
    assert _items_cap_for("https://www.ecb.europa.eu/press/") == 15
    assert _items_cap_for("https://www.stellantis.com/en/news") == 15


def test_overrides_match_subdomains_and_paths() -> None:
    assert _items_cap_for("https://www.popmech.ru/") == 30
    assert _items_cap_for("https://avtonovostidnya.ru/") == 45


def test_time_bound_sources_are_deliberately_left_alone() -> None:
    """iz.ru and autostat sit at 184-186s against the 180s source budget, at
    3.1s a link. They are time-bound, not cap-bound: a bigger number changes
    nothing until fetching goes parallel. Every source raised above costs
    0.2-0.5s a link and has 200+ links of headroom inside its budget."""
    assert _items_cap_for("https://iz.ru") == 60
    assert _items_cap_for("https://iz.ru/rubric/auto") == 60
    assert _items_cap_for("https://www.autostat.ru/news/") == 60


def test_untouched_sources_keep_their_tier() -> None:
    assert _items_cap_for("https://tass.ru/rss/v2.xml") == 100     # explicit
    assert _items_cap_for("https://carscoops.com") == 60           # deep index
    assert _items_cap_for("https://example-unknown-site.com") == 15  # default
