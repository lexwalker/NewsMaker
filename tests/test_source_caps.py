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


def test_pure_auto_sites_were_unthrottled() -> None:
    # 110km gave 10.7 auto stories out of 15 links in 12 seconds.
    assert _items_cap_for("https://110km.ru") == 45
    assert _items_cap_for("https://www.32cars.ru/posts/id-1") == 45
    assert _items_cap_for("https://avtonovostidnya.ru/") == 45


def test_general_portals_raised_to_seventy() -> None:
    # Their extra links are mostly non-auto, but the heuristic drops those
    # before any LLM call — the cost is fetch seconds, not tokens.
    for u in ("https://lenta.ru", "https://ria.ru/lenta/",
              "https://www.kommersant.ru/RSS/news.xml",
              "https://www.vedomosti.ru", "https://rg.ru/tema/ekonomika"):
        assert _items_cap_for(u) == 70, u


def test_overrides_match_subdomains_and_paths() -> None:
    assert _items_cap_for("https://www.dp.ru/a/2026/08/04/x") == 30
    assert _items_cap_for("https://www.popmech.ru/") == 30


def test_slow_source_was_deliberately_left_alone() -> None:
    # iz.ru is saturated too, but spends 93s fetching 17 of its 60 links: a
    # bigger cap buys nothing there, it needs budget or concurrency.
    assert _items_cap_for("https://iz.ru") == 60
    assert _items_cap_for("https://iz.ru/rubric/auto") == 60


def test_untouched_sources_keep_their_tier() -> None:
    assert _items_cap_for("https://tass.ru/rss/v2.xml") == 100     # explicit
    assert _items_cap_for("https://carscoops.com") == 60           # deep index
    assert _items_cap_for("https://example-unknown-site.com") == 15  # default
