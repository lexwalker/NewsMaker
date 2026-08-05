"""The guard that decides a diverted row may go back to the feed.

Yesterday's manual release skipped this check on the ARCHIVE side — it matched
the editor's archive by exact URL only — so stories he had already published
under a different headline went back into the feed and he deleted 14 of 31 by
hand. The guard exists so that cannot repeat.

It is deliberately asymmetric: a title match DECLINES a release. Title fuzz is
noise as a positive signal (measured: at threshold 70 it flags 17% of real
duplicates and 15% of wanted stories), but as a veto a false match costs only a
row we were already withholding.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import release_second_chance as rsc  # noqa: E402


class _Landed(rsc.Landed):
    """Same matcher, hand-fed instead of read from Sheets."""

    def __init__(self, urls, titles, brands=("bmw", "audi", "geely")):
        import re
        self._rx = {b: re.compile(rf"(?<![a-zа-яё0-9]){b}(?![a-zа-яё0-9])")
                    for b in brands}
        self.urls = set(urls)
        from collections import defaultdict
        self.by_brand = defaultdict(list)
        for t in titles:
            self._index(t)


def test_exact_url_blocks_release() -> None:
    from news_agent.core.published_dedup import url_key
    g = _Landed({url_key("https://a.example/x")}, [])
    assert g.has("https://a.example/x", "EN: something else entirely here")


def test_same_story_under_another_headline_blocks_release() -> None:
    # The case that burned us: same story, different outlet, different wording.
    g = _Landed(set(), ["audi predstavila flagmanskii krossover q9 v evrope"])
    assert g.has("https://other.example/1",
                 "RU: Audi представила флагманский кроссовер Q9 в Европе")


def test_different_story_same_brand_is_released() -> None:
    g = _Landed(set(), ["audi predstavila flagmanskii krossover q9 v evrope"])
    assert not g.has("https://other.example/2",
                     "RU: Audi отзывает 12 тысяч автомобилей из-за подушек")


def test_other_brand_never_blocks() -> None:
    g = _Landed(set(), ["bmw otzyvaet 29 tysyach avtomobilei v ssha"])
    assert not g.has("https://other.example/3",
                     "RU: Geely отзывает 29 тысяч автомобилей в США")


def test_short_titles_are_ignored_not_matched() -> None:
    # A stump would match everything; it must neither index nor veto.
    g = _Landed(set(), ["v geely predstavili v"])
    assert not g.has("https://other.example/4", "RU: в Geely представили в")


def test_title_lines_strips_language_prefixes() -> None:
    got = rsc._title_lines("EN: Audi unveiled the Q9 flagship SUV in Europe\n"
                           "RU: Audi представила флагманский кроссовер Q9")
    assert len(got) == 2
    assert not any(g.startswith(("en:", "ru:")) for g in got)
