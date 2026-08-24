"""Facts that identify an event — extraction and scoring.

Every case here is drawn from the real failures this replaces. Title matching
missed cross-source duplicates by construction; word overlap on bodies scored a
flat zero because a Russian and an English report of the same launch share no
vocabulary; the brand+model+type key caught 9% and false-flagged 9% because it
cannot tell two events on one car apart.

Numbers cross the language barrier and separate events on the same model, which
is the whole idea being pinned down here.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.core.event_facts import FactIndex, extract  # noqa: E402


# ---------------------------------------------- crossing the language barrier

def test_the_same_price_in_two_languages_is_one_fact() -> None:
    """The reason word overlap scored zero: these two sentences describe one
    event and share no words at all."""
    ru = extract("Leapmotor A05 дебютировал в Китае, цена от 63 900 юаней, запас хода 510 км")
    en = extract("Leapmotor A05 debuts in China with 510 km range, priced from 63,900 yuan")
    assert ru & en >= {"m:a05", "n:63900", "n:510"}


def test_a_narrow_no_break_space_separator_also_normalises() -> None:
    assert extract("63\u00a0900") == extract("63,900") == extract("63 900")


def test_leading_zeros_are_formatting_not_identity() -> None:
    assert extract("рейс 0510") & extract("рейс 510") == {"n:510"}


def test_an_all_zero_number_is_not_a_fact() -> None:
    """It strips to nothing; an empty token would match every other empty one."""
    assert not any(f == "n:" for f in extract("000 000"))


# ------------------------------------- telling two events on one car apart

def test_a_price_list_and_a_sales_start_do_not_match_on_the_model_alone() -> None:
    """The case the operator named. Same brand, same model, different events —
    and the brand+model key cannot see the difference."""
    price = extract("Changan опубликовал прайс Eado Max: от 899 000 рублей")
    sales = extract("Changan начал продажи Eado Max в России")
    idx = FactIndex.build([price, sales])
    assert idx.score(price, sales) == 0.0, "no shared figure — must not score"


def test_two_reports_of_the_same_event_do_match() -> None:
    """Corpus of realistic size: in a two-document corpus every shared fact is
    in 100% of it and rarity is zero by definition, which says nothing about
    the matcher. Production compares against ~1650 delivered rows."""
    a = extract("Changan опубликовал прайс Eado Max: от 899 000 рублей, 181 л.с.")
    b = extract("Changan Eado Max price announced: 899,000 roubles, 181 hp")
    idx = FactIndex.build([extract(f"прочая новость про {900 + i} машин") for i in range(200)]
                          + [a, b])
    assert idx.score(a, b) > 0


def test_a_figure_followed_by_a_three_digit_one_stays_two_facts() -> None:
    """Thousands-separator handling used to eat this: «899 000 рублей 181 л.с.»
    collapsed into one token 899000181 and BOTH real facts vanished."""
    f = extract("899 000 рублей 181 л.с.")
    assert f == {"n:899000", "n:181"}, f


# ----------------------------------------------------- noise must not identify

def test_years_are_not_facts() -> None:
    assert extract("модель 2026 года выйдет в 2027") == frozenset()


def test_two_digit_numbers_are_not_facts() -> None:
    """Percentages, ages and counts. Sharing «15» meant nothing, and the first
    cut of this scored a perfect 1.00 on exactly that."""
    assert extract("продажи выросли на 15%, доля 25%") == frozenset()


def test_a_common_fact_outweighs_nothing() -> None:
    """Rarity is the point: a figure in half the corpus must score far below
    one that appears twice."""
    corpus = [extract(f"мощность 100 л.с., пробег {700 + i}") for i in range(50)]
    idx = FactIndex.build(corpus)
    assert idx.weight("n:100") < idx.weight("n:701")


# ------------------------------------------------------------------ scoring

def test_the_score_is_a_sum_not_a_ratio() -> None:
    """A one-fact article must not reach a perfect match on one coincidence —
    the defect that made the first attempt read 1.00 across the board."""
    idx = FactIndex.build([extract("цена 899000 мощность 181 запас 510")
                           for _ in range(3)] + [extract("цена 899000")])
    tiny = extract("цена 899000")
    rich = extract("цена 899000 мощность 181 запас 510")
    assert idx.score(rich, rich) > idx.score(tiny, rich)


def test_an_unseen_fact_counts_as_rare() -> None:
    """Today's news carries figures nobody has written before; treating them as
    common would blind the matcher to the freshest duplicates."""
    idx = FactIndex.build([extract("мощность 100") for _ in range(20)])
    assert idx.weight("n:987654") > idx.weight("n:100")


def test_no_shared_facts_scores_zero() -> None:
    idx = FactIndex.build([extract("цена 899000"), extract("запас 510")])
    assert idx.score(extract("цена 899000"), extract("запас 510")) == 0.0


def test_an_empty_corpus_does_not_divide_by_zero() -> None:
    assert FactIndex().weight("n:510") == 0.0
    assert FactIndex().score(extract("510000"), extract("510000")) == 0.0


# ------------------------------------------------------------- best_match

def test_best_match_returns_the_closest_candidate_and_what_was_shared() -> None:
    idx = FactIndex.build([extract("899000 181 510"), extract("123456"), extract("899000")])
    q = extract("Eado Max, 899 000 рублей, 181 л.с.")
    score, key, shared = idx.best_match(
        q, [("far", extract("123456")), ("near", extract("899000 181"))])
    assert key == "near" and shared == {"n:899000", "n:181"} and score > 0


def test_best_match_on_nothing_is_not_a_match() -> None:
    idx = FactIndex.build([extract("899000")])
    assert idx.best_match(extract("899000"), [])[1] is None


def test_a_factless_article_matches_nobody() -> None:
    """61-72% of rows looked like this before the stored text was widened. The
    matcher must return no-match rather than pairing them with each other."""
    idx = FactIndex.build([extract("Компания представила новый кроссовер")])
    empty = extract("Бренд обновил модельный ряд")
    assert empty == frozenset()
    assert idx.best_match(empty, [("x", extract("899000"))])[1] is None
