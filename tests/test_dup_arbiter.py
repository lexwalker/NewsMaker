"""Dup-arbiter retrieval + the jul-23 published_dup_hint repairs.

Anchored on the real editor case («нет, писали уже давно»): fresh
«Седан Geely отобрал у Chery мировой рекорд по дальности хода»
(event geely|galaxy a7 em|other) vs archive
«geely galaxy a7 sedan traveled 2 608 km on a single tank of fuel».
No fuzz threshold separates that pair from a genuinely NEW A7 story —
retrieval must surface it as an LLM candidate; and the deterministic
branch-B model anchor must stop being broken by the «em» suffix.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.core.dedup import published_dup_hint  # noqa: E402
from news_agent.core.dup_arbiter import (  # noqa: E402
    archive_candidates,
    build_fresh_display,
    graykey_candidates,
)

_GEELY_TWIN = "geely galaxy a7 sedan traveled 2 608 km on a single tank of fuel"
_ARCHIVE = {
    _GEELY_TWIN,
    "geely certified the electric galaxy a7 sedan in china",
    "sales of the geely galaxy a7 sedan started in china",
    "geely produced 2 millionth galaxy car",
    "interior of the geely galaxy starshine 7 sedan leaked online",
    "toyota unveiled the new camry hybrid",
}


def test_archive_candidates_surface_geely_twin() -> None:
    cands = archive_candidates(
        title="Седан Geely отобрал у Chery мировой рекорд по дальности хода",
        event_brand="geely",
        event_model="galaxy a7 em",
        pub_titles=_ARCHIVE,
    )
    assert _GEELY_TWIN in cands
    # brand-gated: the Toyota title never qualifies
    assert all("toyota" not in c for c in cands)


def test_archive_candidates_empty_without_brand() -> None:
    assert archive_candidates(
        title="Некая новость", event_brand="", event_model="x",
        pub_titles=_ARCHIVE) == []


def test_graykey_surfaces_empty_model_same_type() -> None:
    # Honda–GAC class: fresh honda||partnership vs stored honda||partnership.
    recent = {
        "honda||partnership": ("2026-07-20T08:00:00+00:00",
                               "https://cnevpost.com/x",
                               "Honda, GAC extend joint venture to 2038"),
    }
    cands = graykey_candidates(
        event_brand="honda", event_model="", event_type="partnership",
        recent=recent)
    assert cands == ["Honda, GAC extend joint venture to 2038"]


def test_graykey_excludes_deterministic_pairs() -> None:
    # Exact / token-subset same-type pairs are the deterministic layers' job.
    recent = {
        "nissan|armada|recall": ("2026-07-21T08:00:00+00:00",
                                 "https://a.example/1", "Nissan recall"),
    }
    assert graykey_candidates(
        event_brand="nissan", event_model="armada qx56 qx80",
        event_type="recall", recent=recent) == []


def test_graykey_same_model_different_type_not_gray() -> None:
    # Removed gray class (jul-23 eval: only false flags, zero real dups) —
    # different lifecycle stages of one model are different news.
    recent = {
        "geely|galaxy a7|launch": ("2026-07-01T08:00:00+00:00",
                                   "https://a.example/2",
                                   "Sales of the Galaxy A7 started"),
    }
    assert graykey_candidates(
        event_brand="geely", event_model="galaxy a7", event_type="reveal",
        recent=recent) == []


def test_fresh_display_carries_both_titles_and_signature() -> None:
    d = build_fresh_display(
        title="Седан Geely отобрал рекорд", alt_title="Geely sedan takes record",
        event_brand="geely", event_model="galaxy a7 em", event_type="other")
    assert "Седан Geely" in d and "takes record" in d
    assert "[event: geely|galaxy a7 em|other]" in d


# --- published_dup_hint repairs -------------------------------------------


def test_model_anchor_survives_extraction_suffix() -> None:
    # «galaxy a7 em» is a substring of nothing, but the digit token 'a7'
    # anchors; with a moderately similar headline branch B now fires.
    pub = {"geely galaxy a7 sedan sets a range record of 2608 km"}
    hint = published_dup_hint(
        "Седан Geely Galaxy A7 установил рекорд дальности 2608 км",
        "geely", "galaxy a7 em", pub)
    assert hint is not None and "возможно дубль" in hint


def test_family_word_alone_does_not_anchor() -> None:
    # em='galaxy starshine' vs a title that only says 'galaxy' (different
    # family member): no digit token, not all word-tokens present → no
    # anchor → the strong 88 full-title bar applies and stays unmet.
    pub = {"geely produced 2 millionth galaxy car"}
    hint = published_dup_hint(
        "Geely Galaxy Starshine получил новую версию",
        "geely", "galaxy starshine", pub)
    assert hint is None


def test_alt_title_en_lifts_cross_language_match() -> None:
    # RU original scores ~50 vs the EN archive twin; the EN translation of
    # the same story clears the strong branch-A bar.
    pub = {"nissan recalls 168 thousand vehicles over incorrect labels"}
    assert published_dup_hint(
        "Nissan отзовет 168 тыс. автомобилей из-за неправильных наклеек",
        "nissan", "", pub) is None
    hint = published_dup_hint(
        "Nissan отзовет 168 тыс. автомобилей из-за неправильных наклеек",
        "nissan", "", pub,
        alt_title="Nissan recalls 168 thousand vehicles due to incorrect labels")
    assert hint is not None and "возможно дубль" in hint
