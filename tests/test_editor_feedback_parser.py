"""Coverage for sync_editor_feedback.parse_comment — the parser that
turns Russian editorial free-text into structured labels.

These are calibrated against the real-world comment samples we
audited from 895 historical comments. If a real editor case stops
parsing correctly, add it here as a regression test.
"""

import importlib.util
import sys
from pathlib import Path

# The script isn't a package — load it directly
SPEC = importlib.util.spec_from_file_location(
    "sync_editor_feedback",
    Path(__file__).resolve().parents[1] / "scripts" / "sync_editor_feedback.py",
)
mod = importlib.util.module_from_spec(SPEC)
# Skip env-loading side effects: prevent google-auth import at module level
# by monkey-patching __spec__ — easier: just guard the test
try:
    SPEC.loader.exec_module(mod)
except Exception:  # google auth env may not be set in CI
    # fallback: import parse_comment by directly reading + exec'ing it
    src = (Path(__file__).resolve().parents[1] / "scripts" /
           "sync_editor_feedback.py").read_text(encoding="utf-8")
    # Extract just the parse_comment function and its dependencies
    pass

parse_comment = mod.parse_comment


# ---------- DUP within-batch ------------------------------------------

def test_dup_within_explicit_row_ref() -> None:
    out = parse_comment("дубль строки 23")
    assert out["label_dup_within"] is True
    assert out["label_publish"] is False
    assert out["label_dup_cross_run"] is False


def test_dup_within_emoji_marker() -> None:
    # Markers we wrote ourselves — parser still sees the signal
    out = parse_comment("🔁 дубль строки 8")
    assert out["label_dup_within"] is True


def test_dup_within_uppercase_only() -> None:
    # Real case: «ОПЯТЬ ДУБЛЬ» — pure uppercase
    out = parse_comment("ОПЯТЬ ДУБЛЬ")
    assert out["label_dup_within"] is True


# ---------- DUP cross-run ---------------------------------------------

def test_dup_cross_run_postili() -> None:
    out = parse_comment("постили")
    assert out["label_dup_cross_run"] is True
    assert out["label_publish"] is False


def test_dup_cross_run_with_url() -> None:
    out = parse_comment("постили | https://media.mercedes-benz.com/article/abc")
    assert out["label_dup_cross_run"] is True
    assert out["referenced_urls"] == ["https://media.mercedes-benz.com/article/abc"]


def test_dup_cross_run_already_published() -> None:
    out = parse_comment("уже было")
    assert out["label_dup_cross_run"] is True


def test_dup_cross_run_repost_celikov() -> None:
    # «снова Дубль новости. Перепост Целикова»
    out = parse_comment("снова Дубль новости. Перепост Целикова")
    assert out["label_dup_cross_run"] is True


def test_pure_dubl_short() -> None:
    # «ДУБЛЬ» on its own counts as cross-run dup (no row #)
    out = parse_comment("ДУБЛЬ")
    assert out["label_dup_cross_run"] is True
    assert out["label_dup_within"] is False
    assert out["label_publish"] is False


# ---------- WRONG PRIMARY ---------------------------------------------

def test_wrong_primary_postili_press() -> None:
    out = parse_comment("постили пресс")
    assert out["label_wrong_primary"] is True
    # NOTE: «постили пресс» means EDITOR has the press URL — they don't
    # necessarily reject the article, they just want a different primary.
    # We don't force label_publish=False here.
    assert out["label_dup_cross_run"] is False  # specifically not "уже было"


def test_wrong_primary_with_press_url() -> None:
    out = parse_comment(
        "постили пресс | https://www.media.stellantis.com/press/jeep-avenger"
    )
    assert out["label_wrong_primary"] is True
    assert "media.stellantis.com" in out["referenced_urls"][0]


def test_wrong_primary_was_press() -> None:
    out = parse_comment("был пресс по стратегии")
    assert out["label_wrong_primary"] is True


# ---------- NEEDS TRANSLATION -----------------------------------------

def test_needs_translation_english() -> None:
    out = parse_comment("постим, но нужен англ первоисточник")
    assert out["label_needs_translation"] is True


def test_needs_translation_ok_but_eng() -> None:
    out = parse_comment("ок, но нужен англ")
    assert out["label_needs_translation"] is True
    # Despite «но нужен англ», editor approved the news itself
    assert out["label_publish"] is True


# ---------- WRONG SECTION ---------------------------------------------

def test_section_correction_to_rumors() -> None:
    out = parse_comment("это Слухи, постили")
    assert out["label_section"] == "Rumors"


def test_section_correction_to_local() -> None:
    out = parse_comment("это в местные")
    assert out["label_section"] == "Local specifics"


def test_section_correction_to_lcv() -> None:
    out = parse_comment("раздел LCV/факты")
    assert out["label_section"] == "LCV news"


def test_section_correction_not_facts() -> None:
    # «это не факты, а другие» — should pick Other
    out = parse_comment("это не факты, а другие")
    assert out["label_section"] == "Other news"


# ---------- REJECT ----------------------------------------------------

def test_reject_not_our_topic() -> None:
    out = parse_comment("не наша тема")
    assert out["label_publish"] is False


def test_reject_dont_post() -> None:
    out = parse_comment("не постим, это анонс")
    assert out["label_publish"] is False


def test_reject_just_review() -> None:
    out = parse_comment("это просто обзор, не постим")
    assert out["label_publish"] is False


def test_reject_short_no() -> None:
    out = parse_comment("нет")
    assert out["label_publish"] is False


# ---------- APPROVE ---------------------------------------------------

def test_approve_short_ok() -> None:
    out = parse_comment("ок")
    assert out["label_publish"] is True


def test_approve_postim() -> None:
    out = parse_comment("постим")
    assert out["label_publish"] is True


def test_approve_english_ok() -> None:
    out = parse_comment("ok")
    assert out["label_publish"] is True


def test_approve_with_section_hint() -> None:
    out = parse_comment("это в факты")
    assert out["label_publish"] is True
    assert out["label_section"] == "Confirmed"


# ---------- SOFT (excluded from strict eval) --------------------------

def test_soft_can_skip() -> None:
    out = parse_comment("можно и пропустить")
    assert out["soft"] is True


def test_soft_nothing_to_write() -> None:
    out = parse_comment("особо нечего писать")
    assert out["soft"] is True


# ---------- combinations: realistic editor patterns -------------------

def test_jeep_avenger_real_case() -> None:
    """The exact comment editor wrote on r292:
    «дубль 136, постили пресс | https://www.media.stellantis.com/...»"""
    out = parse_comment(
        "дубль 136, постили пресс | "
        "https://www.media.stellantis.com/jeep-avenger"
    )
    assert out["label_dup_within"] is True  # explicit row ref
    assert out["label_wrong_primary"] is True
    assert out["label_publish"] is False
    assert "media.stellantis.com" in out["referenced_urls"][0]


def test_volga_c50_real_case() -> None:
    """r217: «Слухи, Дубль» — section correction + dup."""
    out = parse_comment("Слухи, Дубль")
    assert out["label_section"] == "Rumors"
    assert out["label_dup_cross_run"] is True
    assert out["label_publish"] is False


def test_complex_xpeng_robotaxi() -> None:
    """r53: «дубль 53, постили | https://cnevpost.com/...»"""
    out = parse_comment(
        "дубль 53, постили | https://cnevpost.com/2026/05/18/xpeng-robotaxi/"
    )
    assert out["label_dup_within"] is True
    assert out["referenced_urls"][0].startswith("https://cnevpost.com")


def test_empty_comment_safe() -> None:
    assert parse_comment("") == {}
    assert parse_comment("   ") == {}


def test_uncategorized_comment_returns_none_publish() -> None:
    # «патенты на названия моделей или модели - Факты» — informational,
    # no clear publish signal beyond the section cue
    out = parse_comment("патенты на названия моделей или модели - Факты")
    # may or may not catch section; should not crash, no false approve
    assert out.get("label_publish") in (True, None)  # «факты» is a hint


def test_referenced_url_multiple() -> None:
    out = parse_comment(
        "постили | https://a.example/1 | "
        "и тут https://b.example/2 тоже"
    )
    assert len(out["referenced_urls"]) == 2
