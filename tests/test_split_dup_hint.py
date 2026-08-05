"""split_dup_hint — unfreezing dup hints from cached reasons (aug-05).

Every format the pipeline actually injects must split losslessly; a reason
the MODEL chose to start with «возможно дубль…» (no marker of ours) must
survive untouched — stripping it would silently un-divert a model-suspected
dup and eat the model's own words.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.core.editorial_pass import split_dup_hint  # noqa: E402


def test_event_key_hint_splits():
    hint, clean = split_dup_hint(
        "(возможно дубль: «evolute i-sky (launch)» уже было сегодня — "
        "проверьте) | Запуск продаж конкретной модели")
    assert "уже было" in hint and hint.startswith("(возможно дубль")
    assert clean == "Запуск продаж конкретной модели"


def test_arbiter_hint_splits():
    hint, clean = split_dup_hint(
        "(возможно дубль, ИИ-арбитр: похоже на «genesis g80 recall» — "
        "проверьте) | Отзыв пассажирских авто")
    assert "ИИ-арбитр" in hint
    assert clean == "Отзыв пассажирских авто"


def test_tier05_hint_splits():
    hint, clean = split_dup_hint(
        "(возможно дубль: недавно уже отправляли в фид о «tesla model 3» — "
        "проверьте) | NHTSA расследование")
    assert "отправляли в фид" in hint
    assert clean == "NHTSA расследование"


def test_archive_strong_hint_splits():
    hint, clean = split_dup_hint(
        "(возможно дубль: похожий заголовок уже публиковали — проверьте) | "
        "Шпионские снимки прототипа")
    assert "уже публиковали" in hint
    assert clean == "Шпионские снимки прототипа"


def test_cluster_stage_hint_splits_defensively():
    # Injected by build_news_clusters, never persisted to SQLite — but the
    # splitter must not choke on it if that ever changes.
    hint, clean = split_dup_hint(
        "возможно дубль: уже публиковалось — https://example.com/a | "
        "Юбилейный выпуск завода")
    assert "уже публиковалось" in hint
    assert clean == "Юбилейный выпуск завода"


def test_hint_only_reason_leaves_empty_clean():
    hint, clean = split_dup_hint(
        "(возможно дубль: «hongqi tiangong 08 (launch)» уже было ~26 дн. "
        "назад — проверьте)")
    assert "уже было" in hint
    assert clean == ""


def test_plain_reason_untouched():
    assert split_dup_hint("Официальный анонс модели — публикуем") == \
        ("", "Официальный анонс модели — публикуем")


def test_model_authored_lookalike_is_preserved():
    # Starts with «возможно дубль» but carries NONE of our hint markers —
    # this is the model's own wording, not our injection.
    reason = "возможно дубль: та же новость с другого сайта | но угол иной"
    assert split_dup_hint(reason) == ("", reason)


def test_empty_and_none():
    assert split_dup_hint("") == ("", "")
    assert split_dup_hint(None) == ("", "")


def test_idempotent_on_clean_text():
    clean = split_dup_hint(
        "(возможно дубль: «x» уже было сегодня — проверьте) | Анонс")[1]
    assert split_dup_hint(clean) == ("", clean)
