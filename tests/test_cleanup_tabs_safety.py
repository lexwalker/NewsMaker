"""The name test that stands between a scheduled task and the editor's archive.

From aug-07 the tab cleanup runs unattended at the end of every chain, so the
question "could this ever delete something that matters" stops being academic.
The workbook holds the editor's own sheets — «Новости (новые)», «Опубликованные
(все)», «Разметка отклонённого (ИИ)», «Источники (для РУ)» — alongside hundreds
of the bot's disposable per-run tabs, and only a name tells them apart.

These tests pin the name test itself. They deliberately do not touch Google: the
question is not whether the API call works, it is whether a title that must
never be deleted can match.
"""

from __future__ import annotations

import re

import pytest

# The literal pattern the script re-derives as its last gate before deleting.
SAFE = re.compile(r"^ТЕСТ (прогон|статьи)( \(гор\))? v\d+$")


@pytest.mark.parametrize("title", [
    # Every non-ТЕСТ sheet present in the live workbook on aug-07.
    "Новости (новые)", "Опубликованные (все)", "Разметка отклонённого (ИИ)",
    "Источники (для РУ)", "Новости", "Новости опубликованные", "Новости old",
    "Опубликованные 2", "Опубликованные 3", "4", "Уточнения",
    "Разделы новостей", "Анализ опубликованных новостей",
    "Непокрытые (анализ)", "Задача",
    # The operator's own hand-made experiment sheets.
    "Тест 1", "Тест 4 (с Haiku LLM)",
    "Тест 6 (все источники + анализ первоисточника)",
    "Тест 7 (правка источников, больше статей)",
])
def test_a_sheet_that_matters_can_never_match(title) -> None:
    assert not SAFE.match(title), f"«{title}» попал бы под удаление"


@pytest.mark.parametrize("title", [
    "ТЕСТ статьи v1", "ТЕСТ статьи v82", "ТЕСТ прогон v81",
    "ТЕСТ статьи (гор) v86", "ТЕСТ прогон (гор) v85",
])
def test_a_disposable_run_tab_matches(title) -> None:
    assert SAFE.match(title)


@pytest.mark.parametrize("title", [
    "ТЕСТ статьи",             # no version at all
    "ТЕСТ статьи v",           # empty version
    "ТЕСТ статьи v1 копия",    # a hand-made copy of a run tab
    "ТЕСТ статьи v1a",
    " ТЕСТ статьи v1",         # leading space
    "ТЕСТ статьи v1 ",         # trailing space
    "тест статьи v1",          # lowercase
    "ТЕСТ статьи (гор2) v1",
    "ТЕСТ новости v1",
])
def test_anything_ambiguous_is_left_alone(title) -> None:
    """When a title is nearly-but-not-quite a run tab, the only safe reading is
    that a human made it. Skipping a deletable tab costs cells; deleting an
    unexpected one costs data."""
    assert not SAFE.match(title)


def test_the_script_and_this_test_use_the_same_pattern() -> None:
    """A copy of a regex in a test is worth nothing if the script's own copy
    drifts. This pins them together."""
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1]
           / "scripts" / "cleanup_old_tabs.py").read_text(encoding="utf-8")
    assert SAFE.pattern in src, "шаблон в скрипте разошёлся с тестом"
