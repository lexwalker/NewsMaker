"""Explicit articles-tab handoff (package 3 — kills the I9 poisoning).

The production chain's input used to be "whatever 'ТЕСТ статьи vN' tab is
newest" with a hard-coded v18 last resort; a manual 1-source push (v24)
was consumed as production input. The resolver prefers the state.json
pointer written by the last HEALTHY batch run.
"""

import json

import pytest

from news_agent.core.tab_handoff import resolve_articles_tab


class FakeSvc:
    def __init__(self, titles):
        self._titles = titles

    def spreadsheets(self):
        return self

    def get(self, spreadsheetId):
        return self

    def execute(self):
        return {"sheets": [{"properties": {"title": t}} for t in self._titles]}


def _state(tmp_path, tab):
    p = tmp_path / "state.json"
    p.write_text(json.dumps({"articles_tab": tab}), encoding="utf-8")
    return p


def test_pointer_beats_newer_manual_tab(tmp_path) -> None:
    # The I9 case: a manual tab (higher vN) must NOT win over the pointer.
    svc = FakeSvc(["ТЕСТ статьи v27", "ТЕСТ статьи v29"])
    tab = resolve_articles_tab(
        svc, "sid", state_path=_state(tmp_path, "ТЕСТ статьи v27"))
    assert tab == "ТЕСТ статьи v27"


def test_argv_override_always_wins(tmp_path) -> None:
    svc = FakeSvc(["ТЕСТ статьи v27", "ТЕСТ статьи v29"])
    tab = resolve_articles_tab(
        svc, "sid", state_path=_state(tmp_path, "ТЕСТ статьи v27"),
        argv_tab="ТЕСТ статьи v29")
    assert tab == "ТЕСТ статьи v29"


def test_newest_vn_is_warned_fallback(tmp_path) -> None:
    svc = FakeSvc(["ТЕСТ статьи v27", "ТЕСТ статьи v29"])
    tab = resolve_articles_tab(
        svc, "sid", state_path=tmp_path / "missing.json")
    assert tab == "ТЕСТ статьи v29"


def test_stale_pointer_falls_back_to_newest(tmp_path) -> None:
    # Pointer names a deleted tab → warned fallback, not a crash.
    svc = FakeSvc(["ТЕСТ статьи v29"])
    tab = resolve_articles_tab(
        svc, "sid", state_path=_state(tmp_path, "ТЕСТ статьи v9999"))
    assert tab == "ТЕСТ статьи v29"


def test_fails_loud_when_nothing_found(tmp_path) -> None:
    # No pointer, no vN tabs → RuntimeError, NOT a silent 'ТЕСТ статьи v18'.
    svc = FakeSvc(["Новости (новые)", "Опубликованные (все)"])
    with pytest.raises(RuntimeError):
        resolve_articles_tab(svc, "sid", state_path=tmp_path / "missing.json")
