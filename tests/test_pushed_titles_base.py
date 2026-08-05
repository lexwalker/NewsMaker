"""The anti-repeat base must not remember what it caused to be hidden.

recent_pushed_titles feeds the «недавно уже отправляли в фид» hint. It selected
on the CLASSIFIER's verdict, which says nothing about whether the row reached
the editor — diverting happens later, in the push. So a story diverted once was
recorded as published, the next outlet's version of it matched, got diverted,
and was recorded again. Self-reinforcing, and it ran for a month.

Measured aug-04 over 30 days: of 3048 rows accepted as «Точно новость» only
1028 (34%) actually reached the feed, and 1478 titles — half the base — carried
a dup hint of their own.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.adapters.storage import DedupStore  # noqa: E402


def _store(tmp_path, rows):
    """rows: list of (title, verdict, llm_reason)"""
    db = DedupStore(tmp_path / "t.sqlite")
    now = datetime.now(timezone.utc).isoformat()
    with db._conn() as c:
        for i, (title, verdict, reason) in enumerate(rows):
            c.execute(
                "INSERT INTO seen_articles (url_hash, canonical_url, title, "
                "first_seen_at, last_seen_at, source_domain, portal, "
                "cached_row_json) VALUES (?,?,?,?,?,?,?,?)",
                (f"h{i}", f"https://e.example/{i}", title, now, now,
                 "e.example", "RU",
                 json.dumps({"verdict": verdict, "llm_reason": reason},
                            ensure_ascii=False)))
    return db


def test_accepted_row_without_a_hint_is_remembered(tmp_path) -> None:
    db = _store(tmp_path, [("АВТОВАЗ представил Lada Azimut", "Точно новость", "")])
    assert len(db.recent_pushed_titles("RU", days=30)) == 1


def test_diverted_row_is_not_remembered(tmp_path) -> None:
    # This is the loop: it never reached the editor, so it cannot be evidence
    # that we already published the story.
    db = _store(tmp_path, [
        ("Hennessey представил Blackbird", "Точно новость",
         "(возможно дубль: недавно уже отправляли в фид о «hennessey blackbird»)"),
    ])
    assert db.recent_pushed_titles("RU", days=30) == set()


def test_diverted_row_in_clean_persist_format_is_not_remembered(tmp_path) -> None:
    # aug-05 clean-persist format: the hint lives in the dup_hint field and
    # llm_reason is clean — the exclusion must read the field, else every
    # new-format hint-carrier slips back into the base.
    db = DedupStore(tmp_path / "t2.sqlite")
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).isoformat()
    with db._conn() as c:
        c.execute(
            "INSERT INTO seen_articles (url_hash, canonical_url, title, "
            "first_seen_at, last_seen_at, source_domain, portal, "
            "cached_row_json) VALUES (?,?,?,?,?,?,?,?)",
            ("hx", "https://e.example/x", "Чистая причина, намёк в поле",
             now, now, "e.example", "RU",
             json.dumps({"verdict": "Точно новость",
                         "llm_reason": "Запуск продаж — публикуем",
                         "dup_hint": "(возможно дубль: … — проверьте)"},
                        ensure_ascii=False)))
    assert db.recent_pushed_titles("RU", days=30) == set()


def test_rejected_rows_never_counted(tmp_path) -> None:
    db = _store(tmp_path, [("Что-то не то", "Отклонено LLM", ""),
                           ("И это тоже", "Точно не новость (не авто)", "")])
    assert db.recent_pushed_titles("RU", days=30) == set()


def test_mixed_batch_keeps_only_the_undiverted(tmp_path) -> None:
    db = _store(tmp_path, [
        ("Первая новость", "Точно новость", ""),
        ("Вторая новость", "Точно новость", "(возможно дубль: …)"),
        ("Третья новость", "Точно новость", "Официальный дебют — реальное событие"),
    ])
    got = db.recent_pushed_titles("RU", days=30)
    assert len(got) == 2
    assert not any("втора" in t for t in got)


def test_window_is_respected(tmp_path) -> None:
    db = _store(tmp_path, [("Свежая новость", "Точно новость", "")])
    old = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    with db._conn() as c:
        c.execute("UPDATE seen_articles SET last_seen_at = ?", (old,))
    assert db.recent_pushed_titles("RU", days=30) == set()
