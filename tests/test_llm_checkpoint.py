"""Freezing the editorial pass as it goes.

The editorial pass is the only expensive stage — ~$3 and ~25 minutes of a full
run — and until aug-06 it was the only stage with no checkpoint. Verdicts lived
in memory until the pass ended, so a power cut or a kill threw away everything
already judged, while the FREE fetch stage had had a resume file since jun-24.
Graceful failures were always safe (main persists on the way out); a hard death
was not.

Two things have to hold, and both are here. The frozen slices must be
contiguous and non-overlapping — a gap leaves paid verdicts unprotected, an
overlap rewrites rows for nothing. And the mid-pass freeze must obey EXACTLY
the same rules as the end-of-run write: if the checkpoint froze a row the final
write refuses, the difference would only become visible after a crash, which is
the worst possible moment to discover it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def bft():
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "scripts"))
    import batch_fetch_test
    return batch_fetch_test


class _Row:
    """Just enough ArticleRow for the cache-entry builder."""

    def __init__(self, **kw) -> None:
        self.article_url = kw.get("url", "https://example.com/a")
        self.verdict = kw.get("verdict", "Точно новость")
        self.from_cache = kw.get("from_cache", False)
        self.llm_relevance = kw.get("llm_relevance", "Да")
        self.title = kw.get("title", "Заголовок")
        self.body_excerpt = kw.get("body", "тело статьи")
        self.published_at = None
        self.is_article = True
        self.article_score = 5
        self.article_reasons = ""
        self.auto_topic = ""
        self.auto_hits = ""
        self.llm_section = "Confirmed"
        self.llm_region = "Global"
        self.llm_confidence = 0.8
        self.llm_title_en = "Headline"
        self.llm_title_ru = "Заголовок"
        self.llm_note = kw.get("note", "")
        self.llm_reason = kw.get("reason", "обычное обоснование")
        # aug-06 attribution fields — mirror ArticleRow's defaults.
        self.llm_rule = kw.get("rule", "")
        self.llm_disputed = kw.get("disputed", False)
        self.primary_url = ""
        self.primary_domain = ""
        self.primary_confidence = ""
        self.primary_method = ""
        self.launch_brand_model = ""
        self.event_brand = "bmw"
        self.event_model = "x5"
        self.event_type = "launch"


# ----------------------------------------------------------- what is frozen

def test_a_judged_row_is_frozen_whole(bft) -> None:
    e = bft._cache_entry_for(_Row())
    assert e is not None and len(e) == 8
    import json
    cached = json.loads(e[6])
    assert cached["llm_relevance"] == "Да"
    assert cached["event_brand"] == "bmw"          # dedup key survives
    assert cached["cls_ver"] == bft.CLASSIFIER_VERSION
    assert e[7] == "тело статьи"                   # the lede, 8th element


def test_a_rejection_is_frozen_too(bft) -> None:
    """A rejection is a paid verdict like any other — re-judging it next run
    would be paying twice for the same answer."""
    assert bft._cache_entry_for(
        _Row(verdict="Отклонено LLM", llm_relevance="Нет")) is not None


@pytest.mark.parametrize("verdict", sorted(
    {"Отклонить (ошибка загрузки)", "Отклонить (не удалось извлечь)",
     "Отклонить (уже опубликовано редактором)",
     "Отклонить (дубль финального URL)"}))
def test_verdicts_that_must_never_be_frozen(bft, verdict) -> None:
    """Fetch errors must be retried; the archive verdict must be re-checked
    against a FRESH archive; and «дубль финального URL» shares a url_hash with
    the real row it would overwrite (810 such rows in one week — the Maybach
    GLS story was pushed and published while its cache row read «дубль»)."""
    assert bft._cache_entry_for(_Row(verdict=verdict)) is None


def test_an_unjudged_candidate_is_not_a_final_state(bft) -> None:
    """I2 froze 1000+ accept-graded rows the LLM never classified. Harmless to
    correctness, poisonous to forensics — let the next run redo them."""
    assert bft._cache_entry_for(
        _Row(verdict="Возможно новость", llm_relevance="")) is None
    # …but a row RESTORED from cache legitimately has no fresh verdict.
    assert bft._cache_entry_for(
        _Row(verdict="Возможно новость", llm_relevance="", from_cache=True)) is not None


def test_a_row_without_a_url_has_nothing_to_key_on(bft) -> None:
    assert bft._cache_entry_for(_Row(url="")) is None


def test_the_cache_marker_never_lands_in_the_stored_note(bft) -> None:
    import json
    e = bft._cache_entry_for(_Row(note="из кэша | что-то ещё"))
    assert json.loads(e[6])["llm_note"] == "что-то ещё"


def test_the_dup_hint_is_stored_apart_from_the_reason(bft) -> None:
    """A hint frozen into the reason replays on every restore and the push
    diverts the row forever, immune to later rule changes (aug-05)."""
    import json
    e = bft._cache_entry_for(
        _Row(reason="(возможно дубль, ИИ-арбитр: похоже на «X» — проверьте) | суть"))
    cached = json.loads(e[6])
    assert "возможно дубль" not in cached["llm_reason"]
    assert "возможно дубль" in cached["dup_hint"]


# ------------------------------------------------------------- the cadence

def test_frozen_slices_are_contiguous_and_never_overlap(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "LLM_CHECKPOINT_EVERY", 25)
    checkpointed, cuts = 0, []
    for i in range(1, 101):                 # the loop's own 1-based counter
        if bft._checkpoint_due(i - 1, checkpointed):
            cuts.append((checkpointed, i - 1))
            checkpointed = i - 1
    assert cuts == [(0, 25), (25, 50), (50, 75)]
    assert all(a[1] == b[0] for a, b in zip(cuts, cuts[1:]))   # no gap, no overlap
    # Rows 76..100 are still in flight when the loop ends: the freeze happens at
    # the TOP of an iteration, so the last group has no iteration left to
    # trigger it. They ride to the end-of-run write, which is not a leak — it is
    # the same tail the checkpoint exists to bound.
    assert cuts[-1][1] == 75


def test_the_tail_is_left_to_the_end_of_run_write(bft, monkeypatch) -> None:
    """Rows since the last freeze are not lost — main persists everything on
    the way out. The checkpoint only bounds what a HARD death can cost."""
    monkeypatch.setattr(bft, "LLM_CHECKPOINT_EVERY", 25)
    checkpointed = 0
    for i in range(1, 31):
        if bft._checkpoint_due(i - 1, checkpointed):
            checkpointed = i - 1
    assert checkpointed == 25          # rows 26..30 ride to the final write


def test_checkpointing_can_be_switched_off(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "LLM_CHECKPOINT_EVERY", 0)
    assert not any(bft._checkpoint_due(i, 0) for i in range(500))


def test_persist_rows_filters_before_touching_the_store(bft, monkeypatch) -> None:
    seen = []

    class _Store:
        def mark_many_with_cache(self, entries):
            seen.append(len(entries))

    monkeypatch.setattr(bft, "DEDUP_STORE", _Store())
    n = bft._persist_rows([
        _Row(),
        _Row(verdict="Отклонить (ошибка загрузки)"),
        _Row(verdict="Возможно новость", llm_relevance=""),
        _Row(verdict="Отклонено LLM", llm_relevance="Нет"),
    ])
    assert n == 2 and seen == [2]


def test_nothing_to_freeze_means_no_write_at_all(bft, monkeypatch) -> None:
    class _Store:
        def mark_many_with_cache(self, entries):        # pragma: no cover
            raise AssertionError("не должно вызываться")

    monkeypatch.setattr(bft, "DEDUP_STORE", _Store())
    assert bft._persist_rows([_Row(url="")]) == 0


def test_no_store_is_not_an_error(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "DEDUP_STORE", None)
    assert bft._persist_rows([_Row()]) == 0


# ------------------------------------------- the provisional mark and its use

def test_the_checkpoint_marks_its_rows_provisional(bft) -> None:
    import json
    plain = json.loads(bft._cache_entry_for(_Row())[6])
    mid = json.loads(bft._cache_entry_for(_Row(), provisional=True)[6])
    assert "provisional" not in plain          # the end-of-run write is unchanged
    assert mid["provisional"] is True
    # Nothing else differs — the verdict is equally real either way.
    assert {k: v for k, v in mid.items() if k != "provisional"} == plain


def test_a_healthy_run_clears_the_mark(bft, monkeypatch) -> None:
    """The end-of-run write overwrites the same url_hash without the flag, so
    a run that finishes leaves nothing provisional behind."""
    written = []

    class _Store:
        def mark_many_with_cache(self, entries):
            written.append(entries)

    monkeypatch.setattr(bft, "DEDUP_STORE", _Store())
    row = _Row()
    bft._persist_rows([row], provisional=True)     # mid-pass
    bft._persist_rows([row])                       # end of run
    import json
    assert written[0][0][0] == written[1][0][0]    # same url_hash — an upsert
    assert json.loads(written[0][0][6])["provisional"] is True
    assert "provisional" not in json.loads(written[1][0][6])


def test_provisional_rows_are_not_evidence_of_a_push(bft) -> None:
    """A run that died pushed nothing. Counting its saved rows as «отправляли в
    фид» would make the RECOVERY run divert the very stories it rescued."""
    from news_agent.adapters.storage import _blob_is_provisional
    assert _blob_is_provisional({"provisional": True})
    assert not _blob_is_provisional({})
    assert not _blob_is_provisional({"provisional": False})


def test_both_anti_repeat_stores_skip_provisional_rows(tmp_path) -> None:
    """End-to-end against a real store: a provisional accepted row must appear
    in neither the own-pushes base nor the event-key map."""
    import json
    from datetime import datetime, timezone
    from news_agent.adapters.storage import DedupStore

    store = DedupStore(tmp_path / "t.sqlite")
    now = datetime.now(timezone.utc).isoformat()

    def blob(**over):
        b = {"verdict": "Точно новость", "llm_relevance": "Да", "llm_reason": "ок",
             "event_brand": "bmw", "event_model": "x5", "event_type": "launch"}
        b.update(over)
        return json.dumps(b, ensure_ascii=False)

    store.mark_many_with_cache([
        ("h1", "https://a/1", "BMW X5 представлен официально", now,
         "a.com", "p", blob(), "лид"),
        ("h2", "https://a/2", "Audi Q7 представлен официально", now,
         "a.com", "p", blob(provisional=True, event_model="q7"), "лид"),
    ])
    titles = store.recent_pushed_titles("p", days=30)
    assert any("bmw" in t for t in titles)
    assert not any("audi" in t for t in titles)
    keys = store.recent_event_keys("p", days=30)
    assert "bmw|x5|launch" in keys
    assert "audi|q7|launch" not in keys


def test_checkpoint_every_is_parsed_defensively(bft, monkeypatch) -> None:
    import importlib
    for raw, want in (("", 25), ("nonsense", 25), ("0", 0), ("-3", 0), ("5", 5)):
        monkeypatch.setenv("LLM_CHECKPOINT_EVERY", raw)
        mod = importlib.reload(bft)
        assert mod.LLM_CHECKPOINT_EVERY == want, f"{raw!r} → {mod.LLM_CHECKPOINT_EVERY}"
    monkeypatch.delenv("LLM_CHECKPOINT_EVERY", raising=False)
    importlib.reload(bft)
