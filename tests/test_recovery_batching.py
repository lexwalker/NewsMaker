"""Recovery judges in batches too — the last stage that did not.

On aug-12 a transient 403 aborted the main LLM pass at candidate 5 of 338. The
chain's auto-recovery finished the job correctly, and charged $2.51 for it: it
read the 8.5k-token constitution 338 times where the main pass would have read
it 34. Everything about batching had been proven over five days of runs in the
main pass and none of it reached the one stage that runs when things go wrong.

The properties that matter are the same ones the main pass needed, so they are
pinned the same way: the two paths must judge identical input, a chunk that
answers nothing must cost articles nothing, and a prefilled row must never
convince the circuit breaker that a dead API is alive.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def rfl():
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "src"))
    sys.path.insert(0, str(root / "scripts"))
    import retry_failed_llm
    return retry_failed_llm


# ------------------------------------------------- what the two paths are fed

def test_the_batch_and_the_single_call_see_the_same_article(rfl) -> None:
    """The whole safety story of batching is that a row falling out of a chunk
    is judged singly and identically. Building the title inline in one path and
    in a helper in the other is how that quietly stops being true."""
    from news_agent.core.articles_schema import COL
    row = [""] * 40
    row[COL.TITLE] = "EN: BMW unveils the new X5\nRU: BMW представил новый X5"
    row[COL.LEDE] = "Тело статьи про X5."
    assert rfl._title_and_body(row) == ("BMW unveils the new X5", "Тело статьи про X5.")


def test_a_title_without_the_en_prefix_is_passed_through(rfl) -> None:
    from news_agent.core.articles_schema import COL
    row = [""] * 40
    row[COL.TITLE] = "Просто заголовок без префикса"
    row[COL.LEDE] = "лид"
    assert rfl._title_and_body(row) == ("Просто заголовок без префикса", "лид")


def test_a_missing_lede_is_empty_not_an_error(rfl) -> None:
    from news_agent.core.articles_schema import COL
    row = [""] * (COL.TITLE + 1)
    row[COL.TITLE] = "Заголовок"
    assert rfl._title_and_body(row) == ("Заголовок", "")


# ------------------------------------------------------------ the batch size

def test_batch_size_matches_the_main_pass(rfl) -> None:
    """Two stages judging the same constitution with different chunk sizes
    would make their costs incomparable and their behaviour diverge under
    load. Same env var, same clamp, same default."""
    import batch_fetch_test as bft
    assert rfl.EDITORIAL_BATCH_SIZE == bft.EDITORIAL_BATCH_SIZE


def test_batch_size_is_parsed_defensively(rfl, monkeypatch) -> None:
    """This is the documented off switch; a recovery must not die on the import
    line because the variable was set to nothing."""
    import importlib
    for raw, want in (("", 10), ("мусор", 10), ("1", 1), ("0", 1), ("999", 25)):
        monkeypatch.setenv("EDITORIAL_BATCH_SIZE", raw)
        mod = importlib.reload(rfl)
        assert mod.EDITORIAL_BATCH_SIZE == want, raw
    monkeypatch.delenv("EDITORIAL_BATCH_SIZE", raising=False)
    importlib.reload(rfl)


# ------------------------------------------- the client contract it relies on

def test_recovery_uses_the_same_batch_call_as_the_main_pass(rfl) -> None:
    """Not a copy of it. The coercion of a stringy index, the range guard that
    keeps a hallucinated number off someone else's article, the tolerance for a
    verdicts array under another key — all of that lives in the client, and the
    recovery gets it only by calling the same method."""
    from news_agent.adapters.llm.anthropic_client import AnthropicLLMClient
    assert hasattr(AnthropicLLMClient, "editorial_review_batch")
    src = (Path(__file__).resolve().parents[1]
           / "scripts" / "retry_failed_llm.py").read_text(encoding="utf-8")
    assert "editorial_review_batch(" in src
    # …and still falls back to the single call for whatever the batch skipped.
    assert "editorial_client.editorial_review(" in src


def test_the_breaker_is_only_cleared_by_a_live_call(rfl) -> None:
    """The aug-06 trap, which this stage was one edit away from inheriting: a
    prefilled row raising no exception used to look like a successful call, so
    with most rows prefilled the counter could never reach five and a dead API
    would finish the pass quietly."""
    src = (Path(__file__).resolve().parents[1]
           / "scripts" / "retry_failed_llm.py").read_text(encoding="utf-8")
    body = src[src.index("for idx, (sheet_row, r) in enumerate(targets"):]
    reset = body.index("consec_errors = 0")
    single = body.index("editorial_client.editorial_review(")
    # The only reset must sit INSIDE the else-branch that made a real call,
    # i.e. after the single call appears — not before it in the shared tail.
    assert reset > single, "счётчик прерывателя сбрасывается вне живого вызова"


# ----------------------------------------------- the cost of a batched verdict

def test_a_batched_row_does_not_read_the_single_calls_usage() -> None:
    """The crash this pins: `ur` is bound only by the live single call, and the
    cost column read it unconditionally — so the first batched row that reached
    the write raised UnboundLocalError. Which is every healthy recovery: the
    aug-17 run died four rows in, after paying for the chunk.

    Static because the alternative is standing up Sheets to reach line 641; the
    property is structural anyway — the cost line must not name `ur`.
    """
    src = (Path(__file__).resolve().parents[1]
           / "scripts" / "retry_failed_llm.py").read_text(encoding="utf-8")
    cost_line = next(l for l in src.splitlines() if "+ ut.cost_usd" in l)
    assert "ur.cost_usd" not in cost_line, cost_line
    assert "_review_cost" in cost_line, cost_line


def test_both_paths_bind_the_review_cost() -> None:
    """Neither branch may leave it unset: the batch pops a stored share, the
    live call takes it from its own usage."""
    src = (Path(__file__).resolve().parents[1]
           / "scripts" / "retry_failed_llm.py").read_text(encoding="utf-8")
    assert "_review_cost = prefilled_cost.pop(" in src
    assert "_review_cost = ur.cost_usd" in src


def test_the_chunk_cost_is_split_over_answered_rows_only() -> None:
    """A chunk that answers three of ten must charge those three, not all ten —
    the seven that fell through pay again for their own single call, and
    charging them twice would overstate the per-row cost on the sheet."""
    src = (Path(__file__).resolve().parents[1]
           / "scripts" / "retry_failed_llm.py").read_text(encoding="utf-8")
    assert "_answered = [_j for _j, _rev in enumerate(_revs) if _rev is not None]" in src
    assert "_u.cost_usd / len(_answered)" in src
    # …and an all-failed chunk must not divide by zero.
    assert "if _answered else 0.0" in src
