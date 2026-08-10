"""Batched editorial review: what happens when a batch answers badly.

The batch exists to stop paying for 418 reads of an 8.2k-token constitution.
Its danger is not a wrong verdict — a wrong verdict is what the editor already
corrects — it is a MISSING verdict, because a story that quietly falls out of
the batch never reaches anyone to be corrected. So everything here is about
absence: a skipped article, a hallucinated number, a truncated list, a dead
call. Each one must leave the article to be judged singly, and none may leave
it silently classified by accident.
"""

from __future__ import annotations

import sys
import types

import pytest

from news_agent.adapters.llm.anthropic_client import AnthropicLLMClient
from news_agent.adapters.llm.base import (
    EDITORIAL_REVIEW_BATCH_SCHEMA,
    EDITORIAL_REVIEW_SCHEMA,
    build_editorial_review_batch_user,
)
from news_agent.core.budget import BudgetExceeded, BudgetTracker
from news_agent.core.models import LLMUsage


def _usage(cost: float = 0.01) -> LLMUsage:
    return LLMUsage(input_tokens=100, output_tokens=10, cost_usd=cost,
                    latency_ms=1, provider="anthropic", model="test")


def _verdict(n: int, *, publish: bool = True) -> dict:
    return {
        "n": n,
        "should_publish": publish,
        "section": "Confirmed",
        "region": "Global",
        "confidence": 0.8,
        "reason": "brand-confirmed launch",
        "event_signature": {"brand": "bmw", "model": "x5", "event_type": "launch"},
    }


class _FakeClient(AnthropicLLMClient):
    """Real parsing, fake transport."""

    def __init__(self, payloads: list) -> None:  # noqa: ANN001
        self.model = "test"
        self._payloads = list(payloads)
        self.calls: list[str] = []

    def _tool_call(self, *, system, user, tool, max_tokens):  # noqa: ANN001
        self.calls.append(user)
        p = self._payloads.pop(0)
        if isinstance(p, Exception):
            raise p
        return p, _usage()


# --------------------------------------------------------------- the schema

def test_batch_item_shape_tracks_the_single_verdict() -> None:
    """Derived, not copied — a field added to the single schema must appear in
    the batch one too, or the two paths would disagree about what a verdict is."""
    item = EDITORIAL_REVIEW_BATCH_SCHEMA["properties"]["verdicts"]["items"]
    assert set(item["properties"]) == set(EDITORIAL_REVIEW_SCHEMA["properties"]) | {"n"}
    assert item["required"] == ["n", *EDITORIAL_REVIEW_SCHEMA["required"]]


def test_event_signature_is_required_in_a_batch() -> None:
    """Dedup is fed by this field. A batch that dropped it would blind the
    semantic layer while every verdict still looked perfectly valid."""
    item = EDITORIAL_REVIEW_BATCH_SCHEMA["properties"]["verdicts"]["items"]
    assert "event_signature" in item["required"]


def test_batch_user_numbers_every_article() -> None:
    msg = build_editorial_review_batch_user([("A", "body a"), ("B", "body b")])
    assert "### Article 1" in msg and "### Article 2" in msg
    assert "Title: A" in msg and "body b" in msg


def test_batch_user_keeps_the_full_body_budget() -> None:
    """A batched article must see what a single article sees; trimming the body
    here would make the batch cheaper by making it judge less."""
    msg = build_editorial_review_batch_user([("T", "z" * 6000)])
    assert msg.count("z") == 4000


# ---------------------------------------------------- partial / bad answers

def test_missing_verdict_leaves_a_hole_not_a_guess() -> None:
    c = _FakeClient([{"verdicts": [_verdict(1), _verdict(3)]}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a"), ("b", "b"), ("c", "c")], sections=[], portal_country="Russia")
    assert out[1] is None            # article 2 was skipped by the model
    assert out[0] is not None and out[2] is not None


def test_out_of_range_number_is_dropped() -> None:
    """A hallucinated index must not land a verdict on the wrong article."""
    c = _FakeClient([{"verdicts": [_verdict(1), _verdict(7), _verdict(0)]}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a"), ("b", "b")], sections=[], portal_country="Russia")
    assert out[0] is not None
    assert out[1] is None


def test_repeated_number_does_not_overwrite() -> None:
    c = _FakeClient([{"verdicts": [_verdict(1, publish=True),
                                   _verdict(1, publish=False)]}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a")], sections=[], portal_country="Russia")
    assert out[0].should_publish is True


def test_unusable_verdict_falls_out_instead_of_raising() -> None:
    bad = _verdict(1)
    bad["confidence"] = 5.0          # outside 0..1 — model rejects it
    c = _FakeClient([{"verdicts": [bad, _verdict(2)]}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a"), ("b", "b")], sections=[], portal_country="Russia")
    assert out[0] is None
    assert out[1] is not None


def test_rejection_tolerates_the_same_gaps_as_a_single_call() -> None:
    c = _FakeClient([{"verdicts": [
        {"n": 1, "should_publish": False, "reason": "yellow press"}]}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a")], sections=[], portal_country="Russia")
    assert out[0].should_publish is False
    assert out[0].section == "" and out[0].region is None


def test_empty_input_costs_nothing() -> None:
    c = _FakeClient([])
    out, u = c.editorial_review_batch(items=[], sections=[], portal_country="Russia")
    assert out == [] and u.cost_usd == 0.0
    assert c.calls == []


def test_a_dead_call_propagates() -> None:
    """Swallowing it here would hide a usage limit from the circuit breaker."""
    c = _FakeClient([RuntimeError("credit balance too low")])
    with pytest.raises(RuntimeError):
        c.editorial_review_batch(items=[("a", "a")], sections=[], portal_country="Russia")


# ------------------------------------------------- the prefill in the runner

def _load_prefill():
    """Import the runner's prefill helper without importing the whole script's
    side effects twice."""
    import batch_fetch_test as bft
    return bft


@pytest.fixture(scope="module")
def bft():
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "scripts"))
    return _load_prefill()


class _Row:
    def __init__(self, title: str) -> None:
        self.title = title
        self.body_excerpt = f"body of {title}"
        self.llm_cost_usd = None


class _BatchClient:
    """Answers per chunk from a script of payloads."""

    def __init__(self, script: list) -> None:
        self._script = list(script)
        self.chunks: list[int] = []

    def editorial_review_batch(self, *, items, sections, portal_country):  # noqa: ANN001
        self.chunks.append(len(items))
        p = self._script.pop(0)
        if isinstance(p, Exception):
            raise p
        return p, _usage()


# ------------------------------------------------ one chunk, judged on demand

def _judge(bft, rows, client, start=0, cap=10.0, out=None):
    if out is None:
        out = {}
    rc = bft._judge_chunk(rows, start, client=client, sections=[],
                          country="Russia", budget=BudgetTracker(cap_usd=cap),
                          out=out)
    return rc, out


def test_a_chunk_fills_indexes_relative_to_the_whole_list(bft, monkeypatch) -> None:
    """`out` is keyed by position in `candidates`, not in the chunk — the loop
    looks itself up by its own counter, so an off-by-chunk would hand a verdict
    to the wrong article."""
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 2)
    rows = [_Row(x) for x in "abcd"]
    rc, out = _judge(bft, rows, _BatchClient([["R3", "R4"]]), start=2)
    assert rc == "" and out == {2: "R3", 3: "R4"}


def test_a_gap_in_the_answer_stays_a_gap(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 3)
    rows = [_Row(x) for x in "abc"]
    _, out = _judge(bft, rows, _BatchClient([["R1", None, "R3"]]))
    assert out == {0: "R1", 2: "R3"}      # index 1 falls to the single path


def test_a_failed_chunk_is_reported_not_raised(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 2)
    rc, out = _judge(bft, [_Row("a"), _Row("b")],
                     _BatchClient([RuntimeError("overloaded")]))
    assert rc == "fail" and out == {}


def test_a_recognised_limit_is_reported_as_such(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 2)
    err = RuntimeError("Your credit balance is too low to access the API")
    assert bft.looks_like_usage_limit(str(err))
    rc, _ = _judge(bft, [_Row("a")], _BatchClient([err]))
    assert rc == "limit"


def test_a_chunk_past_the_end_does_nothing(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 5)
    client = _BatchClient([])
    rc, out = _judge(bft, [_Row("a")], client, start=10)
    assert rc == "" and out == {} and client.chunks == []


def test_only_answered_rows_are_charged(bft, monkeypatch) -> None:
    """An unanswered row pays again for its own single call; charging it here
    too would overstate what the sheet reports per story."""
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 2)
    rows = [_Row("a"), _Row("b")]
    _judge(bft, rows, _BatchClient([["R1", None]]))
    assert rows[0].llm_cost_usd == pytest.approx(0.01)
    assert rows[1].llm_cost_usd is None


def test_a_budget_trip_keeps_the_verdicts_already_paid_for(bft, monkeypatch) -> None:
    """The money is spent either way. Losing the verdicts too would mean paying
    for the same judgement twice — the caller owns the dict for this reason."""
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 1)
    out: dict = {}
    budget = BudgetTracker(cap_usd=0.015)
    rows = [_Row(x) for x in "abc"]
    bft._judge_chunk(rows, 0, client=_BatchClient([["R1"]]), sections=[],
                     country="Russia", budget=budget, out=out)
    with pytest.raises(BudgetExceeded):
        bft._judge_chunk(rows, 1, client=_BatchClient([["R2"]]), sections=[],
                         country="Russia", budget=budget, out=out)
    assert out == {0: "R1", 1: "R2"}      # both survived the raise


# ------------------------------------------------------- when to judge, and when to stop

def test_chunks_are_judged_once_each_in_order(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 10)
    d = bft._BatchDriver(True)
    fired = []
    for i in range(1, 46):               # the loop's own 1-based counter
        if d.due(i - 1):
            fired.append(d.next_at)
            d.record("")
    assert fired == [0, 10, 20, 30, 40]  # aligned, no repeats, no gaps


def test_a_failed_chunk_still_advances_the_boundary(bft, monkeypatch) -> None:
    """The bug this class exists for: leaving the boundary put made the next
    iteration judge the very same ten again — and pay for them again."""
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 10)
    d = bft._BatchDriver(True)
    assert d.due(0)
    d.record("fail")
    assert d.next_at == 10
    assert not d.due(1)                  # row 1 falls to the single path


def test_two_failures_in_a_row_stop_batching(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 10)
    d = bft._BatchDriver(True)
    assert d.record("fail") == ""
    msg = d.record("fail")
    assert "поштучно" in msg and not d.on
    assert not d.due(999)


def test_a_success_between_failures_resets_the_count(bft, monkeypatch) -> None:
    """Alternating failures are a flaky API, not a dead one — batching should
    survive them."""
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 10)
    d = bft._BatchDriver(True)
    for outcome in ("fail", "", "fail", "", "fail"):
        d.record(outcome)
    assert d.on


def test_a_recognised_limit_stops_batching_at_once(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 10)
    d = bft._BatchDriver(True)
    msg = d.record("limit")
    assert "лимит" in msg and not d.on


def test_a_disabled_driver_never_fires(bft) -> None:
    d = bft._BatchDriver(False)
    assert not any(d.due(i) for i in range(100))


# ------------------------- aug-10: a good answer thrown away over its shape

def test_a_number_written_as_text_is_still_a_number() -> None:
    """Twice on aug-10 a batch returned ten well-formed verdicts and every one
    was discarded, costing ten single calls to redo work already paid for. The
    range check is the safety property; the type never was."""
    from news_agent.adapters.llm.anthropic_client import _as_index
    assert _as_index(3) == 3
    assert _as_index("3") == 3
    assert _as_index(" #3 ") == 3
    assert _as_index(3.0) == 3


def test_anything_that_is_not_a_whole_number_stays_rejected() -> None:
    from news_agent.adapters.llm.anthropic_client import _as_index
    for junk in (None, True, False, 3.5, "3.5", "три", "", "-", [3], {"n": 3}):
        assert _as_index(junk) is None, junk


def test_a_string_index_lands_on_the_right_article() -> None:
    c = _FakeClient([{"verdicts": [dict(_verdict(1), n="1"),
                                   dict(_verdict(2), n="2")]}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a"), ("b", "b")], sections=[], portal_country="Russia")
    assert out[0] is not None and out[1] is not None


def test_the_range_check_still_holds_for_coerced_numbers() -> None:
    """Coercing the type must not soften the guard that keeps a hallucinated
    number off someone else's article."""
    c = _FakeClient([{"verdicts": [dict(_verdict(1), n="9"),
                                   dict(_verdict(1), n="0")]}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a")], sections=[], portal_country="Russia")
    assert out == [None]


def test_verdicts_under_another_key_are_still_read() -> None:
    """A model that answers with `reviews` has still done the work."""
    c = _FakeClient([{"reviews": [_verdict(1), _verdict(2)]}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a"), ("b", "b")], sections=[], portal_country="Russia")
    assert out[0] is not None and out[1] is not None


def test_a_genuinely_empty_answer_is_still_a_hole() -> None:
    c = _FakeClient([{"verdicts": []}])
    out, _ = c.editorial_review_batch(
        items=[("a", "a")], sections=[], portal_country="Russia")
    assert out == [None]
