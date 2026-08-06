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


def test_prefill_indexes_against_the_candidate_list(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 2)
    rows = [_Row("a"), _Row("b"), _Row("c"), _Row("d")]
    client = _BatchClient([["R1", "R2"], ["R3", None]])
    out = bft._prefill_editorial_reviews(
        rows, client=client, sections=[], country="Russia",
        budget=BudgetTracker(cap_usd=10.0))
    assert out == {0: "R1", 1: "R2", 2: "R3"}   # index 3 stays for the loop
    assert client.chunks == [2, 2]


def test_prefill_survives_a_failed_chunk(bft, monkeypatch) -> None:
    """One dead chunk costs ten single calls, not ten stories."""
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 2)
    rows = [_Row(x) for x in "abcd"]
    client = _BatchClient([RuntimeError("overloaded"), ["R3", "R4"]])
    out = bft._prefill_editorial_reviews(
        rows, client=client, sections=[], country="Russia",
        budget=BudgetTracker(cap_usd=10.0))
    assert out == {2: "R3", 3: "R4"}


def test_prefill_stops_batching_after_two_failures(bft, monkeypatch) -> None:
    """A dead API means every chunk dies. Hand the failure to the per-article
    loop, where the circuit breaker can see it, instead of burning the run."""
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 1)
    rows = [_Row(x) for x in "abcde"]
    client = _BatchClient([RuntimeError("403"), RuntimeError("403"),
                           ["R3"], ["R4"], ["R5"]])
    out = bft._prefill_editorial_reviews(
        rows, client=client, sections=[], country="Russia",
        budget=BudgetTracker(cap_usd=10.0))
    assert out == {}
    assert len(client.chunks) == 2


def test_prefill_respects_the_cost_cap(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 1)
    rows = [_Row(x) for x in "abc"]
    client = _BatchClient([["R1"], ["R2"], ["R3"]])
    with pytest.raises(BudgetExceeded):
        bft._prefill_editorial_reviews(
            rows, client=client, sections=[], country="Russia",
            budget=BudgetTracker(cap_usd=0.015))


def test_prefill_charges_only_the_rows_it_answered(bft, monkeypatch) -> None:
    """An unanswered row pays again for its own single call; charging it here
    too would overstate what the sheet reports per story."""
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 2)
    rows = [_Row("a"), _Row("b")]
    client = _BatchClient([["R1", None]])
    bft._prefill_editorial_reviews(
        rows, client=client, sections=[], country="Russia",
        budget=BudgetTracker(cap_usd=10.0))
    assert rows[0].llm_cost_usd == pytest.approx(0.01)
    assert rows[1].llm_cost_usd is None


def test_prefill_skips_clients_without_the_batch_call(bft, monkeypatch) -> None:
    monkeypatch.setattr(bft, "EDITORIAL_BATCH_SIZE", 5)
    stub = types.SimpleNamespace()      # legacy/stub client
    out = bft._prefill_editorial_reviews(
        [_Row("a")], client=stub, sections=[], country="Russia",
        budget=BudgetTracker(cap_usd=10.0))
    assert out == {}
