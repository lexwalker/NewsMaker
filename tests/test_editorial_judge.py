"""Coverage for editorial_judge — precedent-based advisory judge.

Mocks both the embed model and the LLM so tests are fast + offline.
The real accuracy is measured live against editor decisions (advisory
log), NOT asserted here — these tests only verify the mechanics.
"""

from __future__ import annotations

import json

import numpy as np
import pytest

from news_agent.core.editorial_judge import EditorialJudge, JudgeVerdict


class _FakeEmbed:
    """Deterministic fake embedder: hashes text to a small vector."""

    def encode(self, texts, **kw):
        out = []
        for t in texts:
            h = abs(hash(t)) % 997
            v = np.array([h % 7, (h // 7) % 7, (h // 49) % 7],
                         dtype=float)
            n = np.linalg.norm(v) or 1.0
            out.append(v / n)
        return np.array(out)


class _FakeUsage:
    input_tokens = 500
    output_tokens = 120


class _FakeResp:
    def __init__(self, text):
        self.content = [type("B", (), {"type": "text", "text": text})()]
        self.usage = _FakeUsage()


class _FakeMessages:
    def __init__(self, planned):
        self.planned = planned
        self.calls = []

    def create(self, **kw):
        self.calls.append(kw)
        if isinstance(self.planned, Exception):
            raise self.planned
        return _FakeResp(self.planned)


class _FakeLLM:
    def __init__(self, planned):
        self.messages = _FakeMessages(planned)


def _decisions_file(tmp_path):
    p = tmp_path / "dec.json"
    p.write_text(json.dumps({
        "positive": [
            {"title": "Hongqi launched new SUV H9", "section": "Confirmed"},
            {"title": "BYD unveiled Seal sedan", "section": "Confirmed"},
            {"title": "Lada sales rose in Russia", "section": "Local specifics"},
        ],
        "negative": [
            {"title": "Maextro outsold Mercedes in sales",
             "comment": "не Факты, продажи одной модели"},
            {"title": "Government funds road repairs after flood",
             "comment": "финансирование ЧС не постим"},
        ],
    }, ensure_ascii=False), encoding="utf-8")
    return p


def test_judge_happy_path(tmp_path):
    llm = _FakeLLM('{"publish": true, "section": "Confirmed", '
                  '"confidence": 0.8, "reason": "похоже на Hongqi launch"}')
    j = EditorialJudge(_decisions_file(tmp_path), llm_client=llm,
                       embed_model=_FakeEmbed())
    v = j.judge("Hongqi names new SUV G919")
    assert v.advisory_publish is True
    assert v.advisory_section == "Confirmed"
    assert v.confidence == 0.8
    assert v.cost_usd > 0
    assert v.error == ""
    # precedents surfaced for transparency
    assert len(v.precedents_published) >= 1


def test_judge_reject(tmp_path):
    llm = _FakeLLM('{"publish": false, "section": "", "confidence": 0.7, '
                   '"reason": "аналогично отклонённой Maextro"}')
    j = EditorialJudge(_decisions_file(tmp_path), llm_client=llm,
                       embed_model=_FakeEmbed())
    v = j.judge("Zeekr dominates segment in China")
    assert v.advisory_publish is False
    assert "Maextro" in v.reason


def test_retrieve_excludes_self_title(tmp_path):
    """Offline-eval guard: a candidate whose exact title is in the
    precedent base must NOT retrieve itself (else it trivially self-
    predicts). 16 real eval rows are in the 88-negative base."""
    llm = _FakeLLM('{"publish": false, "section": "", "confidence": 0.6, '
                   '"reason": "x"}')
    j = EditorialJudge(_decisions_file(tmp_path), llm_client=llm,
                       embed_model=_FakeEmbed(), k_neg=2)
    self_title = "Maextro outsold Mercedes in sales"  # exact negative precedent
    v = j.judge(self_title, exclude_title=self_title)
    surfaced = " | ".join(v.precedents_rejected)
    assert "Maextro outsold Mercedes" not in surfaced, \
        "candidate retrieved its own exact-title precedent"


def test_retrieve_without_exclude_includes_self(tmp_path):
    """Without exclude_title the exact match IS retrieved (baseline
    behaviour preserved)."""
    llm = _FakeLLM('{"publish": false, "section": "", "confidence": 0.6, '
                   '"reason": "x"}')
    j = EditorialJudge(_decisions_file(tmp_path), llm_client=llm,
                       embed_model=_FakeEmbed(), k_neg=2)
    self_title = "Maextro outsold Mercedes in sales"
    v = j.judge(self_title)
    surfaced = " | ".join(v.precedents_rejected)
    assert "Maextro outsold Mercedes" in surfaced


def test_judge_empty_title(tmp_path):
    j = EditorialJudge(_decisions_file(tmp_path), llm_client=_FakeLLM("{}"),
                       embed_model=_FakeEmbed())
    v = j.judge("")
    assert v.advisory_publish is None
    assert "empty" in v.reason


def test_judge_llm_error_graceful(tmp_path):
    llm = _FakeLLM(RuntimeError("API down"))
    j = EditorialJudge(_decisions_file(tmp_path), llm_client=llm,
                       embed_model=_FakeEmbed())
    v = j.judge("Some car news")
    assert v.advisory_publish is None
    assert "llm" in v.error


def test_judge_bad_json_graceful(tmp_path):
    llm = _FakeLLM("not json at all, the model rambled")
    j = EditorialJudge(_decisions_file(tmp_path), llm_client=llm,
                       embed_model=_FakeEmbed())
    v = j.judge("Some car news")
    assert v.advisory_publish is None
    assert v.error == "json_parse"
    # cost still recorded (LLM was called)
    assert v.cost_usd > 0


def test_judge_advisory_never_decides(tmp_path):
    """Sanity: the verdict object is advisory-only — it has no method
    or field that mutates a pipeline decision. This is a contract test."""
    v = JudgeVerdict(advisory_publish=True, advisory_section="Confirmed")
    # The fields are named 'advisory_*' on purpose.
    assert hasattr(v, "advisory_publish")
    assert not hasattr(v, "should_publish")  # not a decision object


def test_vectors_cache_roundtrip(tmp_path):
    cache = tmp_path / "vec.npz"
    j = EditorialJudge(_decisions_file(tmp_path),
                       vectors_cache=cache,
                       llm_client=_FakeLLM('{"publish": true, "reason": "x"}'),
                       embed_model=_FakeEmbed())
    j.judge("test one")
    assert cache.exists()  # cache written
    # second instance loads from cache (same decisions hash)
    j2 = EditorialJudge(_decisions_file(tmp_path),
                        vectors_cache=cache,
                        llm_client=_FakeLLM('{"publish": true, "reason": "x"}'),
                        embed_model=_FakeEmbed())
    j2.judge("test two")
    assert cache.exists()
