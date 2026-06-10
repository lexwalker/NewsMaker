"""Archive-dedup judge (precision #4). Embedding retrieval narrows the
published archive to a few near neighbours; the LLM judges same-event.

Tests use a fake embed model (explicit unit vectors → controlled cosines)
and a recording fake judge, so the retrieval + short-circuit + verdict
plumbing is pinned without any network or model download.
"""

import numpy as np

from news_agent.core.archive_dedup import (
    ArchiveDedupJudge,
    ArchiveEntry,
    build_dedup_prompt,
    parse_verdict,
    _query_lines,
)


class FakeEmbed:
    """Maps known strings to explicit vectors; unknowns → [0,0,1]."""

    def __init__(self, table):
        self.table = table

    def encode(self, texts, normalize_embeddings=True,
               show_progress_bar=False, batch_size=128):
        out = []
        for t in texts:
            v = np.array(self.table.get(t, [0.0, 0.0, 1.0]), dtype="float32")
            n = np.linalg.norm(v)
            out.append(v / n if n else v)
        return np.array(out, dtype="float32")


class RecordingJudge:
    def __init__(self, response):
        self.response = response
        self.calls = 0

    def __call__(self, prompt):
        self.calls += 1
        self.last_prompt = prompt
        return self.response, 0.001


CAND = "Geely Coolray facelift revealed"
TABLE = {
    CAND: [1.0, 0.0, 0.0],
    "Geely Coolray facelift official": [0.99, 0.1, 0.0],  # ~0.995 to CAND
    "Geely Coolray facelift revealed copy": [1.0, 0.0, 0.0],  # self, 1.0
    "BMW X5 unrelated launch": [0.0, 1.0, 0.0],            # 0.0 to CAND
}


def _judge(entries, response, **kw):
    j = RecordingJudge(response)
    judge = ArchiveDedupJudge(entries, embed_model=FakeEmbed(TABLE),
                              judge_fn=j, **kw)
    judge.build_index()
    return judge, j


# ── cheap short-circuit ─────────────────────────────────────────────

def test_no_near_neighbor_skips_llm() -> None:
    judge, j = _judge([ArchiveEntry("BMW X5 unrelated launch")],
                      '{"duplicate":true,"match":1}')
    v = judge.is_duplicate(CAND)
    assert v.is_duplicate is False
    assert v.n_candidates == 0
    assert j.calls == 0          # no LLM call when nothing is close


# ── duplicate detected ──────────────────────────────────────────────

def test_near_neighbor_llm_says_duplicate() -> None:
    judge, j = _judge(
        [ArchiveEntry("Geely Coolray facelift official", date="2026-06-01")],
        '{"duplicate":true,"match":1,"confidence":0.9,"reason":"same reveal"}')
    v = judge.is_duplicate(CAND)
    assert j.calls == 1
    assert v.is_duplicate is True
    assert v.matched_title == "Geely Coolray facelift official"
    assert v.matched_date == "2026-06-01"
    assert v.confidence == 0.9


def test_near_neighbor_llm_says_not_duplicate() -> None:
    judge, j = _judge(
        [ArchiveEntry("Geely Coolray facelift official")],
        '{"duplicate":false,"match":0,"reason":"new generation, not facelift"}')
    v = judge.is_duplicate(CAND)
    assert j.calls == 1
    assert v.is_duplicate is False
    assert v.matched_title == ""


# ── self-exclusion (offline measurement realism) ────────────────────

def test_exclude_self_removes_own_copy_then_no_neighbor() -> None:
    # genuine row: its only archive match is its own identical copy →
    # after self-exclusion nothing is near → not-dup, no LLM
    judge, j = _judge(
        [ArchiveEntry("Geely Coolray facelift revealed copy"),
         ArchiveEntry("BMW X5 unrelated launch")],
        '{"duplicate":true,"match":1}')
    v = judge.is_duplicate(CAND, exclude_self_cos=0.95)
    assert v.is_duplicate is False
    assert j.calls == 0


def test_exclude_self_keeps_real_prior_duplicate() -> None:
    # dup row: self copy (1.0) excluded, but a DIFFERENT prior story
    # (0.995) remains → judge is consulted
    judge, j = _judge(
        [ArchiveEntry("Geely Coolray facelift revealed copy"),   # self 1.0
         ArchiveEntry("Geely Coolray facelift official")],        # 0.995
        '{"duplicate":true,"match":1,"confidence":0.8}')
    v = judge.is_duplicate(CAND, exclude_self_cos=0.95)
    assert j.calls == 1
    assert v.is_duplicate is True


# ── parsing / prompt / helpers ──────────────────────────────────────

def test_json_parse_error_is_safe_notdup() -> None:
    judge, j = _judge([ArchiveEntry("Geely Coolray facelift official")],
                      "not json at all")
    v = judge.is_duplicate(CAND)
    assert v.is_duplicate is False
    assert v.error == "json_parse"


def test_query_lines_splits_en_ru() -> None:
    q = _query_lines("EN: Geely Coolray revealed\nRU: Geely Coolray показан")
    assert q == ["Geely Coolray revealed", "Geely Coolray показан"]
    assert _query_lines("") == []


def test_build_prompt_has_candidate_and_neighbors() -> None:
    from news_agent.core.archive_dedup import Neighbor
    p = build_dedup_prompt(
        "Cand title", "lead text",
        [Neighbor(ArchiveEntry("Published A", date="2026-06-01"), 0.9)])
    assert "Cand title" in p
    assert "Published A" in p
    assert "ДУБЛЕМ" in p           # the discriminator instruction


def test_parse_verdict_extracts_json() -> None:
    assert parse_verdict('blah {"duplicate": true, "match": 2} tail')[
        "duplicate"] is True
