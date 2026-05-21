"""Coverage for news_agent.core.llm_editor.

Tests are split between pure helpers (no LLM) and the cluster_group
function (mocked LLM client). The PoC validation on real v41 data is
in scripts/_llm_editor_poc.py — leave that as the empirical regression
harness, not part of pytest.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from news_agent.core.llm_editor import (
    DEFAULT_CONFIDENCE,
    Event,
    EditorResult,
    cluster_group,
    group_articles_by_brand,
)


# ── group_articles_by_brand ─────────────────────────────────────────

def test_group_by_brand_basic() -> None:
    articles = [
        {"row": 1, "title": "Mercedes-Benz S-Class refreshed", "lede": ""},
        {"row": 2, "title": "Audi A4 spy shots", "lede": ""},
        {"row": 3, "title": "Mercedes-Benz GLC update", "lede": ""},
    ]
    g = group_articles_by_brand(articles)
    assert {"Mercedes-Benz", "Audi"} <= set(g.keys())
    assert len(g["Mercedes-Benz"]) == 2
    assert len(g["Audi"]) == 1


def test_group_by_brand_unknown_bucket() -> None:
    articles = [
        {"row": 1, "title": "Random tech story", "lede": "blockchain stuff"},
    ]
    g = group_articles_by_brand(articles)
    assert "_unknown" in g
    assert len(g["_unknown"]) == 1


def test_group_by_brand_bucket_aliases() -> None:
    """Mercedes-AMG and Mercedes-Benz collapse into one bucket when
    alias is set — the editor groups them together."""
    articles = [
        {"row": 1, "title": "Mercedes-AMG GT 4-Door", "lede": ""},
        {"row": 2, "title": "Mercedes-Benz S-Class", "lede": ""},
        {"row": 3, "title": "BMW M5 spy", "lede": ""},
    ]
    g = group_articles_by_brand(
        articles, bucket_aliases={"Mercedes-AMG": "Mercedes-Benz"}
    )
    assert len(g["Mercedes-Benz"]) == 2
    assert "Mercedes-AMG" not in g
    assert len(g["BMW"]) == 1


def test_group_by_brand_canonicalises_aliases() -> None:
    """SsangYong → KGM bucketing happens via canonicalize_brand."""
    articles = [
        {"row": 1, "title": "SsangYong Torres refreshed", "lede": ""},
        {"row": 2, "title": "KGM Torres EVX sales", "lede": ""},
    ]
    g = group_articles_by_brand(articles)
    assert g.get("KGM") and len(g["KGM"]) == 2
    assert "SsangYong" not in g


# ── Event.from_tool_output ──────────────────────────────────────────

def test_event_from_tool_output_minimal() -> None:
    d = {
        "event_id": "x", "summary": "y", "section": "Confirmed",
        "member_rows": [1, 2], "primary_row": 1,
        "confidence": 0.9, "reasoning": "z",
    }
    ev = Event.from_tool_output(d)
    assert ev.event_id == "x"
    assert ev.member_rows == [1, 2]
    assert ev.is_cross_run_dup is False  # default


def test_event_from_tool_output_cross_run() -> None:
    d = {
        "event_id": "x", "summary": "y", "section": "Confirmed",
        "member_rows": [5], "primary_row": 5, "confidence": 0.9,
        "reasoning": "", "is_cross_run_dup": True,
        "cross_run_match_url": "https://history.example/old",
    }
    ev = Event.from_tool_output(d)
    assert ev.is_cross_run_dup is True
    assert ev.cross_run_match_url == "https://history.example/old"


# ── cluster_group with mocked client ────────────────────────────────

class _FakeUsage:
    def __init__(self, in_tok=500, out_tok=200) -> None:
        self.input_tokens = in_tok
        self.output_tokens = out_tok


class _FakeBlock:
    def __init__(self, tool_input: dict) -> None:
        self.type = "tool_use"
        self.input = tool_input


class _FakeResponse:
    def __init__(self, tool_input: dict, in_tok=500, out_tok=200) -> None:
        self.content = [_FakeBlock(tool_input)]
        self.usage = _FakeUsage(in_tok, out_tok)


class _FakeMessages:
    """Mock anthropic .messages.create."""

    def __init__(self, planned_output: dict | Exception) -> None:
        self.planned = planned_output
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self.planned, Exception):
            raise self.planned
        return _FakeResponse(self.planned)


class _FakeClient:
    def __init__(self, planned: dict | Exception) -> None:
        self.messages = _FakeMessages(planned)


def _articles_pair() -> list[dict]:
    return [
        {"row": 8, "title": "VinFast unveiled second-gen VF 8 SUV",
         "lede": "VF8 ребрендинг с новой платформой",
         "section": "Confirmed", "url": "https://a.example/8"},
        {"row": 18, "title": "VinFast VF 8 SUV entered a new generation",
         "lede": "Кроссовер VinFast VF 8 перешёл в новое поколение",
         "section": "Confirmed", "url": "https://b.example/18"},
    ]


def test_cluster_group_happy_path() -> None:
    """LLM merges 2 VinFast articles into one event."""
    fake_output = {
        "events": [{
            "event_id": "vinfast_vf8_gen2",
            "summary": "VinFast unveiled second-gen VF 8",
            "section": "Confirmed",
            "member_rows": [8, 18],
            "primary_row": 8,
            "confidence": 0.95,
            "reasoning": "Same reveal told two ways",
        }],
    }
    fake = _FakeClient(fake_output)
    res = cluster_group(brand="VinFast", articles=_articles_pair(),
                        client=fake)
    assert res.error == ""
    assert len(res.events) == 1
    assert res.events[0].member_rows == [8, 18]
    assert res.events[0].confidence == 0.95
    # Cost computed
    assert res.cost_usd > 0
    assert res.input_tokens == 500
    assert res.output_tokens == 200


def test_cluster_group_low_confidence_tagged() -> None:
    """Events with conf < threshold get LOWCONF__ prefix on event_id."""
    fake_output = {
        "events": [{
            "event_id": "maybe_dup",
            "summary": "Could be one event, not sure",
            "section": "Confirmed",
            "member_rows": [8, 18],
            "primary_row": 8,
            "confidence": 0.5,  # below 0.7 default
            "reasoning": "Hedged",
        }],
    }
    fake = _FakeClient(fake_output)
    res = cluster_group(brand="VinFast", articles=_articles_pair(),
                        client=fake)
    assert len(res.events) == 1
    assert res.events[0].event_id.startswith("LOWCONF__")


def test_cluster_group_drops_unknown_member_rows() -> None:
    """If LLM hallucinates a row number not in input, drop it."""
    fake_output = {
        "events": [{
            "event_id": "vinfast",
            "summary": "x", "section": "Confirmed",
            "member_rows": [8, 18, 999],  # 999 didn't exist in input
            "primary_row": 8,
            "confidence": 0.9, "reasoning": "y",
        }],
    }
    fake = _FakeClient(fake_output)
    res = cluster_group(brand="VinFast", articles=_articles_pair(),
                        client=fake)
    assert res.events[0].member_rows == [8, 18]  # 999 dropped


def test_cluster_group_fallback_primary_when_hallucinated() -> None:
    """If primary_row is hallucinated, fall back to first valid member."""
    fake_output = {
        "events": [{
            "event_id": "x",
            "summary": "x", "section": "Confirmed",
            "member_rows": [8, 18], "primary_row": 999,
            "confidence": 0.9, "reasoning": "",
        }],
    }
    fake = _FakeClient(fake_output)
    res = cluster_group(brand="VinFast", articles=_articles_pair(),
                        client=fake)
    assert res.events[0].primary_row == 8  # first in member_rows


def test_cluster_group_llm_error_returns_empty_events() -> None:
    """Network / API failure → empty events + error string. Caller
    falls back to lexical clustering — never WORSE than current."""
    fake = _FakeClient(RuntimeError("connection refused"))
    res = cluster_group(brand="VinFast", articles=_articles_pair(),
                        client=fake)
    assert res.events == []
    assert "RuntimeError" in res.error
    assert "connection refused" in res.error
    # No cost charged on error
    assert res.cost_usd == 0.0


def test_cluster_group_empty_input_is_noop() -> None:
    res = cluster_group(brand="X", articles=[], client=None)
    assert res.events == []
    assert res.cost_usd == 0.0


def test_cluster_group_history_passed_to_llm() -> None:
    """History rendering should reach the LLM prompt."""
    fake_output = {"events": []}
    fake = _FakeClient(fake_output)
    history = [{
        "url": "https://history.example/a",
        "title": "VinFast VF 8 first details",
        "ts": "2026-05-10T12:00:00+00:00",
        "lede": "Раньше анонсировали VF 8",
    }]
    cluster_group(brand="VinFast", articles=_articles_pair(),
                  history=history, client=fake)
    sent_messages = fake.messages.calls[0]["messages"]
    user_content = sent_messages[0]["content"]
    assert "история" in user_content.lower() or \
           "History" in user_content or \
           "history.example" in user_content


def test_cluster_group_cross_run_dup_captured() -> None:
    fake_output = {
        "events": [{
            "event_id": "vinfast_gen2",
            "summary": "VinFast VF 8 gen2 reveal",
            "section": "Confirmed",
            "member_rows": [8, 18],
            "primary_row": 8,
            "confidence": 0.9,
            "reasoning": "Same as previous coverage",
            "is_cross_run_dup": True,
            "cross_run_match_url": "https://history.example/old",
        }],
    }
    fake = _FakeClient(fake_output)
    res = cluster_group(brand="VinFast", articles=_articles_pair(),
                        history=[{
                            "url": "https://history.example/old",
                            "title": "VinFast earlier story",
                            "ts": "2026-05-10",
                        }],
                        client=fake)
    assert res.events[0].is_cross_run_dup is True
    assert res.events[0].cross_run_match_url == \
        "https://history.example/old"


# ── build_news_clusters integration (smoke) ─────────────────────────

def test_apply_llm_editor_pass_merges_separate_lexical_groups(
    monkeypatch,
) -> None:
    """The whole point: when LLM-as-editor returns an event grouping
    two articles that lexical clustering left in SEPARATE buckets,
    the pass unions those buckets. Validates safe-mode: only adds
    merges, never splits."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

    # Re-import the function from the scripts module
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "build_news_clusters_module",
        Path(__file__).resolve().parents[1] / "scripts"
        / "build_news_clusters.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # Two VinFast articles in DIFFERENT lexical groups (lexical missed
    # the dup). LLM-as-editor will say they're one event.
    article_a = {
        "sheet_row": 8, "title": "VinFast unveiled second-gen VF 8",
        "lede": "VF8", "section": "Confirmed",
        "url": "https://a.example/8", "pub_dt": None, "domain": "a.example",
        "normalised": "vinfast vf 8", "region": "", "country": "",
        "published": "", "image_url": "", "launch_stage": "",
        "launch_brand_model": "VinFast VF 8", "llm_reason": "",
        "primary_dom": "", "primary_url": "", "primary_conf": "",
        "event_brand": "", "event_model": "", "event_type": "",
    }
    article_b = {
        "sheet_row": 18, "title": "VinFast VF 8 entered new generation",
        "lede": "Кроссовер VF 8", "section": "Confirmed",
        "url": "https://b.example/18", "pub_dt": None, "domain": "b.example",
        "normalised": "vinfast vf 8 generation", "region": "", "country": "",
        "published": "", "image_url": "", "launch_stage": "",
        "launch_brand_model": "VinFast VF 8", "llm_reason": "",
        "primary_dom": "", "primary_url": "", "primary_conf": "",
        "event_brand": "", "event_model": "", "event_type": "",
    }
    articles = [article_a, article_b]
    lexical_groups = [[article_a], [article_b]]  # split — lexical missed

    # Monkeypatch cluster_group to return a merge decision
    import news_agent.core.llm_editor as le

    def fake_cluster(*, brand, articles, **kwargs):
        return le.EditorResult(
            events=[le.Event(
                event_id="vinfast_vf8_gen2",
                summary="VinFast unveiled gen 2",
                section="Confirmed",
                member_rows=[8, 18],
                primary_row=8,
                confidence=0.95,
                reasoning="Same reveal",
            )],
            cost_usd=0.005, elapsed_s=2.5,
        )
    monkeypatch.setattr(mod, "_apply_llm_editor_pass", mod._apply_llm_editor_pass)
    monkeypatch.setattr(le, "cluster_group", fake_cluster)

    new_groups, stats = mod._apply_llm_editor_pass(lexical_groups, articles)

    # Both articles now in ONE group
    assert len(new_groups) == 1
    rows_in_group = {a["sheet_row"] for a in new_groups[0]}
    assert rows_in_group == {8, 18}
    assert stats["merges_applied"] == 1
    assert stats["events_returned"] == 1
    assert stats["llm_errors"] == 0


def test_apply_llm_editor_pass_does_not_split_existing_clusters(
    monkeypatch,
) -> None:
    """Safe-mode guarantee: even if LLM says two articles are
    DIFFERENT events, we keep them together if lexical already merged
    them. Lexical merges are sticky — LLM only ADDS merges."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "build_news_clusters_module2",
        Path(__file__).resolve().parents[1] / "scripts"
        / "build_news_clusters.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # Two Mercedes articles lexical ALREADY merged
    article_a = {
        "sheet_row": 23, "title": "Mercedes-AMG GT 4-Door tri-motor",
        "lede": "GT 4-Door", "section": "Confirmed",
        "url": "https://a.example/23", "pub_dt": None, "domain": "a.example",
        "normalised": "mercedes amg gt 4 door", "region": "", "country": "",
        "published": "", "image_url": "", "launch_stage": "",
        "launch_brand_model": "Mercedes-AMG GT", "llm_reason": "",
        "primary_dom": "", "primary_url": "", "primary_conf": "",
        "event_brand": "", "event_model": "", "event_type": "",
    }
    article_b = {
        "sheet_row": 38, "title": "Mercedes-AMG unveiled GT 4-Door Coupé",
        "lede": "AMG GT 4-Door reveal", "section": "Confirmed",
        "url": "https://b.example/38", "pub_dt": None, "domain": "b.example",
        "normalised": "mercedes amg gt 4 door coupe", "region": "",
        "country": "", "published": "", "image_url": "", "launch_stage": "",
        "launch_brand_model": "Mercedes-AMG GT", "llm_reason": "",
        "primary_dom": "", "primary_url": "", "primary_conf": "",
        "event_brand": "", "event_model": "", "event_type": "",
    }
    articles = [article_a, article_b]
    lexical_groups = [[article_a, article_b]]  # already together

    import news_agent.core.llm_editor as le

    def fake_cluster_splits(*, brand, articles, **kwargs):
        # LLM thinks they're DIFFERENT events
        return le.EditorResult(
            events=[
                le.Event("a", "x", "Confirmed", [23], 23, 0.9, "different"),
                le.Event("b", "y", "Confirmed", [38], 38, 0.9, "different"),
            ],
            cost_usd=0.005, elapsed_s=2.5,
        )
    monkeypatch.setattr(le, "cluster_group", fake_cluster_splits)

    new_groups, stats = mod._apply_llm_editor_pass(lexical_groups, articles)

    # SAFE: lexical merge stays intact
    assert len(new_groups) == 1
    assert {a["sheet_row"] for a in new_groups[0]} == {23, 38}
    # No merges applied (each LLM event was singleton)
    assert stats["merges_applied"] == 0


def test_apply_llm_editor_pass_handles_llm_error_gracefully(
    monkeypatch,
) -> None:
    """If LLM API fails for a brand group, log it and continue. Other
    brand groups still get processed. Lexical clusters unaffected."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "build_news_clusters_module3",
        Path(__file__).resolve().parents[1] / "scripts"
        / "build_news_clusters.py",
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    article_a = {
        "sheet_row": 1, "title": "VinFast VF 8", "lede": "",
        "section": "Confirmed", "url": "u1", "pub_dt": None, "domain": "x",
        "normalised": "vinfast vf 8", "region": "", "country": "",
        "published": "", "image_url": "", "launch_stage": "",
        "launch_brand_model": "VinFast", "llm_reason": "",
        "primary_dom": "", "primary_url": "", "primary_conf": "",
        "event_brand": "", "event_model": "", "event_type": "",
    }
    article_b = {**article_a, "sheet_row": 2,
                  "title": "VinFast VF8 new gen", "url": "u2"}
    articles = [article_a, article_b]
    lexical_groups = [[article_a], [article_b]]

    import news_agent.core.llm_editor as le

    def fake_cluster_error(*, brand, articles, **kwargs):
        return le.EditorResult(
            events=[], cost_usd=0.0, elapsed_s=0.1,
            error="APIError: timeout",
        )
    monkeypatch.setattr(le, "cluster_group", fake_cluster_error)

    new_groups, stats = mod._apply_llm_editor_pass(lexical_groups, articles)

    # Lexical groups unchanged on LLM error
    assert len(new_groups) == 2
    assert stats["llm_errors"] == 1
    assert stats["merges_applied"] == 0


def test_cluster_group_threshold_override() -> None:
    """Caller can lower the confidence floor — useful for offline
    calibration runs where we want to inspect everything."""
    fake_output = {
        "events": [{
            "event_id": "x",
            "summary": "x", "section": "Confirmed",
            "member_rows": [8, 18], "primary_row": 8,
            "confidence": 0.5, "reasoning": "y",
        }],
    }
    fake = _FakeClient(fake_output)
    res = cluster_group(brand="VinFast", articles=_articles_pair(),
                        client=fake, confidence_threshold=0.3)
    # Below default but above override → no LOWCONF prefix
    assert not res.events[0].event_id.startswith("LOWCONF__")
