"""Coverage for news_agent.core.press_verify (Layer 3 Phase B).

Mock both HTTP (httpx MockTransport) and LLM (fake client object).
No real network, no API key required. Real-world smoke test lives
in scripts/_layer3_smoke.py (Phase E).
"""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest

from news_agent.core.press_search import PressCandidate
from news_agent.core.press_verify import (
    DEFAULT_ACCEPTANCE_THRESHOLD,
    VerificationResult,
    _date_in_window,
    _extract_head_fields,
    _parse_iso_date,
    verify_candidates,
    verify_press_candidate,
)


# ── Pure helpers ────────────────────────────────────────────────────

def test_parse_iso_date_valid() -> None:
    dt = _parse_iso_date("2026-05-15T10:00:00+00:00")
    assert dt is not None and dt.year == 2026


def test_parse_iso_date_z_suffix() -> None:
    dt = _parse_iso_date("2026-05-15T10:00:00Z")
    assert dt is not None


def test_parse_iso_date_invalid_returns_none() -> None:
    assert _parse_iso_date("") is None
    assert _parse_iso_date("not a date") is None
    assert _parse_iso_date(None) is None  # type: ignore


def test_date_in_window_yes() -> None:
    ex = datetime(2026, 5, 15, tzinfo=timezone.utc)
    pg = datetime(2026, 5, 16, tzinfo=timezone.utc)
    assert _date_in_window(pg, ex, window_days=3) is True


def test_date_in_window_no() -> None:
    ex = datetime(2026, 5, 15, tzinfo=timezone.utc)
    pg = datetime(2026, 5, 25, tzinfo=timezone.utc)
    assert _date_in_window(pg, ex, window_days=3) is False


def test_date_in_window_missing_extracted() -> None:
    """Missing date → None (unknown, don't reject)."""
    ex = datetime(2026, 5, 15, tzinfo=timezone.utc)
    assert _date_in_window(None, ex, window_days=3) is None


def test_date_in_window_naive_dates_handled() -> None:
    """Naive datetimes get tagged as UTC instead of raising."""
    ex = datetime(2026, 5, 15)
    pg = datetime(2026, 5, 16)
    assert _date_in_window(pg, ex, window_days=3) is True


# ── HTML head extraction ────────────────────────────────────────────

def test_extract_title_only() -> None:
    html = "<html><head><title>Hello world</title></head></html>"
    f = _extract_head_fields(html)
    assert f["title"] == "Hello world"
    assert f["og_title"] == ""


def test_extract_og_fields() -> None:
    html = """
    <html><head>
      <title>Page</title>
      <meta property="og:title" content="Mercedes-AMG GT 4-Door reveal">
      <meta property="og:description" content="Tri-motor 1169hp">
      <meta property="og:type" content="article">
      <meta property="article:published_time" content="2026-05-15T10:00:00+00:00">
    </head></html>
    """
    f = _extract_head_fields(html)
    assert f["og_title"].startswith("Mercedes-AMG GT")
    assert "1169hp" in f["og_description"]
    assert f["og_type"] == "article"
    assert f["article_published_time"].startswith("2026-05-15")


def test_extract_handles_single_quotes() -> None:
    html = """<meta property='og:title' content='Single-quoted'>"""
    f = _extract_head_fields(html)
    assert f["og_title"] == "Single-quoted"


def test_extract_handles_entities() -> None:
    html = """<title>BMW &amp; Audi compared</title>"""
    f = _extract_head_fields(html)
    assert f["title"] == "BMW & Audi compared"


def test_extract_robust_on_malformed_html() -> None:
    """Real-world HTML often has missing close tags; regex must
    survive without raising."""
    html = "<title>partial title<meta property='og:title' content='x'"
    f = _extract_head_fields(html)
    assert isinstance(f["title"], str)


# ── Mock infrastructure ─────────────────────────────────────────────

class _FakeUsage:
    def __init__(self, in_tok=300, out_tok=80):
        self.input_tokens = in_tok
        self.output_tokens = out_tok


class _FakeToolUseBlock:
    def __init__(self, tool_input: dict):
        self.type = "tool_use"
        self.input = tool_input


class _FakeLLMResponse:
    def __init__(self, tool_input: dict, in_tok=300, out_tok=80):
        self.content = [_FakeToolUseBlock(tool_input)]
        self.usage = _FakeUsage(in_tok, out_tok)


class _FakeMessages:
    def __init__(self, plan):
        self.plan = plan
        self.calls = []

    def create(self, **kw):
        self.calls.append(kw)
        if isinstance(self.plan, Exception):
            raise self.plan
        return _FakeLLMResponse(self.plan)


class _FakeAnthropicClient:
    def __init__(self, judge_output_or_error):
        self.messages = _FakeMessages(judge_output_or_error)


def _make_http_client(handler) -> httpx.Client:
    transport = httpx.MockTransport(handler)
    return httpx.Client(transport=transport)


def _candidate(url="https://x.example/article",
                title="Article title") -> PressCandidate:
    return PressCandidate(
        url=url, title=title, snippet="snippet",
        domain="x.example", published_at_iso="",
    )


# ── verify_press_candidate ──────────────────────────────────────────

def test_verify_happy_path_match() -> None:
    """Live URL → head with og fields → LLM says match → accept."""
    def http_handler(req):
        body = """
        <html><head>
          <title>Mercedes-AMG GT 4-Door reveal</title>
          <meta property="og:title" content="Mercedes-AMG GT 4-Door tri-motor">
          <meta property="og:description" content="Official reveal 2026-05-15">
          <meta property="article:published_time" content="2026-05-15T10:00:00+00:00">
        </head></html>
        """
        return httpx.Response(200, text=body,
                               headers={"Content-Type": "text/html"})
    http = _make_http_client(http_handler)
    llm = _FakeAnthropicClient({
        "match": True,
        "confidence": 0.95,
        "reason": "Page describes the AMG GT 4-Door reveal on the same date.",
    })
    res = verify_press_candidate(
        _candidate(),
        event_summary="Mercedes-AMG GT 4-Door reveal with tri-motor",
        expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert res.match is True
    assert res.confidence == 0.95
    assert res.fetched_status == 200
    assert res.error == ""
    assert res.cost_usd > 0
    assert res.extracted_date_iso.startswith("2026-05-15")
    assert res.date_in_window is True


def test_verify_dead_url_rejects_without_llm() -> None:
    def handler(req): return httpx.Response(404, text="not found")
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": True, "confidence": 1.0,
                                 "reason": "won't be called"})
    res = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert res.match is False
    assert res.fetched_status == 404
    assert res.error == "http_404"
    assert res.cost_usd == 0  # LLM never called
    assert len(llm.messages.calls) == 0


def test_verify_timeout_returns_error() -> None:
    def handler(req): raise httpx.TimeoutException("slow")
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": True, "confidence": 1.0,
                                 "reason": "x"})
    res = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert res.match is False
    assert res.error == "timeout"
    assert res.cost_usd == 0


def test_verify_date_out_of_window_cheap_reject() -> None:
    """If page date is in head AND outside window, reject without LLM."""
    def handler(req):
        body = """
        <html><head>
          <meta property="article:published_time" content="2024-01-01T00:00:00+00:00">
        </head></html>
        """
        return httpx.Response(200, text=body)
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": True, "confidence": 1.0,
                                 "reason": "won't be called"})
    res = verify_press_candidate(
        _candidate(),
        event_summary="recent event",
        expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert res.match is False
    assert res.date_in_window is False
    assert "date_out_of_window" in res.reason
    assert res.cost_usd == 0
    assert len(llm.messages.calls) == 0


def test_verify_missing_date_proceeds_to_llm() -> None:
    """If page has no date metadata, fall through to LLM judge."""
    def handler(req):
        return httpx.Response(200, text="<html><head><title>x</title></head></html>")
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": True, "confidence": 0.85,
                                 "reason": "ok"})
    res = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert res.date_in_window is None
    assert res.match is True
    assert len(llm.messages.calls) == 1


def test_verify_llm_below_threshold_rejected() -> None:
    """LLM says match=True but confidence too low → reject."""
    def handler(req):
        return httpx.Response(200, text="<html><title>x</title></html>")
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": True, "confidence": 0.5,
                                 "reason": "uncertain"})
    res = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert res.match is False  # below threshold (0.70)
    assert res.confidence == 0.5
    assert res.cost_usd > 0  # but LLM was called


def test_verify_llm_says_no_match_rejected() -> None:
    def handler(req):
        return httpx.Response(200, text="<html><title>x</title></html>")
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": False, "confidence": 0.9,
                                 "reason": "wrong model"})
    res = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert res.match is False
    assert res.reason == "wrong model"


def test_verify_llm_error_doesnt_raise() -> None:
    def handler(req):
        return httpx.Response(200, text="<html><title>x</title></html>")
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient(RuntimeError("API down"))
    res = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert res.match is False
    assert "llm_error" in res.error
    assert "RuntimeError" in res.error


def test_verify_no_llm_client_returns_error() -> None:
    def handler(req):
        return httpx.Response(200, text="<html><title>x</title></html>")
    http = _make_http_client(handler)
    res = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=None,
    )
    assert res.match is False
    assert res.error == "no_llm_client"
    assert res.fetched_status == 200  # HTTP did happen


def test_verify_acceptance_threshold_can_be_overridden() -> None:
    """Caller can loosen or tighten the LLM confidence floor."""
    def handler(req):
        return httpx.Response(200, text="<html><title>x</title></html>")
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": True, "confidence": 0.55,
                                 "reason": "ok"})
    # Default 0.70 → reject
    r1 = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert r1.match is False
    # Lowered to 0.50 → accept
    r2 = verify_press_candidate(
        _candidate(),
        event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
        acceptance_threshold=0.50,
    )
    assert r2.match is True


# ── verify_candidates orchestration ─────────────────────────────────

def test_verify_candidates_stops_at_first_match() -> None:
    """When stop_at_first_match=True, don't burn money on rest."""
    def handler(req):
        return httpx.Response(200, text="<html><title>ok</title></html>")
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": True, "confidence": 0.9,
                                 "reason": "ok"})
    cands = [_candidate(url=f"https://x.example/{i}") for i in range(5)]
    accepted, attempts = verify_candidates(
        cands, event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert accepted is not None
    # Only first was tried (or at least, stopped early)
    assert len(attempts) == 1


def test_verify_candidates_all_fail_returns_none() -> None:
    def handler(req): return httpx.Response(404)
    http = _make_http_client(handler)
    llm = _FakeAnthropicClient({"match": True, "confidence": 1.0,
                                 "reason": "won't be called"})
    cands = [_candidate(url=f"https://x.example/{i}") for i in range(3)]
    accepted, attempts = verify_candidates(
        cands, event_summary="x", expected_date_iso="2026-05-15",
        http_client=http, llm_client=llm,
    )
    assert accepted is None
    assert len(attempts) == 3
    assert all(a.match is False for a in attempts)


def test_verify_candidates_empty_input() -> None:
    accepted, attempts = verify_candidates(
        [], event_summary="x", expected_date_iso="2026-05-15",
    )
    assert accepted is None
    assert attempts == []
