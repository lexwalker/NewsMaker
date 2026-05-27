"""Coverage for news_agent.core.press_search (Layer 3 Phase A).

Strategy: pure-function tests + mocked HTTP via httpx-MockTransport
+ in-memory SQLite cache via tmp_path. No real network, no env keys.

The integration test against real Brave API lives in
scripts/_layer3_smoke.py (Phase E), not here.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from news_agent.core.press_search import (
    BRAVE_ENDPOINT,
    BraveSearchClient,
    PressCandidate,
    PressSearchCache,
    SearchQuery,
    SearchResult,
    _brave_freshness_for_date,
    _build_brave_query,
    find_press_release,
)


# ── SearchQuery cache key stability ─────────────────────────────────

def test_cache_key_stable_across_instances() -> None:
    q1 = SearchQuery(
        brand_canonical="Mercedes-Benz",
        event_summary="AMG GT 4-Door reveal",
        expected_date_iso="2026-05-15",
        model="AMG GT 4-Door",
        event_type="reveal",
        target_domains=("media.mercedes-benz.com",),
    )
    q2 = SearchQuery(
        brand_canonical="Mercedes-Benz",
        event_summary="AMG GT 4-Door reveal",
        expected_date_iso="2026-05-15",
        model="AMG GT 4-Door",
        event_type="reveal",
        target_domains=("media.mercedes-benz.com",),
    )
    assert q1.cache_key() == q2.cache_key()


def test_cache_key_normalises_case_and_whitespace() -> None:
    q1 = SearchQuery(
        brand_canonical="Mercedes-Benz",
        event_summary="AMG GT Reveal",
        expected_date_iso="2026-05-15",
        target_domains=("Media.Mercedes-Benz.COM",),
    )
    q2 = SearchQuery(
        brand_canonical="mercedes-benz",
        event_summary="amg gt reveal",
        expected_date_iso="2026-05-15",
        target_domains=("media.mercedes-benz.com",),
    )
    assert q1.cache_key() == q2.cache_key()


def test_cache_key_domains_order_independent() -> None:
    q1 = SearchQuery(
        brand_canonical="X", event_summary="y",
        expected_date_iso="2026-01-01",
        target_domains=("a.com", "b.com"),
    )
    q2 = SearchQuery(
        brand_canonical="X", event_summary="y",
        expected_date_iso="2026-01-01",
        target_domains=("b.com", "a.com"),
    )
    assert q1.cache_key() == q2.cache_key()


def test_cache_key_differs_on_real_change() -> None:
    q1 = SearchQuery(brand_canonical="BMW", event_summary="M5",
                      expected_date_iso="2026-01-01")
    q2 = SearchQuery(brand_canonical="Audi", event_summary="M5",
                      expected_date_iso="2026-01-01")
    assert q1.cache_key() != q2.cache_key()


# ── PressCandidate.from_brave_result ────────────────────────────────

def test_press_candidate_from_brave_result() -> None:
    raw = {
        "url": "https://www.media.mercedes-benz.com/article/abc",
        "title": "Mercedes-AMG GT 4-Door reveal",
        "description": "Official press release...",
        "page_age": "2026-05-15T10:00:00+00:00",
    }
    c = PressCandidate.from_brave_result(raw)
    assert c.url == raw["url"]
    assert c.domain == "media.mercedes-benz.com"
    assert c.title.startswith("Mercedes-AMG")
    assert c.published_at_iso == "2026-05-15T10:00:00+00:00"
    assert c.source_engine == "brave"


def test_press_candidate_empty_safe() -> None:
    c = PressCandidate.from_brave_result({})
    assert c.url == ""
    assert c.domain == ""


# ── Brave query builder ─────────────────────────────────────────────

def test_build_query_with_domain_and_model() -> None:
    q = _build_brave_query(
        event_summary="GT 4-Door tri-motor reveal in Stuttgart",
        target_domain="media.mercedes-benz.com",
        model="GT 4-Door",
    )
    assert "site:media.mercedes-benz.com" in q
    assert '"GT 4-Door"' in q


def test_build_query_no_domain_uses_summary() -> None:
    q = _build_brave_query(
        event_summary="Honda Pilot recall safety affecting 54000",
        target_domain="",
    )
    # No site filter, falls back to summary tokens
    assert "site:" not in q
    assert "Honda" in q or "Pilot" in q


def test_build_query_strips_stopwords() -> None:
    q = _build_brave_query(
        event_summary="The new Tesla in the US for the recall",
        target_domain="",
    )
    # Stop words filtered out
    assert "the" not in q.lower().split()


# ── Freshness conversion ────────────────────────────────────────────

def test_freshness_range_format() -> None:
    expected = datetime(2026, 5, 15, tzinfo=timezone.utc)
    f = _brave_freshness_for_date(expected, window_days=3)
    assert f == "2026-05-12to2026-05-18"


def test_freshness_window_zero_collapses() -> None:
    expected = datetime(2026, 5, 15, tzinfo=timezone.utc)
    f = _brave_freshness_for_date(expected, window_days=0)
    assert f == "2026-05-15to2026-05-15"


# ── BraveSearchClient: mocked HTTP ──────────────────────────────────

def _make_mock_client(handler) -> BraveSearchClient:
    transport = httpx.MockTransport(handler)
    http = httpx.Client(transport=transport)
    return BraveSearchClient(api_key="test_key", http_client=http)


def test_brave_client_happy_path() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/res/v1/web/search"
        assert request.headers["X-Subscription-Token"] == "test_key"
        body = {
            "web": {
                "results": [
                    {"url": "https://a.example/1", "title": "T1",
                     "description": "S1", "page_age": "2026-05-15"},
                    {"url": "https://b.example/2", "title": "T2",
                     "description": "S2"},
                ]
            }
        }
        return httpx.Response(200, json=body)
    client = _make_mock_client(handler)
    results, err = client.search("test query")
    assert err == ""
    assert len(results) == 2
    assert results[0]["url"] == "https://a.example/1"


def test_brave_client_no_key_returns_error() -> None:
    client = BraveSearchClient(api_key="")
    results, err = client.search("anything")
    assert results == []
    assert "BRAVE_SEARCH_API_KEY" in err


def test_brave_client_handles_429() -> None:
    def handler(req): return httpx.Response(429, json={"error": "rate"})
    client = _make_mock_client(handler)
    results, err = client.search("x")
    assert results == []
    assert err == "rate_limited"


def test_brave_client_handles_500() -> None:
    def handler(req): return httpx.Response(500, text="oops")
    client = _make_mock_client(handler)
    results, err = client.search("x")
    assert results == []
    assert err == "http_500"


def test_brave_client_handles_timeout() -> None:
    def handler(req):
        raise httpx.TimeoutException("slow")
    client = _make_mock_client(handler)
    results, err = client.search("x")
    assert results == []
    assert err == "timeout"


def test_brave_client_handles_bad_json() -> None:
    def handler(req): return httpx.Response(200, text="not json at all")
    client = _make_mock_client(handler)
    results, err = client.search("x")
    assert results == []
    assert err == "bad_json"


def test_brave_client_handles_empty_results() -> None:
    def handler(req):
        return httpx.Response(200, json={"web": {"results": []}})
    client = _make_mock_client(handler)
    results, err = client.search("x")
    assert results == []
    assert err == ""  # no error, just empty


def test_brave_client_freshness_passed_through() -> None:
    captured = {}
    def handler(req):
        captured["params"] = dict(req.url.params)
        return httpx.Response(200, json={"web": {"results": []}})
    client = _make_mock_client(handler)
    client.search("x", freshness="2026-05-12to2026-05-18")
    assert captured["params"].get("freshness") == "2026-05-12to2026-05-18"


# ── PressSearchCache ────────────────────────────────────────────────

def _make_query() -> SearchQuery:
    return SearchQuery(
        brand_canonical="BMW", event_summary="M5 spy shots",
        expected_date_iso="2026-05-15", model="M5",
        target_domains=("press.bmwgroup.com",),
    )


def _make_candidates() -> list[PressCandidate]:
    return [
        PressCandidate(url="https://press.bmwgroup.com/x", title="t1",
                       snippet="s1", domain="press.bmwgroup.com"),
        PressCandidate(url="https://press.bmwgroup.com/y", title="t2",
                       snippet="s2", domain="press.bmwgroup.com"),
    ]


def test_cache_miss_returns_none(tmp_path) -> None:
    cache = PressSearchCache(tmp_path / "p.sqlite")
    assert cache.get(_make_query()) is None


def test_cache_put_then_get(tmp_path) -> None:
    cache = PressSearchCache(tmp_path / "p.sqlite")
    q = _make_query()
    cs = _make_candidates()
    cache.put(q, cs)
    out = cache.get(q)
    assert out is not None
    assert len(out) == 2
    assert out[0].url == "https://press.bmwgroup.com/x"


def test_cache_respects_ttl(tmp_path) -> None:
    cache = PressSearchCache(tmp_path / "p.sqlite", ttl_hours=1)
    q = _make_query()
    cache.put(q, _make_candidates())
    # Manually backdate the cached_at to expire
    with cache._conn() as c:
        old_ts = (datetime.now(timezone.utc)
                   - timedelta(hours=2)).isoformat()
        c.execute(
            "UPDATE press_search_cache SET cached_at = ?",
            (old_ts,),
        )
    assert cache.get(q) is None


def test_cache_upsert_overwrites(tmp_path) -> None:
    cache = PressSearchCache(tmp_path / "p.sqlite")
    q = _make_query()
    cache.put(q, _make_candidates()[:1])
    cache.put(q, _make_candidates())  # overwrite with both
    out = cache.get(q)
    assert len(out) == 2


def test_cache_purge_expired(tmp_path) -> None:
    cache = PressSearchCache(tmp_path / "p.sqlite", ttl_hours=1)
    q = _make_query()
    cache.put(q, _make_candidates())
    with cache._conn() as c:
        old_ts = (datetime.now(timezone.utc)
                   - timedelta(hours=48)).isoformat()
        c.execute("UPDATE press_search_cache SET cached_at = ?",
                  (old_ts,))
    n = cache.purge_expired()
    assert n >= 1


# ── find_press_release orchestration ───────────────────────────────

def test_find_no_search_client_returns_error(tmp_path) -> None:
    cache = PressSearchCache(tmp_path / "p.sqlite")
    res = find_press_release(
        brand_canonical="BMW", event_summary="M5",
        expected_date_iso="2026-05-15",
        target_domains=["press.bmwgroup.com"],
        cache=cache, search_client=None,
    )
    assert res.candidates == []
    assert "no search client" in res.error
    assert res.api_calls == 0


def test_find_hits_cache(tmp_path) -> None:
    cache = PressSearchCache(tmp_path / "p.sqlite")
    q = SearchQuery(
        brand_canonical="BMW", event_summary="M5",
        expected_date_iso="2026-05-15",
        target_domains=("press.bmwgroup.com",),
    )
    cache.put(q, _make_candidates())
    res = find_press_release(
        brand_canonical="BMW", event_summary="M5",
        expected_date_iso="2026-05-15",
        target_domains=["press.bmwgroup.com"],
        cache=cache, search_client=None,
    )
    assert res.cache_hit is True
    assert len(res.candidates) == 2
    assert res.api_calls == 0


def test_find_aggregates_and_dedupes(tmp_path) -> None:
    """Multi-domain search merges results, drops duplicate URLs."""
    call_count = [0]
    def handler(req):
        call_count[0] += 1
        return httpx.Response(200, json={
            "web": {"results": [
                {"url": "https://a.example/1", "title": "T1",
                 "description": "S1"},
                # duplicate across domain calls
                {"url": "https://a.example/dup", "title": "Dup",
                 "description": "S"},
            ]}
        })
    client = _make_mock_client(handler)
    cache = PressSearchCache(tmp_path / "p.sqlite")
    res = find_press_release(
        brand_canonical="BMW", event_summary="M5",
        expected_date_iso="2026-05-15",
        target_domains=["a.example", "b.example"],
        max_results=5,
        cache=cache, search_client=client,
    )
    # 2 domains queried, but duplicate URLs collapsed
    assert res.cache_hit is False
    assert res.api_calls >= 1
    urls = {c.url for c in res.candidates}
    assert "https://a.example/1" in urls
    # The duplicate is only there once
    assert len(res.candidates) == len({c.url for c in res.candidates})


def test_find_writes_cache_on_success(tmp_path) -> None:
    def handler(req):
        return httpx.Response(200, json={"web": {"results": [
            {"url": "https://x.example/1", "title": "T",
             "description": "S"},
        ]}})
    client = _make_mock_client(handler)
    cache = PressSearchCache(tmp_path / "p.sqlite")
    res1 = find_press_release(
        brand_canonical="BMW", event_summary="M5",
        expected_date_iso="2026-05-15",
        target_domains=["x.example"], cache=cache, search_client=client,
    )
    assert len(res1.candidates) == 1
    # Second call hits cache (no network)
    res2 = find_press_release(
        brand_canonical="BMW", event_summary="M5",
        expected_date_iso="2026-05-15",
        target_domains=["x.example"], cache=cache, search_client=client,
    )
    assert res2.cache_hit is True
    assert res2.api_calls == 0


def test_find_does_not_cache_errors(tmp_path) -> None:
    """If search errors out (timeout, rate-limit), don't poison the
    cache with empty results. Next call should re-try."""
    state = {"n": 0}
    def handler(req):
        state["n"] += 1
        if state["n"] == 1:
            raise httpx.TimeoutException("first time fails")
        return httpx.Response(200, json={"web": {"results": [
            {"url": "https://x.example/1", "title": "T",
             "description": "S"},
        ]}})
    client = _make_mock_client(handler)
    cache = PressSearchCache(tmp_path / "p.sqlite")
    res1 = find_press_release(
        brand_canonical="BMW", event_summary="M5",
        expected_date_iso="2026-05-15",
        target_domains=["x.example"], cache=cache, search_client=client,
    )
    assert res1.candidates == []
    assert res1.error == "timeout"
    res2 = find_press_release(
        brand_canonical="BMW", event_summary="M5",
        expected_date_iso="2026-05-15",
        target_domains=["x.example"], cache=cache, search_client=client,
    )
    # Second call should retry (not hit empty cache)
    assert res2.cache_hit is False
    assert len(res2.candidates) == 1


def test_find_uses_brand_registry_when_no_domains(tmp_path) -> None:
    """If caller doesn't supply target_domains, fall back to brand
    registry (Layer 1) — keeps the two layers honest."""
    captured = {"queries": []}
    def handler(req):
        q = dict(req.url.params).get("q", "")
        captured["queries"].append(q)
        return httpx.Response(200, json={"web": {"results": []}})
    client = _make_mock_client(handler)
    cache = PressSearchCache(tmp_path / "p.sqlite")
    res = find_press_release(
        brand_canonical="Audi",  # known brand → registry has audi-mediacenter
        event_summary="A4 spy shots",
        expected_date_iso="2026-05-15",
        cache=cache, search_client=client,
    )
    # Brand registry kicked in — at least one site: query was issued
    assert any("site:audi" in q for q in captured["queries"]) \
        or any("audi-mediacenter" in q for q in captured["queries"])
    assert res.api_calls >= 1
