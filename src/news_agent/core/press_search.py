"""Layer 3 — active press-release retrieval.

When an LLM-as-editor cluster lacks an authoritative source, this
module searches OEM / regulator / industry domains for the canonical
press release of the event and returns ranked candidates. The
verification step (Layer 4) lives in press_verify.py.

Design doc: docs/LAYER_3_DESIGN.md

Phase A (this file): data classes + Brave Search API client + SQLite
cache. Search-only — verification ships as Phase B.

Architecture notes
------------------
  • Pure functions where possible — Brave client is injected so tests
    can mock without env / network.
  • Graceful failure baked in at every level: HTTP timeout, rate
    limit, empty results, malformed JSON — all return empty results
    with a non-empty error string. Never raise to caller.
  • Cache keys are stable hashes over normalised query inputs so
    rerunning the same prog hits cache at $0.
  • get_brand_domains is used to derive target_domains when caller
    doesn't supply them — keeps Layer 3 honest to Layer 1 registry.

Phase A guarantees
------------------
  • find_press_release returns ≤ max_results candidates.
  • If BRAVE_SEARCH_API_KEY is missing OR search_client is None,
    returns empty result with error="no search client configured" —
    pipeline continues in degraded mode.
  • Cache hits never call the network.
  • All HTTP / parsing errors are caught and surfaced via .error.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import httpx

from news_agent.core.brand_canonical import get_brand_domains

BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
DEFAULT_TTL_HOURS = 24
DEFAULT_TIMEOUT_S = 10.0
DEFAULT_DATE_WINDOW_DAYS = 3


# ── Data classes ────────────────────────────────────────────────────

@dataclass(frozen=True)
class SearchQuery:
    """Inputs to a press search. Normalised so equivalent searches
    share a cache key regardless of input formatting."""
    brand_canonical: str
    event_summary: str
    expected_date_iso: str          # ISO 8601 yyyy-mm-dd
    model: str = ""
    event_type: str = ""
    target_domains: tuple[str, ...] = ()
    date_window_days: int = DEFAULT_DATE_WINDOW_DAYS

    def cache_key(self) -> str:
        """Stable sha1 over normalised fields. Identical inputs
        produce identical key across processes / time."""
        normalised = {
            "brand": self.brand_canonical.strip().lower(),
            "model": self.model.strip().lower(),
            "event_type": self.event_type.strip().lower(),
            # Truncate summary to first 200 chars so minor wording
            # tweaks don't bust cache.
            "summary": self.event_summary.strip().lower()[:200],
            "date": self.expected_date_iso[:10],
            "domains": sorted(d.lower() for d in self.target_domains),
            "window": self.date_window_days,
        }
        blob = json.dumps(normalised, sort_keys=True, ensure_ascii=False)
        return hashlib.sha1(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PressCandidate:
    """One search result, ready for verification or use."""
    url: str
    title: str
    snippet: str
    domain: str
    published_at_iso: str = ""      # Brave's page_age if available
    source_engine: str = "brave"

    @classmethod
    def from_brave_result(cls, r: dict) -> "PressCandidate":
        url = r.get("url", "")
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc.lower().lstrip("www.")
        except Exception:
            domain = ""
        return cls(
            url=url,
            title=r.get("title", "")[:300],
            snippet=r.get("description", "")[:500],
            domain=domain,
            published_at_iso=r.get("page_age", "") or "",
            source_engine="brave",
        )


@dataclass
class SearchResult:
    """Return type of find_press_release. Always populated, even on
    error (candidates=[], error=...)."""
    query: SearchQuery
    candidates: list[PressCandidate] = field(default_factory=list)
    cache_hit: bool = False
    api_calls: int = 0
    elapsed_s: float = 0.0
    error: str = ""


# ── Brave Search client ─────────────────────────────────────────────

class BraveSearchClient:
    """HTTP client for Brave Search API. Inject httpx-mock in tests."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        endpoint: str = BRAVE_ENDPOINT,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("BRAVE_SEARCH_API_KEY", "")
        self.endpoint = endpoint
        self.timeout_s = timeout_s
        self._http = http_client  # for test injection

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def search(
        self,
        query: str,
        *,
        count: int = 10,
        freshness: str | None = None,
        country: str = "ALL",
    ) -> tuple[list[dict], str]:
        """Run one Brave Web Search. Returns (results, error_string).

        ``freshness`` accepts Brave's syntax: 'pd' / 'pw' / 'pm' /
        'py' or an explicit 'YYYY-MM-DDtoYYYY-MM-DD' range.
        """
        if not self.is_configured():
            return [], "BRAVE_SEARCH_API_KEY not set"
        params: dict[str, Any] = {
            "q": query, "count": count, "country": country,
        }
        if freshness:
            params["freshness"] = freshness
        headers = {
            "Accept": "application/json",
            "X-Subscription-Token": self.api_key,
        }
        client = self._http or httpx.Client(timeout=self.timeout_s)
        try:
            resp = client.get(self.endpoint, params=params, headers=headers)
            if resp.status_code == 429:
                return [], "rate_limited"
            if resp.status_code >= 400:
                return [], f"http_{resp.status_code}"
            data = resp.json()
        except httpx.TimeoutException:
            return [], "timeout"
        except httpx.RequestError as e:
            return [], f"network: {type(e).__name__}"
        except (json.JSONDecodeError, ValueError):
            return [], "bad_json"
        finally:
            if self._http is None:
                try:
                    client.close()
                except Exception:
                    pass
        web = (data.get("web") or {}).get("results") or []
        return web, ""


# ── Cache ───────────────────────────────────────────────────────────

class PressSearchCache:
    """SQLite-backed cache for Layer 3 results.

    Reuses the same SQLite file as DedupStore (table
    ``press_search_cache`` defined in the shared SCHEMA). Independent
    instance so tests can use a temp DB.
    """

    def __init__(
        self,
        db_path: Path,
        *,
        ttl_hours: int = DEFAULT_TTL_HOURS,
    ) -> None:
        self.db_path = db_path
        self.ttl_hours = ttl_hours
        self._ensure_table()

    def _ensure_table(self) -> None:
        with self._conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS press_search_cache (
                    query_hash      TEXT PRIMARY KEY,
                    query_json      TEXT NOT NULL,
                    results_json    TEXT NOT NULL,
                    verified_url    TEXT,
                    verified_conf   REAL,
                    cached_at       TEXT NOT NULL,
                    ttl_hours       INTEGER NOT NULL DEFAULT 24
                );
                CREATE INDEX IF NOT EXISTS idx_press_cache_cached_at
                    ON press_search_cache(cached_at);
            """)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def get(self, query: SearchQuery) -> list[PressCandidate] | None:
        """Return cached candidates if fresh, else None."""
        key = query.cache_key()
        with self._conn() as c:
            row = c.execute(
                "SELECT results_json, cached_at, ttl_hours "
                "FROM press_search_cache WHERE query_hash = ?",
                (key,),
            ).fetchone()
        if row is None:
            return None
        try:
            cached_at = datetime.fromisoformat(row["cached_at"])
        except (ValueError, TypeError):
            return None
        ttl = timedelta(hours=row["ttl_hours"] or DEFAULT_TTL_HOURS)
        if datetime.now(timezone.utc) - cached_at > ttl:
            return None
        try:
            raw = json.loads(row["results_json"])
        except (ValueError, TypeError):
            return None
        return [PressCandidate(**r) for r in raw if isinstance(r, dict)]

    def put(
        self,
        query: SearchQuery,
        candidates: list[PressCandidate],
    ) -> None:
        key = query.cache_key()
        now_iso = datetime.now(timezone.utc).isoformat()
        results_json = json.dumps(
            [asdict(c) for c in candidates], ensure_ascii=False,
        )
        query_json = json.dumps(asdict(query), ensure_ascii=False)
        with self._conn() as c:
            c.execute(
                "INSERT INTO press_search_cache "
                "(query_hash, query_json, results_json, "
                " cached_at, ttl_hours) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(query_hash) DO UPDATE SET "
                "  results_json = excluded.results_json, "
                "  cached_at = excluded.cached_at, "
                "  ttl_hours = excluded.ttl_hours",
                (key, query_json, results_json, now_iso, self.ttl_hours),
            )

    def purge_expired(self) -> int:
        """Delete expired rows. Returns count deleted."""
        cutoff_default = (
            datetime.now(timezone.utc) - timedelta(hours=DEFAULT_TTL_HOURS)
        ).isoformat()
        with self._conn() as c:
            cur = c.execute(
                "DELETE FROM press_search_cache "
                "WHERE cached_at < ?",
                (cutoff_default,),
            )
            return cur.rowcount


# ── High-level orchestration ────────────────────────────────────────

def _build_brave_query(
    *,
    event_summary: str,
    target_domain: str,
    model: str = "",
) -> str:
    """Construct a Brave query targeting one domain. Wraps key terms
    in quotes when they're multi-word for tighter matching.

    Model gets the most weight (quoted exact match); event_summary
    contributes context terms. If model is empty, fall back to
    summary alone.
    """
    parts: list[str] = []
    if target_domain:
        parts.append(f"site:{target_domain}")
    if model and len(model.split()) > 1:
        parts.append(f'"{model}"')
    elif model:
        parts.append(model)
    # Pull 2-3 substantive nouns/proper nouns from summary, skipping
    # filler. Simple heuristic — first 8 tokens stripped of stopwords.
    stop = {"the", "a", "an", "in", "on", "of", "to", "for", "and",
            "is", "was", "be", "by", "with", "as", "at", "from",
            "this", "that", "it", "its", "into", "via"}
    tokens = [t for t in event_summary.split()[:12]
              if t.lower() not in stop and len(t) > 2]
    if tokens and not model:
        parts.append(" ".join(tokens[:6]))
    return " ".join(parts).strip()


def _brave_freshness_for_date(
    expected: datetime, window_days: int,
) -> str:
    """Convert (expected_date, window) into Brave's freshness param."""
    lo = (expected - timedelta(days=window_days)).strftime("%Y-%m-%d")
    hi = (expected + timedelta(days=window_days)).strftime("%Y-%m-%d")
    return f"{lo}to{hi}"


def find_press_release(
    *,
    brand_canonical: str,
    event_summary: str,
    expected_date_iso: str,
    model: str = "",
    event_type: str = "",
    target_domains: list[str] | None = None,
    max_results: int = 3,
    cache: PressSearchCache | None = None,
    search_client: BraveSearchClient | None = None,
    date_window_days: int = DEFAULT_DATE_WINDOW_DAYS,
) -> SearchResult:
    """Find candidate press-release URLs for an event.

    1. Resolve target_domains from brand registry if not provided.
    2. Check cache by query key; return cached candidates if fresh.
    3. Iterate target domains, run Brave site-filtered search.
    4. Aggregate up to max_results candidates, dedupe by URL.
    5. Store in cache; return SearchResult.
    """
    # Resolve target_domains from registry if caller didn't supply.
    if target_domains is None:
        target_domains = []
    if not target_domains and brand_canonical:
        target_domains = get_brand_domains(brand_canonical)

    query = SearchQuery(
        brand_canonical=brand_canonical,
        event_summary=event_summary,
        expected_date_iso=expected_date_iso,
        model=model,
        event_type=event_type,
        target_domains=tuple(target_domains or ()),
        date_window_days=date_window_days,
    )

    t0 = time.time()

    # Cache lookup
    if cache is not None:
        cached = cache.get(query)
        if cached is not None:
            return SearchResult(
                query=query, candidates=cached[:max_results],
                cache_hit=True, api_calls=0,
                elapsed_s=time.time() - t0,
            )

    if search_client is None or not search_client.is_configured():
        return SearchResult(
            query=query, candidates=[], cache_hit=False,
            api_calls=0, elapsed_s=time.time() - t0,
            error="no search client configured",
        )

    # Parse expected date for freshness filter
    try:
        expected_dt = datetime.fromisoformat(
            expected_date_iso.replace("Z", "+00:00")
        )
        if expected_dt.tzinfo is None:
            expected_dt = expected_dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        expected_dt = datetime.now(timezone.utc)
    freshness = _brave_freshness_for_date(expected_dt, date_window_days)

    aggregated: list[PressCandidate] = []
    seen_urls: set[str] = set()
    api_calls = 0
    last_error = ""

    for dom in (target_domains or [""]):
        q = _build_brave_query(
            event_summary=event_summary, target_domain=dom, model=model,
        )
        if not q:
            continue
        raw_results, err = search_client.search(
            q, count=max_results * 2, freshness=freshness,
        )
        api_calls += 1
        if err:
            last_error = err
            continue
        for r in raw_results:
            cand = PressCandidate.from_brave_result(r)
            if not cand.url or cand.url in seen_urls:
                continue
            seen_urls.add(cand.url)
            aggregated.append(cand)
            if len(aggregated) >= max_results:
                break
        if len(aggregated) >= max_results:
            break

    # Store in cache (even empty results — saves repeat zero-hit queries)
    if cache is not None and not last_error:
        cache.put(query, aggregated)

    return SearchResult(
        query=query,
        candidates=aggregated[:max_results],
        cache_hit=False,
        api_calls=api_calls,
        elapsed_s=time.time() - t0,
        error=last_error if not aggregated else "",
    )
