# Layer 3+4 design — active press-release retrieval & verification

**Status**: design draft (2026-05-27). Spec for next-week implementation.
**Author**: built during v42 production validation loop.

---

## Motivation

The v42 editor-feedback validation closed all the within-batch dup
gaps the editor flagged (4/4 after registry gap-fill). One class of
complaint remains structurally uncatchable by the current
architecture:

> «постили 14.05, https://register.dpma.de/…»
> «постили пресс https://www.media.stellantis.com/…»

Translation: the editor manually checked the canonical OEM/regulator
URL on the open web and wants it as the primary source. Our crawl
either doesn't have that URL at all (Layer 2 coverage gap, partly
fixable by registry expansion) or had it days ago, outside the
current run window (history-aware lookup needed).

**This document describes the missing slice** that makes the system
truly autonomous: instead of waiting for the editor to point at the
right press release, the bot finds it itself, the way a human editor
opens a browser tab to search.

---

## Architecture position in the 6-layer stack

```
Layer 1  Registry              ✓ brand_domains.yaml + source_priority
Layer 2  Discovery             ✓ ~338 crawl sources + DedupStore history
Layer 3  Active retrieval      ◀ THIS DOC
Layer 4  Verification          ◀ THIS DOC
Layer 5  Editorial reasoning   ✓ LLM-as-editor (validated on v41+v42)
Layer 6  Feedback loop         ✓ sync_editor_feedback + validation script
```

Layers 3+4 sit between cluster formation and the LLM-as-editor reasoning
pass. They give that pass a richer view of the world than what the
local crawl alone provides.

---

## Trigger conditions (when Layer 3 activates)

Hybrid rule + LLM-driven:

### Rule-based pre-trigger (deterministic, cheap)
In `_apply_llm_editor_pass`, BEFORE calling `cluster_group()`, for
each multi-article brand group:

```
trigger_search = (
    not any(domain_tier(m.domain, brand) ≤ 2 for m in members)
    AND brand != "_unknown"
    AND brand is in our REGISTRY
)
```

That is: **call search when the cluster has zero OEM/regulator/press
sources of its own**. This is the most common "wrong primary" pattern.

### LLM-driven post-trigger (flexible, smart)
Inside `cluster_group`'s system prompt, give the LLM a tool
`find_press_release`. The LLM can invoke it during reasoning if it
sees a Cluster like "spy shots / leak / repost" but suspects an
official press release exists.

Cap at 3 LLM-initiated searches per cluster_group call to bound cost.

---

## Search backend

**Recommended: Brave Search API.**

| Backend | Pros | Cons | Cost |
|---|---|---|---|
| Brave Search | AI-focused, simple HTTP API, site: filter, EU-friendly | Less Google-comprehensive | 2K free/mo, then $3/1K |
| Serper.dev | Google quality, fast | More expensive | $50/mo for 10K |
| Google CSE | Official Google | Quota tedious | 100 free/day, $5/1K |
| Bing Search | Microsoft-comprehensive | Azure setup overhead | $4/1K |
| DuckDuckGo | Free | Rate-limited, unofficial | — |

Expected volume: 10-20 searches per prog × 3-5 progs/week ≈ **200-400
searches/month**. Brave free tier covers it; first paid month ~$1-2.

Set `BRAVE_SEARCH_API_KEY` in `.env`.

---

## Tool schema (LLM-facing)

```python
FIND_PRESS_TOOL = {
    "name": "find_press_release",
    "description": (
        "Search OEM/regulator domains for the official press release "
        "of a specific event. Only call when the cluster lacks an "
        "authoritative source (no member is on a tier-0/1/2 domain). "
        "Returns 0-3 candidate URLs ranked by relevance."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "brand_canonical": {
                "type": "string",
                "description": "Canonical brand name from registry",
            },
            "model": {
                "type": "string",
                "description": "Specific model name (e.g. 'GT 4-Door')",
            },
            "event_type": {
                "type": "string",
                "enum": ["launch", "reveal", "recall", "patent",
                          "financial", "sales_stat", "strategy",
                          "partnership", "other"],
            },
            "event_summary": {
                "type": "string",
                "description": "One-line event description, e.g. "
                               "'BMW M3 CS 2027 reveal with manual gearbox'",
            },
            "expected_date_iso": {
                "type": "string",
                "description": "ISO date when the event likely happened",
            },
            "target_domains": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Domains to site-filter on. If empty, the "
                               "pipeline derives them from the brand "
                               "registry. Example: ['media.mercedes-benz.com', "
                               "'nhtsa.gov'].",
            },
        },
        "required": ["brand_canonical", "event_summary", "expected_date_iso"],
    },
}
```

Pipeline implements the tool. LLM only consumes results.

---

## Pipeline-level search function (Layer 3 implementation)

```python
# src/news_agent/core/press_search.py

def find_press_release(
    *,
    brand_canonical: str,
    event_summary: str,
    expected_date_iso: str,
    model: str = "",
    event_type: str = "",
    target_domains: list[str] | None = None,
    max_results: int = 3,
    cache: PressSearchCache,
    search_client: BraveSearchClient,
) -> list[PressCandidate]:
    """
    1. Resolve target_domains from brand registry if not provided.
    2. Build site-filtered query for each target.
    3. Check cache by query hash; return cached if fresh (<24h).
    4. Issue Brave Search query, filter by date window (±3 days).
    5. Return up to max_results PressCandidate(url, title, snippet, date).

    Errors (timeout, rate limit, network) → return []; never raise.
    """
```

### Query construction
```
For brand=Mercedes-Benz, model=AMG GT, date=2026-05-15:
  Query 1: site:media.mercedes-benz.com "AMG GT" after:2026-05-13 before:2026-05-17
  Query 2: site:group.mercedes-benz.com "AMG GT" after:2026-05-13 before:2026-05-17
  Query 3 (fallback): "Mercedes-AMG GT" press release site:.com after:2026-05-13
```

---

## Verification (Layer 4 implementation)

Search returns 3 candidates. We verify before using.

```python
# src/news_agent/core/press_verify.py

def verify_press_candidate(
    candidate: PressCandidate,
    event_summary: str,
    expected_date: datetime,
    *,
    llm_client,
) -> VerificationResult:
    """
    Pipeline:
      1. HTTP HEAD on candidate.url:
         - dead/4xx → reject
         - redirect → follow once, then check status
         - check Last-Modified / og:date if available
      2. If date metadata exists, must be within ±3 days of expected.
      3. Fetch first 4KB of the page (title + meta + opening).
      4. LLM judge:
         "Does this page describe THIS event?
          event: {event_summary}
          expected_date: {expected_date}
          page excerpt: {excerpt}
          Return: {match: bool, confidence: 0-1, reason: str}"
      5. confidence >= 0.7 → accept; else next candidate.

    Returns VerificationResult(verified_url, confidence, reasoning)
    or VerificationResult(None, 0.0, "all candidates failed").
    """
```

### Why verification is non-negotiable
Without it, LLM might use a search-result URL that turns out to be:
- A 404 (search index stale)
- The wrong event with similar wording
- A paywalled article (will look broken in editor sheet)
- A category-list page, not the press release itself

Verification step is HTTP HEAD + 4KB fetch + 1 LLM call per candidate.
~$0.003 per verification. For 3 candidates ≈ $0.009 per cluster.

---

## Caching

Search results AND verifications cache by query hash.

```sql
CREATE TABLE IF NOT EXISTS press_search_cache (
    query_hash      TEXT PRIMARY KEY,    -- sha1 of normalized query
    query_json      TEXT NOT NULL,        -- the input dict
    results_json    TEXT NOT NULL,        -- list[PressCandidate]
    verified_url    TEXT,                 -- selected URL if verification ran
    verified_conf   REAL,
    cached_at       TEXT NOT NULL,
    ttl_hours       INTEGER NOT NULL DEFAULT 24
);
```

Hits return immediately at $0. TTL 24h (press releases are immutable
once published; re-searching same query within a day is pointless).

Same query across progs (e.g. editor re-pushes a cluster, we re-cluster)
→ cache hit, no API call.

---

## Pipeline integration

### Where it slots in

```
build_news_clusters.main()
  ↓
cluster_articles()              # lexical
  ↓
_apply_llm_editor_pass()
  ├─ brand-group articles
  ├─ for each multi-article brand group:
  │    rule-trigger Layer 3 search?    ← NEW
  │      if no tier-≤2 member in cluster
  │      → search OEM/regulator domains
  │      → verify candidates
  │      → if verified URL found:
  │           inject as virtual cluster member with primary tag
  │    cluster_group(brand, articles + virtual_member, history)
  │      ← LLM-editor can ALSO call find_press_release tool if it sees need
  ↓
output cluster picks primary via tier rank
  (virtual member from search will win as tier 0 OEM)
```

### Two-track activation
- **Pipeline pre-search** handles the common case (cluster has no OEM)
- **LLM-tool search** handles edge cases (LLM sees specific need)

Both feed the same `find_press_release` function, both hit the same cache.

---

## Cost model

Per typical prog (115 candidates → 30 multi-source clusters):

| Component | Volume | Per-call cost | Total |
|---|---:|---:|---:|
| Existing editorial_review | 200+ articles | varies | $0.50-0.70 |
| Existing LLM-as-editor | 20 brand groups | $0.005-0.010 | $0.10-0.20 |
| **NEW: Layer 3 search** | 10-15 triggered | $0.003 Brave | $0.04 |
| **NEW: Layer 4 verify** | 10-15 × 1-2 candidates | $0.003 LLM | $0.05 |
| **NEW: cache infra** | — | $0 | $0 |
| **NEW total** | | | **+$0.09-0.10** |
| **Prog total** | | | **$0.69 → $0.79** |

Cost increase: **+14%** for the autonomy upgrade.

Monthly: ~12-15 progs × $0.79 ≈ **$12/month** total LLM + search.

---

## Failure modes & graceful degradation

| Failure | Response |
|---|---|
| Brave API timeout | Return [], LLM proceeds without |
| Brave returns 0 results | Log + use cluster's tier-best as primary |
| HTTP HEAD on candidate fails | Try next; if all fail, give up |
| Verification LLM error | Skip verification, use top search result with low confidence |
| Rate-limit (429) | Exp. backoff (1, 2, 4s), 3 retries, then give up |
| Network down | Cache hits still work; new searches return [] |

**Guarantee**: Layer 3 is enhancement, not gate. If it fully fails,
output quality reverts to today's LLM-editor + tier-priority output.
**Never worse than current.**

---

## Eval methodology

The system already has the measurement infrastructure
(`scripts/_validate_llm_editor_vs_feedback.py`). Extension:

### Pre-flight (before deploy)
- Replay v42's editor-cited URLs through Layer 3+4 standalone
- For each `wrong_primary` complaint in v2 dataset:
  - Did Layer 3's search find the editor's URL (or a same-tier alternative)?
  - Recall measurement: % editor-cited URLs Layer 3 could have surfaced
- For each `dup_cross_run` complaint with referenced_url:
  - Did Layer 3 find the same URL when given the brand+date?
  - Validates the cross-run dup catch path

### Live
- After Layer 3 wired in pipeline, run a regular prog with feature flag
- Compare cluster outputs vs. previous progs on same dataset
- Editor feedback sync continues to measure jalob rate

### Acceptance criteria
- ≥80% of v2 wrong_primary cases auto-surface editor's URL
- ≥50% of v2 dup_cross_run cases get flagged in LLM-editor's events
- No regression in within-batch dup recall (currently 80% on v42)
- Cost ≤ +20% vs current prog

---

## Phased rollout

| Phase | Days | Work | Validation |
|---|---:|---|---|
| A | 1 | `press_search.py` skeleton + Brave API client + cache table | Unit tests, mock responses |
| B | 1 | `press_verify.py` HTTP fetch + LLM judge | Unit tests, fixture HTML |
| C | 1 | Wire as pipeline pre-search in `_apply_llm_editor_pass` | A/B on v42 data |
| D | 1 | Add LLM tool to `cluster_group` | A/B on v42 data |
| E | 1 | Production validation on v43+ progs | Editor feedback sync diff |

**Total: 5 days for Layer 3+4 done right.**

Each phase shippable independently:
- After A: can manually search and pass results into cluster_group
- After B: can also verify candidates before using
- After C: pipeline auto-searches when cluster lacks OEM
- After D: LLM can request searches mid-reasoning
- After E: measured production validation

---

## What this does NOT solve

Layer 3+4 is necessary for autonomy but not sufficient for 100% editor
replacement. Even after deploying:

- **Brand-new models nobody has named yet** — search may return zero
  results because no journalist has written about it. Genuinely unfindable.
- **Editor's subjective preference between two equally-OK English
  primaries** — carscoops vs autoevolution for the same story is an
  editorial call our tier system flattens.
- **Aspect-of-story disagreements** — "is this Confirmed or Rumors?"
  when even editors argue. We measured this ceiling at ~10% irreducible.

Realistic expectation: **autonomous handling of ~90-95% of stories**,
editor's role becomes spot-check + the 5-10% genuinely subjective.

---

## Open questions

1. **Brave vs Serper** — should we A/B both for one week to see real
   quality difference? Brave is cheaper but Google has the better
   index for niche OEM domains.

2. **Cache TTL** — 24h is conservative. Could push to 7d for press
   releases since they don't change. Need to handle "press release
   updated by OEM" edge case if it ever happens.

3. **Cross-language search** — Brave site:audi-mediacenter.com works
   for English. For Russian OEM press (lada.ru) we may need Cyrillic
   query construction. Test in Phase A.

4. **Verification model** — Haiku is fine for simple match check.
   For ambiguous cases ("is this article about THE recall vs A
   different recall"), should we escalate to Sonnet? Cost vs accuracy
   trade-off.

5. **Crawl-vs-search overlap** — once Layer 3 ships, do we still need
   to crawl OEM domains in Layer 2? Yes — crawling is cheaper than
   per-event search, and crawl coverage is the "warm cache" for Layer 3.

---

## Decision log entries to add to DECISIONS.md once shipped

- Why Brave Search over Serper (cost, EU jurisdiction, AI-friendly API)
- Cache TTL = 24h initial, may extend to 7d after measurement
- Verification is non-negotiable (raw search results too noisy)
- Layer 3 is hybrid pipeline-rule + LLM-tool — neither alone covers
  all cases cleanly
