"""Miss-funnel: trace each editor publication through our pipeline to find
WHERE it died (peer-review §3).

For every story the editor actually published, we ask: did we collect it,
and if so, which step killed it? The decomposition turns the vague "recall
~32-54%, mostly coverage" into an actionable map:

  S1  source not in our 338-source list   → add the source
  S2  source present, we never collected   → fix RSS / link-scoring there
  S3  collected, a heuristic killed it     → prune that blacklist phrase
  S4  collected, the LLM rejected it       → classification problem
  S0/S5  we accepted it (reached the table, or died in clustering/dedup)

Pure matching + bucketing; the script (scripts/miss_funnel.py) does the
Google-Sheet + SQLite I/O and feeds windowed inputs in.

Honesty caveats baked into the design:
  * The editor's "Outer Link" is their CHOSEN source (often a primary like
    moex.com), which may differ from where WE would have found the story.
    So S1-by-Outer-Link-domain answers "is the editor's chosen source in
    our list", not "could we ever find this story". Treat S1 as an upper
    bound on source-gaps.
  * Cross-language title matching (editor EN/RU vs our collected title) is
    imperfect; brand/model tokens match well (language-neutral) but pure
    Russian civic news matches weakly. A failed match inflates S2, so the
    script reports match-method counts + an unmatched sample for eyeballing.
  * S0 (reached table) vs S5 (accepted but lost downstream) is NOT split
    here — both mean "our classifier said publish", which is the recall
    question. Splitting needs push-history matching (deferred).
"""

from __future__ import annotations

from dataclasses import dataclass

from rapidfuzz import fuzz

from news_agent.core.brand_canonical import canonicalize_brand
from news_agent.core.primary_source import normalise_title
from news_agent.core.reject_stage import (
    COLLAPSED,
    S3_HEURISTIC,
    S4_LLM,
    classify_outcome,
)
from news_agent.core.urls import canonicalise, domain_of

# Funnel stage codes -------------------------------------------------
S1 = "S1"            # source not in our list
S2 = "S2"            # source present, not collected
S3 = "S3"            # collected, killed by heuristic
S4 = "S4"            # collected, killed by LLM
ACCEPTED = "S0/S5"   # we accepted it (reached table or lost downstream)

DEFAULT_TITLE_THRESHOLD = 85.0
# Guard against the short-string trap: token_set_ratio returns 100 when a
# tiny title (an emoji "5️⃣", a bare "Седан") is a token-subset of a long
# one. Both sides must clear this bar before a fuzzy match is trusted.
_MIN_MATCH_TOKENS = 3


def _token_count(norm: str) -> int:
    return len([t for t in norm.split() if t])


@dataclass
class EditorPub:
    """A story the editor published (ground truth)."""

    title_en: str
    title_ru: str
    url: str       # the editor's Outer Link (their chosen source)
    date: str      # publication date string (for windowing upstream)
    section: str

    @property
    def domain(self) -> str:
        return domain_of(self.url) if self.url else ""

    @property
    def brand(self) -> str:
        return canonicalize_brand(f"{self.title_en} {self.title_ru}")

    @property
    def display_title(self) -> str:
        return self.title_en or self.title_ru


@dataclass
class Collected:
    """One article WE collected and scored (from the SQLite cache)."""

    canonical_url: str
    domain: str
    title: str
    first_seen_at: str
    verdict: str

    @property
    def brand(self) -> str:
        return canonicalize_brand(self.title)


@dataclass
class FunnelRow:
    pub: EditorPub
    stage: str
    cause: str
    match_method: str   # "url" | "fuzzy" | "none"
    matched_title: str
    score: float


def _stage_for_match(verdict: str) -> tuple[str, str]:
    """Stage for a publication we DID collect, from its verdict."""
    o = classify_outcome(verdict)
    if o.stage == S3_HEURISTIC:
        return S3, o.cause
    if o.stage == S4_LLM:
        return S4, "llm"
    if o.stage == COLLAPSED:
        # we had the URL but collapsed it into another collected row —
        # that sibling may well be on the sheet, so this is not a miss
        return ACCEPTED, "dedup_collapse"
    if not o.is_reject:
        return ACCEPTED, "accepted"
    # stale / fetch-error / unknown: we touched the URL but produced no
    # usable article → effectively a collection-side failure
    return S2, o.cause or "matched_reject"


def _stage_for_unmatched(pub: EditorPub, source_domains: set[str]) -> tuple[str, str]:
    """Stage for a publication we never matched to our collection."""
    if not pub.url:
        return S2, "no_source_url"
    if pub.domain not in source_domains:
        return S1, "source_not_in_list"
    return S2, "not_collected"


def _best_fuzzy(
    pub: EditorPub,
    prepared: list[tuple[Collected, str, str]],
    threshold: float,
    brand_gate: bool,
) -> tuple[Collected | None, float]:
    """Best collected item by token_set_ratio over the editor's EN+RU
    titles, optionally gated to the same canonical brand."""
    pub_norms = [
        normalise_title(t) for t in (pub.title_en, pub.title_ru) if t.strip()
    ]
    # Drop pub titles too short to match safely (short-string trap).
    pub_norms = [p for p in pub_norms if _token_count(p) >= _MIN_MATCH_TOKENS]
    if not pub_norms:
        return None, 0.0
    pub_brand = pub.brand
    best: Collected | None = None
    best_score = 0.0
    for item, item_norm, item_brand in prepared:
        # Item side must also clear the minimum-content bar.
        if _token_count(item_norm) < _MIN_MATCH_TOKENS:
            continue
        if brand_gate and pub_brand and item_brand and pub_brand != item_brand:
            continue
        score = max(fuzz.token_set_ratio(p, item_norm) for p in pub_norms)
        if score > best_score:
            best_score = score
            best = item
    if best is not None and best_score >= threshold:
        return best, best_score
    return None, best_score


def build_funnel(
    pubs: list[EditorPub],
    collected: list[Collected],
    source_domains: set[str],
    *,
    threshold: float = DEFAULT_TITLE_THRESHOLD,
    brand_gate: bool = True,
) -> list[FunnelRow]:
    """Classify every editor publication into a funnel stage.

    Matching order: exact canonical-URL match first (high precision), then
    brand-gated fuzzy title match over the editor's EN+RU titles. Unmatched
    rows split into S1 (source not in list) / S2 (not collected).
    """
    by_url: dict[str, Collected] = {}
    for it in collected:
        if it.canonical_url:
            by_url.setdefault(it.canonical_url, it)
    prepared = [(it, normalise_title(it.title), it.brand) for it in collected]

    rows: list[FunnelRow] = []
    for pub in pubs:
        cu = canonicalise(pub.url) if pub.url else ""
        m = by_url.get(cu) if cu else None
        if m is not None:
            stage, cause = _stage_for_match(m.verdict)
            rows.append(FunnelRow(pub, stage, cause, "url", m.title, 100.0))
            continue
        best, score = _best_fuzzy(pub, prepared, threshold, brand_gate)
        if best is not None:
            stage, cause = _stage_for_match(best.verdict)
            rows.append(FunnelRow(pub, stage, cause, "fuzzy", best.title, score))
        else:
            stage, cause = _stage_for_unmatched(pub, source_domains)
            rows.append(FunnelRow(pub, stage, cause, "none", "", score))
    return rows


def summarise(rows: list[FunnelRow]) -> dict:
    """Aggregate funnel rows into stage counts, S3 sub-causes, S1/S2
    domains, and match-method counts — everything the report prints."""
    from collections import Counter

    stages: Counter = Counter()
    s3_causes: Counter = Counter()
    s1_domains: Counter = Counter()
    s2_domains: Counter = Counter()
    methods: Counter = Counter()
    for r in rows:
        stages[r.stage] += 1
        methods[r.match_method] += 1
        if r.stage == S3:
            s3_causes[r.cause] += 1
        elif r.stage == S1:
            s1_domains[r.pub.domain or "(no-url)"] += 1
        elif r.stage == S2:
            s2_domains[r.pub.domain or "(no-url)"] += 1
    total = len(rows)
    return {
        "total": total,
        "stages": dict(stages),
        "s3_causes": dict(s3_causes),
        "s1_domains": dict(s1_domains),
        "s2_domains": dict(s2_domains),
        "methods": dict(methods),
    }
