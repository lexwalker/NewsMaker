"""Shared per-row editorial-pass building blocks.

The per-candidate LLM review logic used to exist as TWO hand-synced copies
(batch_fetch_test._run_llm_pass and retry_failed_llm) and had already
drifted four ways: the recovery tool lacked the usage-limit abort, lacked
the heuristic-section override, carried 1 of 3 dup-hint tiers, and its
language-tag map had 8 languages vs the main run's 15 (a recovered Korean
title got "(KO)" instead of the editor's "(КОР)"). Every future fix had to
be applied twice — and wasn't.

This module is the single source of truth for the drift-prone pieces. The
loop orchestration (printing, budget, stats) deliberately stays in the
scripts — extracting it wholesale would churn the hottest production path
for cosmetic gain.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from news_agent.core.dedup import (
    archive_model_hint_is_weak,
    event_hint_is_stale,
    published_dup_hint,
    recent_event_dup_hint,
    recent_model_dup_hint,
)
from news_agent.core.heuristic_relevance import heuristic_section

# ---------------------------------------------------------------- lang tags
# Source language → (EN-title tag, RU-title tag), matching the format the
# editorial team uses in "Новости опубликованные".
#   EN line gets the ISO code in Latin uppercase: (EN), (RU), (DE)...
#   RU line gets a 3-letter Russian abbreviation: (АНГЛ), (РУС), (НЕМ)...
# Mapping verified against 2,817 rows of "Новости опубликованные" — these
# are the exact strings the editor uses, not guesses.
LANG_TAG_MAP: dict[str, tuple[str, str]] = {
    "en": ("EN", "АНГЛ"),
    "ru": ("RU", "РУС"),
    "de": ("DE", "НЕМ"),      # editor uses НЕМ, not ДЕ
    "fr": ("FR", "ФР"),
    "it": ("IT", "ИТАЛ"),     # editor uses ИТАЛ, not ИТ
    "es": ("ES", "ИСП"),
    "zh": ("ZH", "КИТ"),
    "ja": ("JA", "ЯП"),
    "ko": ("KO", "КОР"),
    "pl": ("PL", "ПОЛ"),
    "pt": ("PT", "ПОР"),
    "nl": ("NL", "НИД"),
    "cs": ("CS", "ЧЕШ"),
    "tr": ("TR", "ТУР"),
    "uk": ("UK", "УКР"),
}


def lang_tags_for(lang: str) -> tuple[str, str]:
    """(EN-tag, RU-tag) for an ISO-639-1 code; uppercase code as fallback."""
    if not lang:
        return ("", "")
    lang = lang.strip().lower()[:2]
    return LANG_TAG_MAP.get(lang, (lang.upper(), lang.upper()))


# ------------------------------------------------------- consistency guard
# The model occasionally returns should_publish=True while its OWN reason
# concludes the item must be rejected. Trust the explicit reject directive
# over the boolean (jun-18: a "…— отклонить" row slipped into the live feed).
REJECT_DIRECTIVE_PHRASES = ("отклонить", "reject", "не публику", "не наша тема")


def has_reject_directive(reason: str | None) -> bool:
    r = (reason or "").lower()
    return any(p in r for p in REJECT_DIRECTIVE_PHRASES)


# ------------------------------------------------------ persistent failures
# An account-level usage-limit 400 is PERSISTENT: every remaining call fails
# identically, so the pass must abort instead of silently burning the tail
# (the I2 incident truncated ~15% of every run it hit).
USAGE_LIMIT_MARKERS = ("usage limit", "reached your specified")


def looks_like_usage_limit(error_message: str) -> bool:
    m = (error_message or "").lower()
    return any(k in m for k in USAGE_LIMIT_MARKERS)


# ------------------------------------------------------- section resolution
@dataclass(frozen=True)
class SectionDecision:
    section: str
    region: str
    confidence: float
    heuristic_note: str   # "эвристика: … (LLM said …)" when overridden, else ""


def resolve_section_region(
    *,
    review_section: str,
    review_region: str | None,
    review_confidence: float,
    title: str,
    body_excerpt: str,
    domain: str,
    section_names: set[str],
) -> SectionDecision:
    """Final section/region for an ACCEPTED row.

    The heuristic pre-classifier overrides the LLM when it is confident
    (e.g. body-type pickup → LCV always wins) — a safety net for systematic
    LLM mistakes. Falls back to the LLM's section with an 'Other news'
    guard for hallucinated section names.
    """
    pre = heuristic_section(
        title=title, body_excerpt=body_excerpt, domain=domain)
    if pre is not None:
        return SectionDecision(
            section=pre.section if pre.section in section_names else "Other news",
            region=pre.region,
            confidence=round(pre.confidence, 2),
            heuristic_note=f"эвристика: {pre.reason} (LLM said {review_section})",
        )
    return SectionDecision(
        section=review_section if review_section in section_names else "Other news",
        region=review_region or "Global",
        confidence=round(review_confidence, 2),
        heuristic_note="",
    )


# ------------------------------------------------------------- dup hints
def dup_hint_for(
    *,
    title: str,
    event_brand: str,
    event_model: str,
    event_type: str,
    launch_brand_model: str,
    canon_url: str,
    pub_titles: set[str],
    recent_ev: dict | None,
    recent_bm: dict | None,
    own_titles: set[str] | None = None,
    alt_title: str = "",
) -> str | None:
    """Advisory dup hint for an accepted row — at most ONE, by priority:

    (0) PUBLISHED-archive paraphrase — already in «Опубликованные (все)»
        under a divergent headline, the strongest "don't re-post" signal the
        exact gate missed;
    (0.5) OUR-OWN-pushes paraphrase (jul-20 dup-wave) — same fuzzy machinery
        vs the titles WE pushed in the last 30 days (DedupStore
        .recent_pushed_titles). Covers the layer nothing else did: a story we
        fed on the 17th resurfacing from another outlet on the 19th — URL
        differs, wording differs, and pre-fix history rows carry no event
        keys, so the semantic tier is blind to them;
    (1) Stage-2a SEMANTIC event-key vs our own recent runs;
    (2) lexical P3-D brand_model.

    Advisory only: the caller prepends it to llm_reason; the push diverts
    such rows to the review tab (reversible). Never raises.
    """
    try:
        hint = published_dup_hint(
            title, event_brand, event_model, pub_titles,
            alt_title=alt_title)
        if not hint and own_titles:
            hint = published_dup_hint(
                title, event_brand, event_model, own_titles,
                alt_title=alt_title,
                source_label="недавно уже отправляли в фид")
        if not hint and event_model and recent_ev:
            hint = recent_event_dup_hint(
                event_brand, event_model, event_type, recent_ev, canon_url)
        if not hint and launch_brand_model and recent_bm:
            hint = recent_model_dup_hint(launch_brand_model, recent_bm, canon_url)
        return hint
    except Exception:  # noqa: BLE001 — advisory only, never break the pass
        return None


def current_dup_hint(**kwargs) -> tuple[str | None, str]:
    """dup_hint_for + the demotions that define the CURRENT rules.

    Returns (hint, demotion) where demotion is '' | 'stale' | 'weak':
      stale — the event-key match is older than a week (25-33% wrong,
      jul-27 measurement on 1058 editor answers);
      weak — the archive brand+model tier, which has no notion of the
      EVENT (24% wrong, jul-28).
    Both demote to None so the caller's LLM arbiter reads those cases
    instead of an auto-divert. ONE shared implementation for the main run,
    the recovery tool and the cached-row re-derive — the recovery tool
    carried its own hint call without the demotions for a week (aug-05
    audit: weak-archive diverts kept appearing on post-jul-28 rows).
    """
    hint = dup_hint_for(**kwargs)
    if hint and event_hint_is_stale(hint):
        return None, "stale"
    if hint and archive_model_hint_is_weak(hint):
        return None, "weak"
    return hint, ""


# The advisory hint is PREPENDED to llm_reason for the sheet/push protocol
# (the push diverts on the «возможно дуб» substring). It must never be
# PERSISTED inside the cached reason: a frozen hint replays on every cache
# restore of the URL (window overlap, hot lane, recover), so a one-shot
# advisory becomes a permanent property of the row, immune to later rule
# changes (aug-05 audit: 151/250 accepted rows carried a frozen hint).
_HINT_PREFIX_RE = re.compile(r"^\(?возможно дубль[:,][^|]*", re.I)
_HINT_MARKERS = (
    "ии-арбитр",           # LLM arbiter (batch_fetch_test)
    "уже было",            # event-key tier (recent_event_dup_hint)
    "отправляли в фид",    # tier 0.5 own-pushes (published_dup_hint)
    "уже публиковали",     # archive tiers (published_dup_hint)
    "уже писали",          # P3-D brand+model (recent_model_dup_hint)
    "уже публиковалось",   # cluster-stage cross-run hint (defensive)
)


def split_dup_hint(reason: str | None) -> tuple[str, str]:
    """(injected_hint, clean_reason) — separate OUR prepended hint from the
    model's own text. Recognised by our hint wording only: a reason the
    MODEL chose to begin with «возможно дубль…» is left intact."""
    text = reason or ""
    m = _HINT_PREFIX_RE.match(text)
    if not m:
        return "", text
    head = m.group(0)
    if not any(k in head.lower() for k in _HINT_MARKERS):
        return "", text
    rest = text[m.end():].lstrip()
    if rest.startswith("|"):
        rest = rest[1:].lstrip()
    return head.strip(), rest
