"""LLM dup-arbiter candidate retrieval (jul-23, editor: «писали уже давно»).

The deterministic dedup layers answer «точно то же» (exact/subset keys,
strong title fuzz). This module collects the NEAR-MISSES they stay silent
on — candidates that look suspiciously related but are not provably the
same event — so a cheap LLM call (AnthropicLLMClient.same_published_event)
can read and decide. Advisory only: a match becomes a «возможно дубль»
hint and the row goes to the review tab, never a hard reject.

Two candidate sources:
  A) the ALL-TIME published archive («Опубликованные (все)», normalised
     titles) — catches the Geely-A7 class: «отобрал у Chery мировой
     рекорд по дальности хода» vs «traveled 2 608 km on a single tank»
     share only brand+model, no threshold can separate that from a
     genuinely new A7 story without reading;
  B) the 30-day event-key store — the gray zone between exact/subset
     equality and unrelated: empty model on either side (Honda–GAC
     partnership class), overlapping-but-not-nested model tokens, same
     model with a different event type.

Pure functions, no LLM/client dependency — the batch owns the call.
"""

from __future__ import annotations

from rapidfuzz import fuzz

from news_agent.core.dedup import (
    brand_name_variants,
    canonical_event_brand,
    squash_model,
)
from news_agent.core.primary_source import normalise_title

_CONNECTORS = {"and", "&", "и", "+", ","}

# Floors mirroring published_dup_hint's calibration: 62 is where that layer
# fires deterministically WITH a model anchor — below it, only the LLM may
# judge. 40 keeps obvious noise (brand-only overlap, ratio ~30) out of paid
# calls; the Geely-A7 pair scores ~45-55.
_ANCHOR_FLOOR = 40.0
_NO_ANCHOR_FLOOR = 55.0


def _model_anchor_tokens(event_model: str) -> tuple[set[str], set[str]]:
    """(digit_tokens, word_tokens) of the model, connectors dropped.

    Digit-bearing tokens ('a7', 'qx80') are STRONG anchors alone; plain
    words ('galaxy', 'armada', len>=3) only anchor as a FULL set — a lone
    family word like 'galaxy' must not pull in every Galaxy-family story
    (same guard published_dup_hint ships)."""
    digit, word = set(), set()
    for tok in (event_model or "").strip().lower().split():
        if tok in _CONNECTORS:
            continue
        if any(ch.isdigit() for ch in tok):
            digit.add(tok)
        elif len(tok) >= 3:
            word.add(tok)
    return digit, word


def archive_candidates(
    *,
    title: str,
    alt_title: str = "",
    event_brand: str,
    event_model: str,
    pub_titles: set[str] | list[str],
    k: int = 5,
) -> list[str]:
    """Near-miss candidates from the all-time published archive.

    Brand-gated through every brand name variant (same gate as
    published_dup_hint). A candidate qualifies via a model-token anchor
    (any specific model token present in the archive title) at a low fuzz
    floor, or a moderately similar title without the anchor. Returns the
    top-``k`` archive titles, best first.
    """
    eb = (event_brand or "").strip().lower()
    if not eb or not pub_titles:
        return []
    variants = brand_name_variants(eb) or frozenset({eb})
    digit_toks, word_toks = _model_anchor_tokens(event_model)
    nts = [n for n in (normalise_title(title), normalise_title(alt_title)) if n]
    if not nts:
        return []
    scored: list[tuple[int, float, str]] = []
    for pt in pub_titles:
        if not pt or not any(v in pt for v in variants):
            continue
        ratio = max(fuzz.token_set_ratio(nt, pt) for nt in nts)
        pt_toks = set(pt.split())
        if digit_toks and digit_toks & pt_toks:
            tier = 2          # specific model token ('a7') — strong anchor
        elif word_toks and word_toks <= pt_toks:
            tier = 1          # full word-model named — weak anchor
        else:
            tier = 0          # brand-only overlap
        if (tier and ratio >= _ANCHOR_FLOOR) or ratio >= _NO_ANCHOR_FLOOR:
            scored.append((tier, ratio, pt))
    scored.sort(reverse=True)
    # Greedy pick with variant dedup: the archive keeps several normalised
    # variants of ONE publication (EN title, EN+RU concatenation) — without
    # this they eat the k slots and push the true paraphrase twin out
    # (jul-23 Geely-A7 forensics: 2 of 5 slots were variant copies).
    out: list[str] = []
    for _, _, pt in scored:
        if any(fuzz.token_set_ratio(pt, prev) >= 90 for prev in out):
            continue
        out.append(pt)
        if len(out) >= k:
            break
    return out


def graykey_candidates(
    *,
    event_brand: str,
    event_model: str,
    event_type: str,
    recent: dict[str, tuple[str, str, str]],
    current_url: str = "",
    k: int = 4,
) -> list[str]:
    """Near-miss candidates from the 30-day event-key store.

    ``recent`` is DedupStore.recent_event_keys: {key → (last_seen_iso,
    canonical_url, display)}. Excludes pairs the deterministic layers
    already decide (exact / squash-equal / token-subset with same type);
    keeps the undecidable gray zone. Returns display lines, newest first.
    """
    eb = canonical_event_brand(event_brand)
    em = (event_model or "").strip().lower()
    et = (event_type or "").strip().lower()
    if not eb or not et or et == "other":
        return []
    my_toks = {w for w in em.split() if w not in _CONNECTORS}
    my_sq = squash_model(em)
    out: list[tuple[str, str]] = []
    for key, (seen_iso, url, display) in recent.items():
        parts = key.split("|")
        if len(parts) != 3:
            continue
        hb, hm, ht = parts
        if canonical_event_brand(hb) != eb:
            continue
        if current_url and url == current_url:
            continue
        hm = hm.strip().lower()
        ht = ht.strip().lower()
        h_toks = {w for w in hm.split() if w not in _CONNECTORS}
        deterministic = (
            (my_sq and my_sq == squash_model(hm) and et == ht)
            or (my_toks and h_toks
                and (my_toks <= h_toks or h_toks <= my_toks) and et == ht)
        )
        if deterministic:
            continue
        # NB: «same model, different type» was tried as a third gray class
        # and removed after the jul-23 offline eval: it produced only false
        # flags (Esteo reveal-vs-tech, Hennessey spy_shot-vs-reveal) and
        # caught zero real dups — different lifecycle stages of one model
        # are almost always genuinely different news.
        gray = (
            ((not em or not hm) and et == ht)
            or (my_toks and h_toks and (my_toks & h_toks)
                and not (my_toks <= h_toks or h_toks <= my_toks))
        )
        if gray:
            out.append((seen_iso, display or key))
    out.sort(reverse=True)
    return [d for _, d in out[:k]]


def build_fresh_display(
    *,
    title: str,
    alt_title: str = "",
    event_brand: str = "",
    event_model: str = "",
    event_type: str = "",
) -> str:
    """One display block for the fresh row, fed to same_published_event."""
    lines = [title.strip()]
    if alt_title.strip() and alt_title.strip() != title.strip():
        lines.append(alt_title.strip())
    sig = "|".join(
        p for p in ((event_brand or "").strip(), (event_model or "").strip(),
                    (event_type or "").strip()))
    if sig.strip("|"):
        lines.append(f"[event: {sig}]")
    return "\n".join(lines)
