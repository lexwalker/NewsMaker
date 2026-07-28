"""Fuzzy-title dedup — pure function over a title and the known-title set."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from functools import lru_cache

from rapidfuzz import fuzz

# ---- Event-key canonicalisation (jul-20 dup-wave forensics) ----------------
# The LLM writes event_brand/event_model free-form, so the SAME happening got
# different keys and slipped every event-dedup layer: «АвтоВАЗ …» keyed
# "avtovaz|…" in one run and "lada|…" in another (brand aliases), Avatr keyed
# "07 l" vs "07l" (model spacing). NB: DedupStore.recent_event_keys ALREADY
# canonicalises the map side via brand_canonical — the actual bug was the
# FRESH side arriving raw, so "avtovaz" never met the map's "lada". These
# helpers wrap the SAME shared module (never a parallel alias table), applied
# at COMPARE time so months of raw history keys also benefit.


@lru_cache(maxsize=4096)
def canonical_event_brand(brand: str) -> str:
    """avtovaz/АвтоВАЗ/ВАЗ → lada; unknown names pass through unchanged.
    Same brand_canonical module the cache-map side uses; any failure degrades
    to identity (hints are advisory, never break a run)."""
    eb = (brand or "").strip().lower()
    if not eb:
        return ""
    try:
        from news_agent.core.brand_canonical import canonicalize_brand
        return (canonicalize_brand(eb) or eb).lower()
    except Exception:  # noqa: BLE001
        return eb


@lru_cache(maxsize=8192)
def squash_model(model: str) -> str:
    """Spacing/hyphen-insensitive model form: "07 l" == "07l" == "07-l"."""
    return re.sub(r"[\s\-]+", "", (model or "").strip().lower())


@lru_cache(maxsize=512)
def brand_name_variants(brand: str) -> frozenset[str]:
    """All names of a brand (canonical + every alias, lowered, len≥3).

    Used as the brand GATE in title checks. Scripts are NOT the issue
    (normalise_title transliterates Cyrillic, «АвтоВАЗ»→"avtovaz") — ALIASES
    are: event_brand says "avtovaz" while the headline says «Лада Искра…»
    (→ "lada iskra"), so a bare ``eb in pt`` gate finds nothing and the
    paraphrase tier stays mute for exactly the brand's other names."""
    eb = (brand or "").strip().lower()
    if not eb:
        return frozenset()
    out = {eb}
    try:
        from news_agent.core.brand_canonical import (
            aliases_for, canonicalize_brand)
        canon = (canonicalize_brand(eb) or eb)
        out.add(canon.lower())
        for a in aliases_for(canon):
            out.add((a or "").strip().lower())
    except Exception:  # noqa: BLE001
        pass
    return frozenset(v for v in out if len(v) >= 3)


def title_is_duplicate(title: str, known: list[str], *, threshold: float) -> bool:
    """True iff ``title`` fuzzy-matches any item in ``known`` at ≥ threshold.

    ``threshold`` is a 0–1 float; ``fuzz.token_set_ratio`` returns 0–100.
    """
    if not title or not known:
        return False
    cutoff = threshold * 100.0
    for other in known:
        if not other:
            continue
        if fuzz.token_set_ratio(title, other) >= cutoff:
            return True
    return False


def recent_model_dup_hint(
    brand_model: str,
    recent: dict[str, tuple[str, str]],
    current_url: str,
    *,
    now: datetime | None = None,
) -> str | None:
    """Plan P3-D — ADVISORY only. Never suppresses or reclassifies.

    Returns a short Russian hint string when ``brand_model`` was already
    classified within the recency window (``recent`` is the map from
    ``DedupStore.recent_brand_models``: {normalised bm → (last_seen_at_iso,
    canonical_url)}), so the editor can spot "мы про эту модель уже
    писали" cases the URL/title fuzzy-dedup misses.

    Returns None when:
      • brand_model empty / not seen
      • the only prior sighting is THIS same URL (a re-run of the same row)
      • timestamps unpar? — degrade silently (advisory feature, never break)
    """
    if not brand_model:
        return None
    key = brand_model.strip().lower()
    if not key:
        return None
    hit = recent.get(key)
    if hit is None:
        return None
    last_seen_iso, prev_url = hit
    # Same URL = just a re-run of the identical article, not a dup signal.
    if prev_url and current_url and prev_url == current_url:
        return None
    try:
        last_seen = datetime.fromisoformat(last_seen_iso)
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        ref = now or datetime.now(timezone.utc)
        days = max(0, (ref - last_seen).days)
        when = "сегодня" if days == 0 else f"~{days} дн. назад"
    except (ValueError, TypeError):
        when = "недавно"
    return f"(возможно дубль: о «{brand_model}» уже писали {when} — проверьте)"


# Age at which an event-key match stops being trustworthy on its own.
# Measured jul-27 on 1058 editor-answered rows in «Разметка отклонённого»:
# false-divert rate by match age — today 6%, 2-3d 12%, 4-7d 5%, but 8-14d 25%
# and 15-30d 33%. Beyond a week the same (brand|model|type) key is usually a
# DIFFERENT happening (a facelift after a reveal, a second recall, a new
# market), so the hint must not divert on its own — the caller hands those to
# the LLM arbiter instead. The 30-day STORE window stays as is: the older
# entries are still useful evidence, just not an automatic verdict.
EVENT_HINT_TRUSTED_DAYS = 7


def archive_model_hint_is_weak(hint: str) -> bool:
    """True for the all-time archive tier's «уже публиковали о «brand model»».

    That tier fires when the editor's archive holds ANY story about the same
    brand+model at a moderate headline similarity — it has no notion of the
    EVENT. Measured jul-28 on 140 editor-answered rows: 33 wrong diverts
    (24%), all of them a NEW happening for a model we covered before — a
    recall of the Urus after its reveal, an X5 LWB for India after the one
    for China, an AMG final edition after the base car, pre-orders for a
    model whose specs we ran. The 107 correct diverts are true re-runs.
    A text-only rule cannot separate those, so the caller hands this tier to
    the LLM arbiter instead of diverting on it. The OTHER archive branch
    («похожий заголовок…», a strong ≥88 full-title match) stays automatic —
    it fired 10 times with 1 error.
    """
    return "уже публиковали о «" in (hint or "")


def event_hint_is_stale(hint: str, threshold_days: int = EVENT_HINT_TRUSTED_DAYS) -> bool:
    """True when a dup hint cites a match older than ``threshold_days``.

    Reads the age straight off the hint text we generate ourselves
    («уже было ~12 дн. назад»). Hints without an explicit age (today /
    «недавно» / archive paraphrase) are never stale — those tiers are
    calibrated separately.
    """
    m = re.search(r"~(\d+)\s*дн", hint or "")
    return bool(m) and int(m.group(1)) > threshold_days


def recent_event_dup_hint(
    event_brand: str,
    event_model: str,
    event_type: str,
    recent: dict[str, tuple[str, str, str]],
    current_url: str,
    *,
    now: datetime | None = None,
) -> str | None:
    """Hybrid Stage 2a — ADVISORY only. Semantic upgrade of
    recent_model_dup_hint: keyed on the LLM event-signature
    (brand|model|event_type), so it catches the SAME happening across
    divergent headlines / languages that the lexical brand_model misses.

    ``recent`` is the map from ``DedupStore.recent_event_keys``:
    {event_key → (last_seen_at_iso, canonical_url, display)}.

    Returns None when: any signature part empty / event_type generic /
    not seen / only prior sighting is THIS same URL / unparseable
    timestamp (degrade silently — advisory must never break the run).
    """
    eb = canonical_event_brand(event_brand)
    em = (event_model or "").strip().lower()
    et = (event_type or "").strip().lower()
    if not (eb and em and et and et != "other"):
        return None
    # Compatible event types. jul-14 (MG-Goodwood 4x dup): the SAME reveal got
    # type='reveal' from one write-up and 'motorshow' from another. jul-20
    # (Avatr 07 L dup): «старт продаж с ценами» keyed 'launch' from one outlet
    # and 'pricing' from another — one happening, two keys. Only these two
    # well-motivated pairs are folded — wider type-equivalence would inflate
    # the false-divert rate the review tab already shows.
    _COMPAT = {"reveal": ("reveal", "motorshow"),
               "motorshow": ("motorshow", "reveal"),
               "launch": ("launch", "pricing"),
               "pricing": ("pricing", "launch")}
    types = frozenset(_COMPAT.get(et, (et,)))
    # Fast path: exact key (already-canonical history).
    hit = None
    for _t in types:
        hit = recent.get(f"{eb}|{em}|{_t}")
        if hit is not None:
            break
    if hit is None:
        # Canonical scan (jul-20 dup-wave): history keys are written free-form,
        # so compare both sides canonically — brand through the alias map
        # ("avtovaz|…" history vs "lada" fresh), model spacing-insensitive
        # ("07 l" vs "07l"). Plus the jul-14 token-subset fallback: two
        # write-ups of the SAME multi-model event normalise the list
        # differently ("go" vs "go and cyber") — same brand + compatible type
        # + one token set contained in the other ⇒ same event ("seal" vs
        # "sealion" is not a subset either way, so distinct models stay apart).
        _CONNECTORS = {"and", "&", "и", "+", ","}
        sq = squash_model(em)
        my = {w for w in em.split() if w not in _CONNECTORS}
        for key, val in recent.items():
            parts = key.split("|")
            if len(parts) != 3:
                continue
            kb, km, kt = parts
            if kt not in types:
                continue
            if canonical_event_brand(kb) != eb:
                continue
            if squash_model(km) == sq:
                hit = val
                break
            theirs = {w for w in km.split() if w not in _CONNECTORS}
            if my and theirs and (my <= theirs or theirs <= my):
                hit = val
                break
    if hit is None:
        return None
    last_seen_iso, prev_url, display = hit
    if prev_url and current_url and prev_url == current_url:
        return None
    try:
        last_seen = datetime.fromisoformat(last_seen_iso)
        if last_seen.tzinfo is None:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        ref = now or datetime.now(timezone.utc)
        days = max(0, (ref - last_seen).days)
        when = "сегодня" if days == 0 else f"~{days} дн. назад"
    except (ValueError, TypeError):
        when = "недавно"
    return (
        f"(возможно дубль: «{display}» уже было {when} — проверьте)"
    )


def published_dup_hint(
    title: str,
    event_brand: str,
    event_model: str,
    pub_titles: set[str] | list[str],
    *,
    alt_title: str = "",
    threshold: float = 88.0,
    model_threshold: float = 62.0,
    source_label: str = "уже публиковали",
) -> str | None:
    """ADVISORY — paraphrase check vs the editor's PUBLISHED archive
    ("Опубликованные (все)", normalised titles within the recency window).

    The deterministic gate (``already_published``) only catches an EXACT
    source URL or EXACT normalised title; a story the editor already
    published, re-surfacing under a rephrased headline / another outlet /
    EN↔RU, slips straight through. Two cheap ($0, no LLM) recall layers
    on top of it, BOTH brand-gated so we never collapse two unrelated
    stories that merely share boilerplate words:

      B) a recent archive title containing BOTH the event brand AND the event
         model (the LLM event-signature) AND a moderate headline overlap
         (``token_set_ratio`` ≥ ``model_threshold``) — catches the SAME
         happening under a divergent headline / language. The similarity
         floor is what separates a real repeat ("Vesta sales +20%" → "Vesta
         sales up 20 percent") from a different event on the same model
         ("Vesta sales" vs "Vesta new colour"), which the archive's missing
         event_type can't tell apart on its own;
      A) no model confirmation (model absent / not in the title) → demand a
         STRONG full-title match (≥ ``threshold``) within the same brand,
         covering rephrases where model extraction failed.

    Never suppresses or reclassifies — returns a 'возможно дубль' string the
    caller appends to ``llm_reason`` (→ diverted to the review tab, where the
    editor confirms; reversible, unlike the exact-match hard reject).

    Returns None when there is no brand anchor (can't gate → don't risk a
    false flag), on empty inputs, or no match. ``pub_titles`` must already be
    normalised (``normalise_title``), matching how the archive index is built;
    the archive's English titles (col D) align with the LLM's English
    brand/model.
    """
    eb = (event_brand or "").strip().lower()
    if not eb or not pub_titles:
        return None  # no brand anchor → can't gate safely → stay silent
    em = (event_model or "").strip().lower()
    from news_agent.core.primary_source import normalise_title
    # jul-23 (Geely A7 «писали давно» forensics): the archive is EN-heavy,
    # so an RU original transliterates to ~50 ratio against its own EN twin.
    # When the caller has the EN translation, match BOTH forms and keep the
    # better score.
    nts = [n for n in (normalise_title(title), normalise_title(alt_title))
           if n]
    if not nts:
        return None
    # Model anchor, token-level (jul-23: full-substring `em in pt` missed
    # every archive title because the extracted model carried a suffix —
    # «galaxy a7 em» is a substring of nothing). Anchored when a
    # digit-bearing model token matches (a7/qx80 — specific enough alone)
    # OR every word-token of the model is present (single-word models keep
    # the old substring semantics; «galaxy» alone must NOT anchor a
    # different Galaxy-family model).
    _digit_toks = {t for t in em.split()
                   if any(ch.isdigit() for ch in t)}
    _word_toks = {t for t in em.split() if len(t) >= 3}
    # Brand gate through ALL name variants (canonical + every alias):
    # event_brand "avtovaz" must also gate against a title that only says
    # «Лада …» (normalised "lada …") — a bare ``eb in pt`` misses every
    # other name of the same brand (jul-20 dup-wave forensics).
    variants = brand_name_variants(eb) or frozenset({eb})
    for pt in pub_titles:
        if not pt or not any(v in pt for v in variants):
            continue  # brand gate: the title must mention this brand
        ratio = max(fuzz.token_set_ratio(nt, pt) for nt in nts)
        pt_toks = set(pt.split())
        model_anchor = bool(
            em and len(em) >= 3
            and ((_digit_toks and _digit_toks & pt_toks)
                 or (_word_toks and _word_toks <= pt_toks)))
        # B — same brand+model AND a plausibly-similar headline.
        if model_anchor and ratio >= model_threshold:
            return (
                f"(возможно дубль: {source_label} о «{event_brand} "
                f"{event_model}» — проверьте)"
            )
        # A — no model confirmation: demand a strong full-title match.
        if ratio >= threshold:
            return (
                f"(возможно дубль: похожий заголовок {source_label} "
                "— проверьте)"
            )
    return None
