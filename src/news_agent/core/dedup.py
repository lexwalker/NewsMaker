"""Fuzzy-title dedup — pure function over a title and the known-title set."""

from __future__ import annotations

from datetime import datetime, timezone

from rapidfuzz import fuzz


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
    eb = (event_brand or "").strip().lower()
    em = (event_model or "").strip().lower()
    et = (event_type or "").strip().lower()
    if not (eb and em and et and et != "other"):
        return None
    hit = recent.get(f"{eb}|{em}|{et}")
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
    threshold: float = 88.0,
    model_threshold: float = 62.0,
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
    nt = normalise_title(title)
    if not nt:
        return None
    for pt in pub_titles:
        if not pt or eb not in pt:
            continue  # brand gate: the archive title must mention this brand
        ratio = fuzz.token_set_ratio(nt, pt)
        # B — same brand+model AND a plausibly-similar headline.
        if em and len(em) >= 3 and em in pt and ratio >= model_threshold:
            return (
                f"(возможно дубль: уже публиковали о «{event_brand} "
                f"{event_model}» — проверьте)"
            )
        # A — no model confirmation: demand a strong full-title match.
        if ratio >= threshold:
            return (
                "(возможно дубль: похожий заголовок уже публиковали "
                "— проверьте)"
            )
    return None
