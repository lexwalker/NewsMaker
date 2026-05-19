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
