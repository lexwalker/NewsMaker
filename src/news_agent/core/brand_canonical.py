"""Brand-canonical mapping — a single source of truth for "what brand
is this article about" across the pipeline.

Why this module exists
----------------------
For a month the dedup layer was missing matches because different parts
of the pipeline produced different brand strings for the same brand:

  • LLM emitted ``"ssangyong"`` for an article about KGM Torres
    (KGM is the post-rename brand). Dedup keyed on (brand, model)
    therefore couldn't merge with the article that said ``"kgm"``.

  • Mercedes-AMG GT articles got ``brand="mercedes-amg"`` while
    Mercedes-Benz S-Class got ``brand="mercedes-benz"`` — both are the
    same parent brand from the editor's POV, but the dedup match was
    exact-string only.

  • BMW Alpina → some articles tagged ``"alpina"``, some ``"bmw"``,
    none merged.

  • ``Volkswagen`` ↔ ``vw`` ↔ ``Vw`` — three different keys.

``canonicalize_brand`` collapses every alias to a canonical name from
``config/brand_domains.yaml``. It's used wherever the pipeline keys
on brand — DedupStore, embedding match, LLM extraction post-processing.

Performance
-----------
The YAML is loaded once at module import. The lookup is O(1) via a
flat lowercase alias→canonical dict; calling sites can canonicalize
millions of strings per second.

Design notes
------------
  • We DO NOT canonicalize across mergers the editor still treats as
    separate. E.g. Mercedes-AMG stays "Mercedes-AMG" — articles about
    AMG GT and S-Class are distinct buckets in the editor's mind.
  • The opposite case: SsangYong→KGM is a real rename (2023). Editor
    flagged the lack of unification in the v41 audit (history had
    "kgm" articles, current run tagged "ssangyong" — no match).
  • Brand cues are word-boundary-aware so "Mercedes Benz" matches but
    "Mercedes Bencher" (hypothetical) does not.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Optional

import yaml

_ROOT = Path(__file__).resolve().parents[3]
_BRAND_YAML = _ROOT / "config" / "brand_domains.yaml"


def _build_alias_map() -> dict[str, str]:
    """Build alias.lower() → canonical brand name. Loaded once."""
    with _BRAND_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    alias_map: dict[str, str] = {}
    for entry in data.get("brands", []):
        canonical = entry.get("brand", "").strip()
        if not canonical:
            continue
        # Canonical name maps to itself
        alias_map[canonical.lower()] = canonical
        for alias in entry.get("aliases", []) or []:
            a = (alias or "").strip().lower()
            if a:
                # Don't overwrite if already mapped (first definition wins).
                # Prevents shared aliases like "Аватр" colliding silently.
                alias_map.setdefault(a, canonical)
    return alias_map


_ALIAS_MAP: dict[str, str] = _build_alias_map()


# Sorted longest-first so "great wall motor" is tried before "great wall".
_ALIASES_SORTED: list[str] = sorted(_ALIAS_MAP.keys(), key=len, reverse=True)


def reload_aliases() -> None:
    """Force-reload from YAML (testing / hot-config edits)."""
    global _ALIAS_MAP, _ALIASES_SORTED
    _ALIAS_MAP = _build_alias_map()
    _ALIASES_SORTED = sorted(_ALIAS_MAP.keys(), key=len, reverse=True)
    canonicalize_brand.cache_clear()


# Word boundary that handles Cyrillic + Latin both. We can't rely on
# ``\b`` because it doesn't fire between ASCII and Cyrillic letters.
def _bounded(alias: str) -> re.Pattern[str]:
    esc = re.escape(alias)
    return re.compile(r"(?:^|[^a-zа-яё0-9])" + esc + r"(?:[^a-zа-яё0-9]|$)",
                       re.IGNORECASE)


_BOUNDED_RX: dict[str, re.Pattern[str]] = {
    a: _bounded(a) for a in _ALIASES_SORTED
}


@lru_cache(maxsize=4096)
def canonicalize_brand(text: str) -> str:
    """Return canonical brand name of the SUBJECT, or empty string.

    Accepts EITHER a bare brand label ("ssangyong") OR free text where
    the brand may be embedded ("KGM Torres SUV launched in Russia").

    For bare labels: exact (lowercase) alias lookup.

    For free text — earliest-mention wins (the article SUBJECT is
    normally the first brand named), with length as tie-breaker so
    "BMW Alpina" beats bare "BMW" when both start at position 0.

    Pre-fix this preferred the alphabetically-first long alias, so
    "Ram Rumble Bee faster than BMW M3" was tagged BMW (last mention)
    instead of Ram (the actual subject) — a v41 audit regression.

    >>> canonicalize_brand("Ram Rumble Bee faster than BMW M3")
    'Ram'
    >>> canonicalize_brand("BMW Alpina Vision concept revealed")
    'BMW Alpina'
    >>> canonicalize_brand("Mercedes-AMG GT 4-Door")
    'Mercedes-AMG'
    >>> canonicalize_brand("Vw Tukan pickup")
    'Volkswagen'
    >>> canonicalize_brand("")
    ''
    """
    if not text:
        return ""
    t = text.strip().lower()

    # Fast path: exact match (label is the whole input)
    direct = _ALIAS_MAP.get(t)
    if direct is not None:
        return direct

    # Free-text scan: find ALL alias matches with position, pick the
    # earliest (subject of the headline) with length as tiebreaker
    # (so "bmw alpina" beats "bmw" when both start at 0).
    best_pos: int | None = None
    best_alias: str = ""
    for alias in _ALIASES_SORTED:
        if alias not in t:
            continue  # cheap prefilter before regex
        m = _BOUNDED_RX[alias].search(text)
        if not m:
            continue
        pos = m.start()
        if best_pos is None or pos < best_pos or (
            pos == best_pos and len(alias) > len(best_alias)
        ):
            best_pos = pos
            best_alias = alias
    return _ALIAS_MAP[best_alias] if best_alias else ""


def all_canonical_brands() -> set[str]:
    """Set of all known canonical names — for testing / coverage."""
    return set(_ALIAS_MAP.values())


def get_brand_domains(canonical: str) -> list[str]:
    """Press / OEM domains registered for ``canonical`` brand."""
    with _BRAND_YAML.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    for entry in data.get("brands", []):
        if entry.get("brand", "").lower() == canonical.lower():
            return list(entry.get("domains", []) or [])
    return []


def aliases_for(canonical: str) -> list[str]:
    """All known lowercase aliases for ``canonical`` (including itself).

    Used by code that needs to strip the brand-prefix from a free-text
    string like "ssangyong torres" → "torres" before re-prefixing with
    the canonical form. Returns longest-first so longer aliases match
    before shorter ones (e.g. "great wall motor" before "great wall").
    """
    canon_lower = canonical.lower()
    out = [a for a, c in _ALIAS_MAP.items() if c.lower() == canon_lower]
    out.sort(key=len, reverse=True)
    return out
