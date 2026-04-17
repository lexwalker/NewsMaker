"""Fuzzy-title dedup — pure function over a title and the known-title set."""

from __future__ import annotations

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
