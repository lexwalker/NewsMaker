"""Rejection-labelling ritual — pure logic (peer-review §2).

The retrieval A/B proved negatives are a precision lever but their effect is
small at 88→186; we need a STREAM of fresh editor decisions. The ritual:
sample what the BOT REJECTED, the editor binary-labels each "нужно / не
нужно", and the labels route three ways:

  не нужно  → confirmed_negative  — editor agrees → grows the negative base
  нужно     → false_reject        — bot was WRONG → a recall hole (+ a
                                     positive) + tells us which heuristic
                                     wrongly killed live content (S3 фразы)
  (blank)   → skip

Pure functions only (sampling stratification + label routing); the scripts
do the SQLite + Sheets I/O. Kept dependency-free for unit tests.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

CONFIRMED_NEGATIVE = "confirmed_negative"
FALSE_REJECT = "false_reject"
SKIP = "skip"

# Editor writes a free-ish "Нужно?" cell; accept the obvious spellings.
_YES = {"да", "нужно", "нужна", "нужен", "yes", "y", "+", "1", "ok", "ок"}
_NO = {"нет", "не нужно", "ненужно", "не нужна", "no", "n", "-", "0"}


def route_reject_label(editor_verdict: str) -> str:
    """Map the editor's 'Нужно?' cell to a routing decision.

    The sampled rows were all bot-REJECTED, so 'нужно' means the bot was
    wrong (false reject), 'не нужно' means the editor confirms the reject.
    """
    v = (editor_verdict or "").strip().lower()
    if v in _YES:
        return FALSE_REJECT
    if v in _NO:
        return CONFIRMED_NEGATIVE
    return SKIP


def stratified_sample(
    items: list[Any],
    key_fn: Callable[[Any], str],
    per_bucket: int,
    shuffle: Callable[[list], None],
    exclude: Callable[[Any], bool] | None = None,
    total: int | None = None,
) -> list[Any]:
    """Sample up to ``per_bucket`` items from each bucket (keyed by
    ``key_fn``), so every reject CAUSE (blacklist / off_topic / not_article
    / llm …) is represented — not just the largest. ``shuffle`` is injected
    (seeded in the script) so the pick is reproducible and tests are
    deterministic. ``exclude`` drops already-labelled rows.

    With ``total`` set, take items round-robin across buckets up to ``total``
    — small buckets are exhausted first, the rest filled from larger ones,
    so we hit the target count while keeping cause diversity.
    """
    buckets: dict[str, list] = defaultdict(list)
    for it in items:
        if exclude and exclude(it):
            continue
        buckets[key_fn(it)].append(it)
    capped = {}
    for key, group in buckets.items():
        shuffle(group)
        capped[key] = group[:per_bucket]
    if total is None:
        out: list = []
        for _key in sorted(capped):
            out.extend(capped[_key])
        return out
    # round-robin one per bucket per round until we reach `total`
    out = []
    keys = sorted(capped)
    i = 0
    while len(out) < total and any(i < len(capped[k]) for k in keys):
        for k in keys:
            if i < len(capped[k]) and len(out) < total:
                out.append(capped[k][i])
        i += 1
    return out


def summarise_labels(routed: list[tuple[str, dict]]) -> dict:
    """Aggregate routed (decision, row) pairs into ritual stats:
    confirmed-negatives added, false-rejects (recall holes) by the
    heuristic that killed them, and skips."""
    neg = [r for d, r in routed if d == CONFIRMED_NEGATIVE]
    fr = [r for d, r in routed if d == FALSE_REJECT]
    skip = [r for d, r in routed if d == SKIP]
    fr_by_cause: dict[str, int] = defaultdict(int)
    for r in fr:
        fr_by_cause[r.get("cause", "?")] += 1
    return {
        "labeled": len(neg) + len(fr),
        "confirmed_negative": len(neg),
        "false_reject": len(fr),
        "skipped": len(skip),
        "false_reject_by_cause": dict(fr_by_cause),
    }
