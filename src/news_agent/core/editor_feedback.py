"""Editor-comment precision — the HONEST 'is what we showed correct?' axis.

The archive-match `found_right` in weekly_kpi answers a *different* question
("did the editor publish exactly this story, title-matchable in the archive?")
and is a strict LOWER bound — it gets dragged down by cross-language match
failures, the editor not publishing everything good, and prog-gap days.

Precision by editor comment is the cleaner signal the editor asked for: of
the rows WE pushed, on how many did the editor leave NO complaint. One
definition, shared by scorecard.py and weekly_kpi.py so they can't drift.

Honesty caveat baked in: the editor comments mostly on PROBLEM rows and
passes clean ones silently, so this is a pessimistic floor on true precision
(`is_biased=True`). It is still the right axis for week-over-week movement.
"""

from __future__ import annotations


def row_errors(row: dict) -> list[str]:
    """The error tags the editor left on one pushed row (may be several).

    Mirrors the editor's free-text comment parse (sync_editor_feedback):
      дубль / не та секция / не тот источник / не нужно / нужен перевод.
    A row with an empty list is 'clean' (editor raised no objection)."""
    errs: list[str] = []
    if row.get("label_dup_within") or row.get("label_dup_cross_run"):
        errs.append("дубль")
    if row.get("label_section"):
        errs.append("не та секция")
    if row.get("label_wrong_primary"):
        errs.append("не тот источник")
    if (row.get("label_publish") is False
            and not (row.get("label_dup_within")
                     or row.get("label_dup_cross_run"))):
        errs.append("не нужно")
    if row.get("label_needs_translation"):
        errs.append("нужен перевод")
    return errs


def precision_from_feedback(rows: list[dict]) -> dict:
    """Of the commented rows, how many are clean (no editor objection).

    `rows` are editor-feedback records (provenance == 'editor_sheet'),
    already windowed by the caller. Returns clean/with_err/total + rate +
    a by-type breakdown. is_biased flags that this is a pessimistic floor."""
    clean = with_err = 0
    by_type: dict[str, int] = {}
    for r in rows:
        errs = row_errors(r)
        if errs:
            with_err += 1
            for e in errs:
                by_type[e] = by_type.get(e, 0) + 1
        else:
            clean += 1
    total = clean + with_err
    return {
        "metric": "precision_editor",
        "hit": clean, "total": total,
        "rate": (clean / total if total else 0.0),
        "with_err": with_err, "by_type": by_type,
        "is_biased": True,   # editor flags problems, passes clean silently
    }
