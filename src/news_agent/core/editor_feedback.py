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


# Complaints that mean «this story should not have been here at all» — the
# editor's time was wasted. Distinguished from the ones below because they are
# the only ones that make a row a mistake.
WASTED = ("дубль", "не нужно")
# Complaints that mean «the story is right, the metadata is not» — a section to
# switch, a source to swap, a headline to translate. The operator counted the
# aug-07 week by hand and read these as CORRECT rows, and he is right to: a
# wrong section costs five seconds, a duplicate costs the whole read. Lumping
# them together was hiding a 4x difference (93 wasted rows vs 18 fixable ones).
FIXABLE = ("не та секция", "не тот источник", "нужен перевод")


def classify_row(row: dict) -> str:
    """One of: clean | fixable | wasted | unparsed.

    `unparsed` is the point of this function. row_errors returns an empty list
    both for «ок» and for a comment whose wording the parser has never seen,
    and precision_from_feedback used to read both as clean — so on the aug-07
    week 48 unrecognised complaints were counted as successes and 60 real «ок»
    were reported as 108. An unrecognised comment is not evidence of anything;
    it must be visible and counted apart, never silently on the good side.
    """
    errs = set(row_errors(row))
    if errs & set(WASTED):
        return "wasted"
    if errs & set(FIXABLE):
        return "fixable"
    return "clean" if _looks_approving(row) else "unparsed"


_APPROVE_WORDS = ("ок", "ok", "окей", "да", "постим", "берем", "берём",
                  "хорошо", "норм", "годится", "пойдет", "пойдёт")


def _looks_approving(row: dict) -> bool:
    """True when the editor's own words say yes, not merely 'nothing matched'.

    The distinction the parser already draws and nobody was reading:
    label_publish is True on an explicit «ок», False on a recognised
    rejection, and None when the wording meant nothing to it. Only True is
    evidence of approval."""
    if row.get("label_publish") is True:
        return True
    if isinstance(row.get("raw_signals"), dict):
        if row["raw_signals"].get("approved"):
            return True
    c = (row.get("editor_comment") or "").strip().lower()
    if not c:
        return False
    head = c.replace("\n", ",").split(",")[0].split(".")[0].strip(" !?.:;")
    return head in _APPROVE_WORDS or head.startswith(("ок ", "ok ", "вроде ок"))


def precision_from_feedback(rows: list[dict]) -> dict:
    """Of the commented rows, how many are clean (no editor objection).

    `rows` are editor-feedback records (provenance == 'editor_sheet'),
    already windowed by the caller. Returns clean/with_err/total + rate +
    a by-type breakdown. is_biased flags that this is a pessimistic floor."""
    buckets = {"clean": 0, "fixable": 0, "wasted": 0, "unparsed": 0}
    by_type: dict[str, int] = {}
    for r in rows:
        buckets[classify_row(r)] += 1
        for e in row_errors(r):
            by_type[e] = by_type.get(e, 0) + 1
    total = sum(buckets.values())
    # The headline is what the OPERATOR counts as a correct row: the story
    # belonged in the feed. A wrong section or a swapped source is a fix, not a
    # mistake — he counted the aug-07 week that way by hand and the reading is
    # the right one. Unparsed stays out of the numerator entirely; it is not
    # evidence either way, and the old code's habit of treating it as success
    # is exactly what made 60 «ок» look like 108.
    right = buckets["clean"] + buckets["fixable"]
    judged = total - buckets["unparsed"]
    return {
        "metric": "precision_editor",
        "hit": right, "total": judged,
        "rate": (right / judged if judged else 0.0),
        "clean": buckets["clean"],           # editor said yes, nothing to fix
        "fixable": buckets["fixable"],       # right story, wrong metadata
        "wasted": buckets["wasted"],         # should not have been shown
        "unparsed": buckets["unparsed"],     # wording the parser cannot read
        "commented": total,
        "with_err": buckets["wasted"] + buckets["fixable"],
        "by_type": by_type,
        "is_biased": True,   # unmarked rows are not in here at all
    }
