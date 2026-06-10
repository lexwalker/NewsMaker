"""Canonical verdict → miss-funnel stage mapping (peer-review §3).

Honest note: the rejection REASON is already logged and persisted today.
Every scored article lands in the SQLite cache with a ``verdict`` that
encodes WHICH step killed it and an ``article_reasons`` / ``llm_reason``
string with the specific detail (e.g. the matched blacklist phrase). What
was missing is a single source of truth that turns those verdict strings
into the funnel's death-stage decomposition, so the miss-funnel (and any
future blacklist-pruning) does not fragile-string-match on its own.

Funnel stages (peer-review §3 table):
  S1  source not in the 338-source list        (computed from URL domain;
                                                 NOT derivable from verdict)
  S2  source present, article never collected   (computed from ABSENCE in
                                                 the collected set)
  S3  collected, killed by a heuristic          (this module: with cause)
  S4  collected, passed heuristics, killed by AI
  S5  passed AI, died later (clustering / junk / dedup)  (computed downstream)
  S0  reached the editor table

This module covers what a single collected row's VERDICT tells us: S3
(with sub-cause), S4, a dedup collapse, a freshness drop, a fetch error,
or "accepted" (reached the pipeline → S0/S5 decided downstream). S1/S2 are
determined by the miss-funnel from presence/absence, not from a verdict.
"""

from __future__ import annotations

from dataclasses import dataclass

# Stage codes ---------------------------------------------------------
S3_HEURISTIC = "S3_heuristic"   # killed by a pre-LLM heuristic
S4_LLM = "S4_llm"               # passed heuristics, rejected by the LLM
COLLAPSED = "collapsed"         # deduped against another collected URL
STALE = "stale"                 # dropped by the freshness window
FETCH_ERROR = "fetch_error"     # could not download / extract
ACCEPTED = "accepted"           # reached the pipeline (S0/S5 decided later)
UNKNOWN = "unknown"             # verdict not recognised


# Heuristic sub-causes (all under S3). The string is a stable, queryable
# tag; the human detail (specific phrase / signals) stays in the row's
# article_reasons field.
_HEURISTIC_VERDICTS = {
    "Точно не новость (чёрный список)": "blacklist",
    "Точно не новость (не статья)": "not_article",
    "Точно не новость (не авто)": "off_topic",
    "Точно не новость (мульти-новость)": "multi_news",
    "Точно не новость (дзен-листикл)": "dzen_listicle",
    "Точно не новость (поставщик-абстракция)": "supplier_abstract",
}

_LLM_VERDICTS = {"Отклонено LLM"}

_COLLAPSED_VERDICTS = {
    "Отклонить (дубль)",
    "Отклонить (дубль финального URL)",
    "Отклонить (обработан ранее)",
}

_STALE_VERDICTS = {"Точно не новость (старая)"}

_FETCH_ERROR_VERDICTS = {
    "Отклонить (ошибка загрузки)",
    "Отклонить (не удалось извлечь)",
}

_ACCEPTED_VERDICTS = {"Точно новость", "Возможно новость"}


@dataclass(frozen=True)
class Outcome:
    """Where a collected row died, derived from its verdict."""

    stage: str          # one of the S*/state constants above
    cause: str          # sub-cause tag (e.g. "blacklist") or ""
    is_reject: bool     # True if the row did NOT reach the pipeline


def classify_outcome(verdict: str) -> Outcome:
    """Map a row ``verdict`` to its funnel death-stage + sub-cause.

    Accepted rows return ``ACCEPTED`` (is_reject=False) — whether they
    reached the editor table (S0) or died later in clustering (S5) is a
    downstream question this function cannot answer from the verdict.
    """
    v = (verdict or "").strip()
    if v in _ACCEPTED_VERDICTS:
        return Outcome(ACCEPTED, "", is_reject=False)
    if v in _HEURISTIC_VERDICTS:
        return Outcome(S3_HEURISTIC, _HEURISTIC_VERDICTS[v], is_reject=True)
    if v in _LLM_VERDICTS:
        return Outcome(S4_LLM, "llm", is_reject=True)
    if v in _COLLAPSED_VERDICTS:
        return Outcome(COLLAPSED, "dedup", is_reject=True)
    if v in _STALE_VERDICTS:
        return Outcome(STALE, "freshness", is_reject=True)
    if v in _FETCH_ERROR_VERDICTS:
        return Outcome(FETCH_ERROR, "fetch", is_reject=True)
    return Outcome(UNKNOWN, "", is_reject=True)
