"""Turn miss-funnel output into the 'Непокрытые (анализ)' sheet — a table of
the editor publications we did NOT surface, each tagged with WHERE it died
(S1-S4) and a recommendation for how to close the gap.

Two recommendation sources, by design:
  * deterministic — always available, derived from the funnel stage/cause
    (e.g. "источника нет в списке — рассмотреть добавление"). The table is
    fully useful with NO LLM call.
  * AI (optional) — a per-SOURCE analysis for S1/S2 (the coverage gaps): is
    this domain worth adding, or why might our crawl be missing it. Per
    source, not per article: the fix is at the source level, and one batched
    call stays cheap + honest (the model can reason about a domain + sample
    titles; it cannot know our crawl internals, so S2 recs are hypotheses).

Pure logic only — the script does the Sheets I/O and the LLM call.
"""

from __future__ import annotations

from news_agent.core.miss_funnel import S1, S2, S3, S4

# Stages that are a real MISS (the editor published it, we did not surface it).
MISS_STAGES = (S1, S2, S3, S4)
_STAGE_ORDER = {s: i for i, s in enumerate(MISS_STAGES)}

STAGE_RU = {
    S1: "S1 · источника нет в списке",
    S2: "S2 · источник есть, не собрали",
    S3: "S3 · собрали, убила эвристика",
    S4: "S4 · собрали, отклонил ИИ",
}

HEADER = ["Дата", "Раздел", "Заголовок", "Стадия", "Причина",
          "Домен", "Похоже у нас (%)", "URL", "ИИ-рекомендация"]


def misses(rows: list) -> list:
    """The funnel rows that are actual misses (drop the ACCEPTED ones)."""
    return [r for r in rows if r.stage in MISS_STAGES]


def domains_to_analyse(rows: list, max_titles: int = 3) -> list[dict]:
    """Unique (stage, domain) groups for S1/S2 with sample titles + counts —
    the input to the AI source analysis. S3/S4 are classifier-side with a
    deterministic cause, so they are NOT sent to the AI."""
    groups: dict[tuple, dict] = {}
    for r in rows:
        if r.stage not in (S1, S2):
            continue
        dom = r.pub.domain or "(нет-url)"
        g = groups.setdefault(
            (r.stage, dom),
            {"stage": r.stage, "domain": dom, "count": 0, "titles": []})
        g["count"] += 1
        if len(g["titles"]) < max_titles:
            g["titles"].append(r.pub.display_title[:120])
    return sorted(groups.values(), key=lambda g: -g["count"])


def deterministic_rec(row) -> str:
    """A useful recommendation with NO AI call (graceful fallback)."""
    dom = row.pub.domain or "(нет-url)"
    if row.stage == S1:
        return f"Источника «{dom}» нет в списке — рассмотреть добавление."
    if row.stage == S2:
        return f"Источник «{dom}» есть — проверить RSS/скоринг/пагинацию."
    if row.stage == S3:
        return f"Эвристика отклонила ({row.cause}) — проверить правило."
    if row.stage == S4:
        return "ИИ отклонил — кандидат в разметку для обучения."
    return ""


def build_sheet_rows(rows: list, recs_by_key: dict | None = None) -> list[list]:
    """Build the data matrix (list of cell-rows) for the sheet, grouped by
    stage (S1→S4). recs_by_key maps (stage, domain) → AI recommendation; any
    row without one falls back to the deterministic recommendation."""
    recs_by_key = recs_by_key or {}
    ms = sorted(misses(rows),
                key=lambda r: (_STAGE_ORDER.get(r.stage, 9), r.pub.domain or ""))
    out = []
    for r in ms:
        dom = r.pub.domain or "(нет-url)"
        rec = recs_by_key.get((r.stage, dom)) or deterministic_rec(r)
        score = f"{r.score:.0f}" if r.match_method == "fuzzy" else ""
        out.append([
            r.pub.date, r.pub.section, r.pub.display_title[:300],
            STAGE_RU.get(r.stage, r.stage), r.cause, dom, score,
            r.pub.url, rec,
        ])
    return out
