"""LLM-as-editor — cluster-level editorial reasoning.

Pattern validated by scripts/_llm_editor_poc.py on v41 push:
  • 5/5 hand-merged dup pairs recovered (100% DUP recall)
  • 6/6 same-brand-different-event pairs preserved (100% distinct)
  • $0.047 for 12 brand groups via Haiku 4.5
  • Bonus: caught r25↔r41 Audi A2 pair I missed in manual audit

This module exposes the pattern as a reusable function that
build_news_clusters can call behind a feature flag. The PoC script
remains as a regression harness — pin it to known v41 data and use it
to verify any prompt / model changes don't break the empirical result.

Design notes
-------------
  • Pure function: takes article dicts, returns event-cluster dicts.
    No filesystem / DB side effects.
  • Anthropic client is injected (so tests can mock).
  • Graceful failure: on any LLM error, returns ``None`` so the caller
    falls back to lexical clustering — never worse than current.
  • Confidence threshold: only events with confidence >= 0.70 are
    kept; below that, articles stay in their original lexical cluster.
  • Cross-run hook: ``history`` parameter lets the caller pass recent
    brand-filtered articles from DedupStore (Stage 2b foundation).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    import anthropic


SECTIONS = [
    "Confirmed", "Local specifics", "Other news", "Rumors",
    "LCV news", "Economics", "Dealer news / Promo", "Motorshow",
]

# Default confidence threshold below which we DON'T trust LLM's merge.
# Calibrated on PoC: events at 0.85 were correct catches we'd otherwise
# miss; events below 0.7 are too speculative for an auto-merge.
DEFAULT_CONFIDENCE = 0.70

# Default model. Haiku 4.5 was validated in PoC; can be overridden by
# caller (e.g. Sonnet for tighter calibration runs).
DEFAULT_MODEL = "claude-haiku-4-5"

# Haiku 4.5 pricing per million tokens — kept here so cost can be
# reported by caller without re-importing settings.
_HAIKU_INPUT_PER_MTOK = 1.0
_HAIKU_OUTPUT_PER_MTOK = 5.0


@dataclass
class Event:
    """One editor-decided event grouping articles."""
    event_id: str
    summary: str
    section: str
    member_rows: list[int]
    primary_row: int
    confidence: float
    reasoning: str
    is_cross_run_dup: bool = False
    cross_run_match_url: str = ""

    @classmethod
    def from_tool_output(cls, d: dict) -> "Event":
        return cls(
            event_id=d.get("event_id", ""),
            summary=d.get("summary", ""),
            section=d.get("section", ""),
            member_rows=list(d.get("member_rows", []) or []),
            primary_row=d.get("primary_row", 0),
            confidence=float(d.get("confidence", 0.0)),
            reasoning=d.get("reasoning", ""),
            is_cross_run_dup=bool(d.get("is_cross_run_dup", False)),
            cross_run_match_url=d.get("cross_run_match_url", "") or "",
        )


@dataclass
class EditorResult:
    """Return type of cluster_group. ``events`` may be empty on LLM
    error — callers should fall back to their lexical clusters in
    that case. ``cost_usd`` and ``elapsed_s`` are always populated
    (zero on error)."""
    events: list[Event]
    cost_usd: float
    elapsed_s: float
    input_tokens: int = 0
    output_tokens: int = 0
    error: str = ""


# ── Prompt + tool schema ────────────────────────────────────────────

SYSTEM_PROMPT = """\
Ты — старший редактор автомобильного новостного агрегатора. На вход \
получаешь группу статей одного бренда из одного батча и (опционально) \
историю того же бренда за последние 14 дней. Твоя задача — разбить \
текущие статьи на отдельные СОБЫТИЯ, для каждого выбрать \
первоисточник + секцию, и пометить дубли с историей.

ЧТО ЕСТЬ «ОДНО СОБЫТИЕ»:
  • Одно реальное произошедшее (reveal, launch, recall, sales report, \
patent filing) может описываться несколькими статьями разными словами \
— ЭТО ОДНО событие.
  • Один и тот же бренд + ОДНА И ТА ЖЕ модель + одно и то же действие \
(reveal/launch/recall/...) = одно событие.
  • Лап-рекорды и performance-тесты — отдельное событие от reveal'а \
модели. Они идут в Other news (это performance PR, не запуск).

ЧТО НЕ ЕСТЬ «ОДНО СОБЫТИЕ»:
  • Разные модели одного бренда (Volvo EX40 ≠ Volvo XC60). Каждая \
своё событие.
  • Reveal + первый обзор после reveal'а — обычно ОДНО событие \
(reveal-неделя).
  • Reveal + отзыв-через-полгода — ДВА события.
  • Стратегическое заявление (next-gen electric) + конкретная машина \
этого поколения — обычно ДВА события (одно про планы, другое про \
машину).

CROSS-RUN ДУБЛИ:
  • Если статья из ТЕКУЩЕГО батча описывает событие, которое уже \
освещалось в истории за последние 14 дней — пометь \
is_cross_run_dup=true и укажи cross_run_match_url.
  • Перепост англоязычной новости через 2-14 дней — это cross-run \
дубль (даже если разные слова в заголовке).

ВЫБОР ПЕРВОИСТОЧНИКА (priority order):
  1. Если есть OEM-press (media.X.com, audi-mediacenter, \
media.mercedes-benz, media.stellantis, global.honda и т.д.) — берём.
  2. Если recall — берём NHTSA или mintrans.
  3. Если есть английский primary (carscoops, autoevolution, motor1, \
cnevpost, koreancarblog, topauto и др.) — берём.
  4. Русские агрегаторы (speedme, naavtotrasse, auto.mail.ru, kolesa, \
autoreview) — берём ТОЛЬКО если первых трёх нет.

ВЫБОР СЕКЦИИ:
  • Confirmed — официальный запуск/reveal модели от бренда. Сюда же \
ВСЕ модельные дебюты от российских OEM (AvtoVAZ, Sollers, UAZ, \
Moskvich, Lada, Aurus, Volga, Esteo, Tenet, Evolute и т.д.) — даже \
если событие на территории РФ. Решает СУБЪЕКТ статьи (модель + бренд \
+ дебют/reveal/сертификация), не география события.
  • Local specifics — про российский АВТОРЫНОК (общие тренды продаж, \
объёмы локализации, дилерская активность в целом, аналитика рынка). \
НЕ про конкретную модель — а про РЫНОК. Если статья называет одну \
модель и её запуск/обновление/сертификацию — это Confirmed, не Local.
  • LCV news — коммерческий транспорт (пикапы, фургоны, минивэны \
8+ мест, грузовики). Российские LCV-launches (SKM, Sollers SF5 \
фургон, Sollers SP7 минивэн) идут СЮДА, не в Confirmed.
  • Other news — глобальные не-российские новости про конкретную \
модель/бренд, не подходящие под Confirmed (sales stats иностранного \
рынка, patents без конкретной модели, корпоративные новости).
  • Rumors — слухи, шпионские фото, утечки, спекуляции, рендеры.
  • Economics — финансы брендов (квартальные отчёты, IPO, \
рефинансирование).
  • Dealer news / Promo — дилерские новости, акции.
  • Motorshow — motor-шоу события (анонс участия в шоу, презентация \
ОТДЕЛЬНО от запуска модели). Если статья про модельный дебют, \
заявленный на motor-show — это Confirmed, не Motorshow.

WORKED EXAMPLES (калибровка на v43 редакторских коррекциях):
  • «Sollers SP7 minivan to get four-seat version» → Confirmed
    (модельный дебют российского OEM)
  • «Deepal S05 SUV certified in Russia» → Confirmed
    (сертификация = официальный шаг бренда к запуску модели)
  • «UAZ Patriot refreshed model sent for certification» → Confirmed
  • «AvtoVAZ to showcase refreshed Niva at SPIEF» → Confirmed
    (модельный refresh, площадка вторична)
  • «Moskovich 3 crossover: evolution» → Confirmed
  • «Skoda new car sales tripled in Russia» → Local specifics
    (статистика рынка, не конкретная модель)
  • «Spring market trends in Russia» → Local specifics (тренд)
  • «AvtoVAZ to present new SKM model at Moscow motor show» → LCV news
    (SKM = commercial vehicle бренд → раздел LCV)
  • «Sollers SF5 van features revealed» → LCV news
    (фургон → LCV, даже если модельный дебют)
  • «Ford developed feature to prevent leaving items behind» → Other news
    (патент на общую функцию без модели, не Confirmed)
  • «Lexus scrapped plans for flagship EV» → Other news
    (ОТМЕНА модели — не дебют, поэтому Другие, не Confirmed)
  • «Why Volvo speeds up EV plans» → Other news
    (стратегия-рассуждение, не запуск модели)
  • «Exlantix appear on Esteo website» → Rumors
    (наблюдение телеграма о ребрендинге — догадка, не факт)

ПРАВИЛО НЕВРЕДЕНИЯ: если уверенность что два события на самом деле \
одно < 0.7 — оставь их раздельно. Лучше пропустить дубль, чем склеить \
разные сюжеты. Если уверенность что текущая статья — дубль с историей \
< 0.7 — НЕ помечай как cross-run-dup."""


EDITOR_TOOL: dict[str, Any] = {
    "name": "decide_event_clusters",
    "description": (
        "Group a brand's articles into distinct events. Same-event "
        "articles get merged. Different events stay separate."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "events": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "event_id": {
                            "type": "string",
                            "description": (
                                "Short snake_case id, e.g. "
                                "'gt_4door_reveal' or "
                                "'amg_electric_strategy'"
                            ),
                        },
                        "summary": {
                            "type": "string",
                            "description": "One-line event description",
                        },
                        "section": {
                            "type": "string",
                            "enum": SECTIONS,
                        },
                        "member_rows": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": (
                                "Sheet row numbers in this event "
                                "(must reference rows from input)"
                            ),
                        },
                        "primary_row": {
                            "type": "integer",
                            "description": (
                                "Which member row should be canonical "
                                "(OEM/regulator > industry-EN > "
                                "industry-RU > aggregator)"
                            ),
                        },
                        "confidence": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 1,
                            "description": (
                                "How confident this grouping is. "
                                "< 0.7 → consumer will keep articles "
                                "in their original buckets."
                            ),
                        },
                        "reasoning": {
                            "type": "string",
                            "description": (
                                "Why this group is one event (or what "
                                "makes a singleton distinct from "
                                "siblings)"
                            ),
                        },
                        "is_cross_run_dup": {
                            "type": "boolean",
                            "description": (
                                "True if this event duplicates a "
                                "history article from the last 14 days"
                            ),
                        },
                        "cross_run_match_url": {
                            "type": "string",
                            "description": (
                                "If is_cross_run_dup, the URL from "
                                "history this matches. Empty otherwise."
                            ),
                        },
                    },
                    "required": [
                        "event_id", "summary", "section",
                        "member_rows", "primary_row", "confidence",
                        "reasoning",
                    ],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["events"],
        "additionalProperties": False,
    },
}


# ── Public API ──────────────────────────────────────────────────────

def _render_articles(articles: list[dict]) -> str:
    """One block per article. Caller's dicts must have 'row', 'title',
    optional 'lede', 'section', 'url'."""
    parts = []
    for r in articles:
        row = r.get("row", "?")
        title = (r.get("title") or "").replace("\n", " | ")[:220]
        lede = (r.get("lede") or "")[:400].replace("\n", " ")
        sec = r.get("section", "")
        url = r.get("url", "")
        parts.append(
            f"[r{row}] section={sec}  url={url}\n"
            f"     title: {title}\n"
            f"     lede:  {lede}"
        )
    return "\n\n".join(parts)


def _render_history(history: list[dict]) -> str:
    """History blocks are condensed — caller only needs to spot dups."""
    if not history:
        return ""
    lines = ["История за последние 14 дней (тот же бренд):"]
    for h in history[:30]:  # cap to keep tokens bounded
        ts = (h.get("ts") or "")[:10]
        title = (h.get("title") or "").replace("\n", " | ")[:180]
        url = h.get("url", "")
        lede = (h.get("lede") or "")[:200].replace("\n", " ")
        lines.append(f"  [{ts}] {title}")
        lines.append(f"         {url}")
        if lede:
            lines.append(f"         lede: {lede}")
    return "\n".join(lines)


def cluster_group(
    *,
    brand: str,
    articles: list[dict],
    history: list[dict] | None = None,
    client: "anthropic.Anthropic | None" = None,
    model: str = DEFAULT_MODEL,
    confidence_threshold: float = DEFAULT_CONFIDENCE,
    max_tokens: int = 2000,
    timeout_s: float = 60.0,
) -> EditorResult:
    """Ask the LLM-as-editor to cluster ``articles`` for ``brand``.

    Parameters
    ----------
    brand : canonical brand name (used as label only — LLM also reads
            article titles).
    articles : list of dicts with at least 'row' and 'title'.
    history : recent same-brand articles from DedupStore (optional).
              Each dict needs 'url', 'title', 'ts'; 'lede' optional.
    client : pre-built anthropic.Anthropic. If None, builds one from
             environment.
    model : Anthropic model name. Default Haiku 4.5.
    confidence_threshold : events below this are dropped from output
                            (caller falls back to lexical).
    max_tokens : output token cap.
    timeout_s : LLM call timeout in seconds.

    Returns
    -------
    EditorResult with .events (possibly empty on error), .cost_usd,
    .elapsed_s, .error (empty string on success).
    """
    if not articles:
        return EditorResult(events=[], cost_usd=0.0, elapsed_s=0.0)
    if client is None:
        import anthropic
        client = anthropic.Anthropic()

    user_parts = [f"Бренд: {brand}"]
    if history:
        user_parts.append(_render_history(history))
    user_parts.append("Статьи из батча (нужно разбить на события):\n\n"
                       + _render_articles(articles))
    user_msg = "\n\n".join(user_parts)

    t0 = time.time()
    try:
        resp = client.messages.create(
            model=model,
            max_tokens=max_tokens,
            system=SYSTEM_PROMPT,
            tools=[EDITOR_TOOL],
            tool_choice={"type": "tool", "name": "decide_event_clusters"},
            messages=[{"role": "user", "content": user_msg}],
            timeout=timeout_s,
        )
    except Exception as e:  # noqa: BLE001 — keep pipeline alive
        elapsed = time.time() - t0
        return EditorResult(
            events=[], cost_usd=0.0, elapsed_s=elapsed,
            error=f"{type(e).__name__}: {e}"[:200],
        )

    elapsed = time.time() - t0
    in_tok = resp.usage.input_tokens
    out_tok = resp.usage.output_tokens
    cost = (in_tok * _HAIKU_INPUT_PER_MTOK
            + out_tok * _HAIKU_OUTPUT_PER_MTOK) / 1_000_000

    events: list[Event] = []
    for block in resp.content:
        if getattr(block, "type", None) == "tool_use":
            tool_input = block.input or {}
            for ev_dict in (tool_input.get("events") or []):
                ev = Event.from_tool_output(ev_dict)
                if ev.confidence < confidence_threshold:
                    # Keep low-confidence events but mark — caller
                    # decides what to do. We tag by ID prefix.
                    ev.event_id = f"LOWCONF__{ev.event_id}"
                # Validate member rows refer back to input
                input_rows = {a.get("row") for a in articles}
                ev.member_rows = [r for r in ev.member_rows
                                   if r in input_rows]
                if ev.primary_row not in input_rows and ev.member_rows:
                    ev.primary_row = ev.member_rows[0]
                if ev.member_rows:
                    events.append(ev)
            break

    return EditorResult(
        events=events, cost_usd=cost, elapsed_s=elapsed,
        input_tokens=in_tok, output_tokens=out_tok,
    )


def group_articles_by_brand(
    articles: list[dict],
    *,
    bucket_aliases: dict[str, str] | None = None,
) -> dict[str, list[dict]]:
    """Group articles by canonical brand. Returns {brand: [articles]}.

    ``bucket_aliases`` lets the caller merge canonical brands into a
    single bucket — e.g. {"Mercedes-AMG": "Mercedes-Benz"} so AMG and
    Benz articles cluster together (the editor groups them as one
    family). Unknown brands go into "_unknown" bucket.
    """
    from collections import defaultdict
    from news_agent.core.brand_canonical import canonicalize_brand

    bucket_aliases = bucket_aliases or {}
    out: dict[str, list[dict]] = defaultdict(list)
    for a in articles:
        text = (a.get("title") or "") + " " + (a.get("lede") or "")[:200]
        canon = canonicalize_brand(text) or "_unknown"
        bucket = bucket_aliases.get(canon, canon)
        out[bucket].append(a)
    return dict(out)
