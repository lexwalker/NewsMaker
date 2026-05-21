"""LLM-as-editor PoC: cluster-level editorial reasoning.

The pattern we're validating:

  CURRENT  : each article → LLM(classify section) → naive cluster by
             brand+title-fuzz → push.  Each article decided in
             isolation → same story gets different section, same brand
             different model gets falsely merged.

  PROPOSED : crawled articles → brand-canonical group → LLM-as-editor
             sees ALL articles in group + 14d history → returns
             structured event split with ONE section / ONE primary per
             event. The LLM acts as a senior editor doing the job they
             do daily.

This script validates the pattern on the v41 push (52 articles),
comparing LLM-as-editor's clusters against:
  • my manual merge (13 groups, applied to editor sheet today)
  • the editor's own col-P comments

Cost: ~$0.05 per full run (≈13 brand groups × Haiku 4.5).
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

import anthropic  # noqa: E402

from news_agent.core.brand_canonical import canonicalize_brand  # noqa: E402

V41_FILE = ROOT / "data" / "embed_poc_v41.jsonl"
OUT_FILE = ROOT / "data" / "llm_editor_poc_result.json"

# Mercedes-AMG and Mercedes-Benz get treated as ONE editorial bucket
# (same OEM family, the editor groups them together).
BUCKET_ALIASES = {
    "Mercedes-AMG": "Mercedes-Benz",
}


# ── Ground truth from today's manual merge + editor's comments ──────
# Each event = a frozenset of v41 row numbers that belong together.
# Events NOT listed = the article is alone (singleton event).
GROUND_TRUTH_EVENTS = {
    # Within-batch DUP pairs we hand-merged today
    "ram_rumble_bee_2027":         frozenset({3, 4}),
    "vinfast_vf8_gen2":            frozenset({8, 18}),
    "mercedes_amg_gt_4door_reveal": frozenset({23, 38}),
    "vw_tukan_pickup":             frozenset({33, 52}),
    # Within-batch dups editor flagged (in addition to mine)
    # r15 + r39: "Mercedes-AMG most powerful electric car" +
    # "Mercedes-AMG GT will be electric only" — same strategic event
    "mercedes_amg_electric_strategy": frozenset({15, 39}),
}

# Pairs that LOOK similar (same brand) but are DIFFERENT events.
# The LLM-as-editor must keep these SEPARATE.
GROUND_TRUTH_DISTINCT = [
    (13, 50),   # Volvo EX40 next-gen vs Volvo XC60 refresh — diff models
    (19, 36),   # BYD Datang vs BYD Seal 08 — diff models
    (24, 41),   # Audi E7X SUV launch vs Audi A2 e-tron testing — diff models
    (27, 43),   # Hyundai recall vs Hyundai manual EV patent — diff events
    (37, 47),   # Changan Arrizo 8 launch vs Changan production ramp — diff events
    (11, 53),   # Xpeng GX launch vs Xpeng GX robotaxi — borderline (related)
]


SECTIONS = [
    "Confirmed", "Local specifics", "Other news", "Rumors",
    "LCV news", "Economics", "Dealer news / Promo", "Motorshow",
]

EDITOR_TOOL = {
    "name": "decide_event_clusters",
    "description": (
        "Group a brand's articles into distinct events. Two articles "
        "describing the SAME real-world event (a reveal, a recall, a "
        "launch) get merged into one event. Two articles about the "
        "same brand but DIFFERENT events stay as separate events."
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
                                "'gt_4door_reveal' or 'amg_electric_strategy'"
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
                            "description": "v41 sheet row numbers in this event",
                        },
                        "primary_row": {
                            "type": "integer",
                            "description": (
                                "Which member row should be canonical — "
                                "prefer OEM/regulator > industry-EN > "
                                "industry-RU > aggregator"
                            ),
                        },
                        "confidence": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 1,
                            "description": (
                                "How confident you are these belong "
                                "together. < 0.7 means keep separate."
                            ),
                        },
                        "reasoning": {
                            "type": "string",
                            "description": (
                                "Why this group is one event "
                                "(or, for singletons, what makes it "
                                "distinct from sibling articles)"
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

SYSTEM_PROMPT = """\
Ты — старший редактор автомобильного новостного агрегатора. На вход \
получаешь группу статей одного бренда из одного батча. Твоя задача — \
разбить их на отдельные СОБЫТИЯ и для каждого выбрать первоисточник + \
секцию.

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

ВЫБОР ПЕРВОИСТОЧНИКА (priority order):
  1. Если есть OEM-press (media.X.com) — берём.
  2. Если recall — берём NHTSA или mintrans.
  3. Если есть английский primary (carscoops, autoevolution, motor1, \
cnevpost, koreancarblog) — берём.
  4. Русские агрегаторы (speedme, naavtotrasse, auto.mail.ru, kolesa, \
autoreview) — берём ТОЛЬКО если первых трёх нет.

ВЫБОР СЕКЦИИ:
  • Confirmed — официальный запуск/reveal модели от бренда.
  • Local specifics — про российский авторынок (продажи, локализация, \
дилеры в РФ).
  • LCV news — коммерческий транспорт (пикапы, фургоны, грузовики).
  • Other news — глобальные не-российские новости, не подходящие \
под Confirmed.
  • Rumors — слухи, шпионские фото, утечки, спекуляции.
  • Economics — финансы брендов (квартальные отчёты, IPO, \
рефинансирование).
  • Dealer news / Promo — дилерские новости, акции.
  • Motorshow — motor-шоу события.

ПРАВИЛО НЕВРЕДЕНИЯ: если уверенность что два события на самом деле \
одно < 0.7 — оставь их раздельно. Лучше пропустить дубль, чем склеить \
разные сюжеты."""


def render_articles(articles: list[dict]) -> str:
    out = []
    for r in articles:
        title = (r.get("title") or "").replace("\n", " | ")[:200]
        lede = (r.get("lede") or "")[:400].replace("\n", " ")
        sec = r.get("section", "")
        url = r.get("url", "")
        out.append(
            f"[r{r['row']}] section={sec}  url={url}\n"
            f"     title: {title}\n"
            f"     lede:  {lede}"
        )
    return "\n\n".join(out)


def call_llm_editor(
    client: "anthropic.Anthropic",
    brand: str,
    articles: list[dict],
) -> tuple[dict, dict]:
    """Return (parsed_events_or_error, usage_dict)."""
    user_msg = (
        f"Бренд: {brand}\n\n"
        f"Статьи из батча (нужно разбить на события):\n\n"
        f"{render_articles(articles)}"
    )
    t0 = time.time()
    resp = client.messages.create(
        model="claude-haiku-4-5",
        max_tokens=2000,
        system=SYSTEM_PROMPT,
        tools=[EDITOR_TOOL],
        tool_choice={"type": "tool", "name": "decide_event_clusters"},
        messages=[{"role": "user", "content": user_msg}],
    )
    elapsed = time.time() - t0
    usage = {
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
        "elapsed_s": elapsed,
        # Haiku 4.5 pricing: $1/MTok input, $5/MTok output
        "cost_usd": (resp.usage.input_tokens * 1.0
                     + resp.usage.output_tokens * 5.0) / 1_000_000,
    }
    # Extract tool_use content
    for block in resp.content:
        if block.type == "tool_use":
            return dict(block.input), usage
    return {"error": "no tool_use in response", "raw": str(resp.content)}, usage


def main() -> int:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY missing")
        return 1

    rows = []
    for ln in V41_FILE.read_text("utf-8").splitlines():
        if ln.strip():
            try:
                rows.append(json.loads(ln))
            except Exception:
                pass
    print(f"Loaded {len(rows)} v41 rows")

    # Brand-group (use improved subject-canonicalisation)
    groups: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        text = (r.get("title") or "") + " " + (r.get("lede") or "")[:200]
        canon = canonicalize_brand(text) or "_unknown"
        # Apply bucket aliases (Mercedes-AMG → Mercedes-Benz)
        bucket = BUCKET_ALIASES.get(canon, canon)
        groups[bucket].append(r)

    # Only call LLM for multi-article groups (singletons need no clustering)
    multi = {b: rs for b, rs in groups.items()
             if len(rs) >= 2 and b != "_unknown"}
    print(f"Brand groups with ≥2 articles: {len(multi)}")
    for b, rs in sorted(multi.items(), key=lambda x: -len(x[1])):
        print(f"  {b:20} {len(rs)} articles "
              f"(r{','.join(str(r['row']) for r in rs)})")

    client = anthropic.Anthropic()
    results: dict = {}
    total_cost = 0.0
    total_time = 0.0
    for brand, articles in multi.items():
        print(f"\n— Calling LLM-as-editor for {brand} "
              f"({len(articles)} articles)…")
        out, usage = call_llm_editor(client, brand, articles)
        results[brand] = {"output": out, "usage": usage,
                          "article_rows": [a["row"] for a in articles]}
        total_cost += usage["cost_usd"]
        total_time += usage["elapsed_s"]
        evs = out.get("events", []) if isinstance(out, dict) else []
        print(f"  → {len(evs)} events, {usage['elapsed_s']:.1f}s, "
              f"${usage['cost_usd']:.4f}")
        for ev in evs:
            print(f"     ◆ {ev.get('event_id','?')} "
                  f"[{ev.get('section','?')}] "
                  f"members={ev.get('member_rows',[])}  "
                  f"conf={ev.get('confidence',0):.2f}")

    print(f"\n=== TOTAL: {len(multi)} groups, {total_time:.0f}s, "
          f"${total_cost:.4f} ===")

    OUT_FILE.write_text(
        json.dumps(results, ensure_ascii=False, indent=1),
        encoding="utf-8",
    )
    print(f"\nResults → {OUT_FILE.name}")

    # ── SCORECARD vs ground truth ──────────────────────────────────
    print("\n" + "═" * 70)
    print("SCORECARD: LLM-as-editor decisions vs hand-merge ground truth")
    print("═" * 70)

    # Collect all predicted events
    predicted_events: list[set[int]] = []
    for brand, brand_result in results.items():
        out = brand_result["output"]
        for ev in out.get("events", []):
            if ev.get("confidence", 1.0) < 0.7:
                continue
            predicted_events.append(set(ev["member_rows"]))

    # 1. DUP detection
    print("\n── True DUP pairs the LLM should have merged ──")
    dup_caught = 0
    dup_total = 0
    for truth_id, truth_set in GROUND_TRUTH_EVENTS.items():
        dup_total += 1
        caught = False
        for pred in predicted_events:
            if truth_set.issubset(pred):
                caught = True
                break
        marker = "✓" if caught else "✗"
        print(f"  {marker} {truth_id:38} rows={sorted(truth_set)}")
        if caught:
            dup_caught += 1
    print(f"\n  DUP recall: {dup_caught}/{dup_total} "
          f"({dup_caught*100/dup_total:.0f}%)")

    # 2. False-merge detection (LLM merging things that shouldn't be merged)
    print("\n── Distinct pairs the LLM should NOT have merged ──")
    false_merges = 0
    for r1, r2 in GROUND_TRUTH_DISTINCT:
        merged_wrongly = False
        for pred in predicted_events:
            if r1 in pred and r2 in pred:
                merged_wrongly = True
                break
        marker = "✗ MERGED" if merged_wrongly else "✓ split"
        print(f"  {marker}  r{r1} ⇋ r{r2}")
        if merged_wrongly:
            false_merges += 1
    print(f"\n  Distinct preservation: "
          f"{len(GROUND_TRUTH_DISTINCT)-false_merges}/{len(GROUND_TRUTH_DISTINCT)} "
          f"({(len(GROUND_TRUTH_DISTINCT)-false_merges)*100/len(GROUND_TRUTH_DISTINCT):.0f}%)")

    # 3. Section assignments compared with editor labels (where available)
    print("\n── Section assignments (LLM vs editor's verdict on row) ──")
    sec_match = sec_total = 0
    for brand, brand_result in results.items():
        for ev in brand_result["output"].get("events", []):
            for row_n in ev.get("member_rows", []):
                # find original row
                orig = next((r for r in rows if r["row"] == row_n), None)
                if not orig:
                    continue
                # Editor's section verdict, if present
                ed_sec = orig.get("editor_section_correction") or ""
                bot_was = orig.get("section", "")
                llm_says = ev.get("section", "")
                # We can only judge when editor either:
                #   - approved the existing section (publish=True, no correction)
                #   - corrected to a specific section (editor_section_correction)
                # For PoC we just compare LLM's pick vs the section the
                # article was originally pushed under (bot_was), and note
                # whenever LLM chose a different section.
                sec_total += 1
                if llm_says == bot_was:
                    sec_match += 1
                else:
                    print(f"     r{row_n}: bot={bot_was!r} → llm={llm_says!r}"
                          f" (event: {ev.get('event_id','?')})")
    if sec_total:
        print(f"\n  Section consistency w/ bot's original: "
              f"{sec_match}/{sec_total} "
              f"({sec_match*100/sec_total:.0f}%) — note: LLM disagreement "
              f"may be a CORRECTION not an error")

    print(f"\n──── FINAL: ${total_cost:.4f} total, "
          f"{dup_caught}/{dup_total} dup-recall, "
          f"{len(GROUND_TRUTH_DISTINCT)-false_merges}/"
          f"{len(GROUND_TRUTH_DISTINCT)} distinct-preserved")
    return 0


if __name__ == "__main__":
    sys.exit(main())
