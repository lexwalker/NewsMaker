"""Offline eval harness — turns "костыли or not" into numbers.

Runs the SAME decision functions the prog uses, against the frozen
labeled set (data/eval_set.jsonl), with NO fetching:

  reject if blacklist_hit(title)            → predicted publish=False
  else editorial_review(title, body)        → predicted publish/section
  heuristic_section override on accept      (mirrors the live pipeline)

Metrics:
  • publish-recall      of label_publish=True (hard), % bot publishes
  • false-reject-rate   of label_publish=True, % bot wrongly rejected
  • publish-precision   of label rows the bot publishes, % truly +
                        (computed on the 116 explicit col-P negatives
                        + positives — the discriminating subset)
  • section-accuracy    of correctly-published, % section == label
Soft-labelled rows are EXCLUDED from precision/recall (hedged verdicts).

LLM results are content+prompt-hash cached (data/eval_llm_cache.json):
re-run after a heuristic-only change = $0. --fast skips the LLM
entirely (heuristic layer only, instant). --sample N caps the LLM rows.

Outputs a scorecard, a diff list (every disagreement), and appends one
line to data/eval_history.jsonl for trend tracking.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from news_agent.adapters.llm.base import EDITORIAL_REVIEW_SYSTEM  # noqa: E402
from news_agent.adapters.llm.factory import make_llm_client  # noqa: E402
from news_agent.core.config_loader import Blacklist, load_sections  # noqa: E402
from news_agent.core.heuristic_relevance import (  # noqa: E402
    blacklist_hit,
    heuristic_section,
)
from news_agent.core.models import RawArticle  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

DATA = ROOT / "data"
EVAL_SET = DATA / "eval_set.jsonl"      # v1 — legacy schema (build_eval_set.py)
EVAL_SET_V2 = DATA / "eval_set_v2.jsonl"  # v2 — rich-label schema (sync_editor_feedback.py)
LLM_CACHE = DATA / "eval_llm_cache.json"
HISTORY = DATA / "eval_history.jsonl"

# Cache key includes a prompt fingerprint so a prompt edit auto-invalidates.
_PROMPT_FP = hashlib.sha1(
    EDITORIAL_REVIEW_SYSTEM.encode("utf-8")
).hexdigest()[:12]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Offline classification eval")
    p.add_argument("--fast", action="store_true",
                   help="Heuristic layer only — no LLM, instant, $0")
    p.add_argument("--sample", type=int, default=0,
                   help="Cap LLM-evaluated rows (0 = all)")
    p.add_argument("--eval-file", default="",
                   help="Alternate jsonl (e.g. a fixed strat subset)")
    p.add_argument("--diff", type=int, default=25,
                   help="How many disagreements to print")
    p.add_argument("--tag", default="",
                   help="Label this run in eval_history.jsonl")
    p.add_argument("--predictions", default="",
                   help="Dump per-row predictions to PATH (for eval_diff.py)")
    return p.parse_args()


def _load_cache() -> dict:
    if LLM_CACHE.exists():
        try:
            return json.loads(LLM_CACHE.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            return {}
    return {}


def _load_jsonl(p: Path) -> list[dict]:
    """Safe-load a jsonl file, skip malformed lines."""
    if not p.exists():
        return []
    out: list[dict] = []
    for ln in p.read_text(encoding="utf-8").splitlines():
        if not ln.strip():
            continue
        try:
            out.append(json.loads(ln))
        except (json.JSONDecodeError, ValueError):
            continue
    return out


def _merge_eval_sets(v1: list[dict], v2: list[dict]) -> list[dict]:
    """Combine v1 + v2 eval sets, preferring v2 when same content key.

    v2 has richer labels (label_dup_*, label_wrong_primary, ...);
    v1 has section_at_push + body content for older entries.
    De-dup by title+url so we don't double-count when sync re-captures
    a comment that build_eval_set already saw.
    """
    def key(r: dict) -> str:
        return (
            (r.get("title", "")[:120].lower().strip() + "|" +
             r.get("url", "")[:120])
        )
    v2_keys = {key(r) for r in v2}
    merged = list(v2)  # v2 wins on collision
    for r in v1:
        if key(r) not in v2_keys:
            merged.append(r)
    return merged


def main() -> int:
    args = _parse_args()
    if args.eval_file:
        rows = _load_jsonl(Path(args.eval_file))
        if not rows:
            print(f"{args.eval_file} missing or empty")
            return 2
    else:
        v1 = _load_jsonl(EVAL_SET)
        v2 = _load_jsonl(EVAL_SET_V2)
        rows = _merge_eval_sets(v1, v2)
        if not rows:
            print("no eval data — run build_eval_set.py + "
                  "sync_editor_feedback.py")
            return 2
        print(f"eval data: v1={len(v1)} + v2={len(v2)} → merged={len(rows)}")
    # strict set excludes hedged ("soft") verdicts
    strict = [r for r in rows if not r.get("soft")]
    bl = Blacklist()
    sections = load_sections()
    section_names = {s.name for s in sections}

    client = None
    cache = _load_cache()
    cache_dirty = False
    if not args.fast:
        client = make_llm_client(get_settings())

    llm_budget = args.sample if args.sample > 0 else len(strict)
    llm_used = 0
    spent = 0.0

    # confusion accumulators
    tp = fp = tn = fn = 0          # publish prediction vs label
    sec_ok = sec_tot = 0
    false_rejects: list[dict] = []
    false_accepts: list[dict] = []
    sec_misses: list[dict] = []
    per_row: list[dict] = []        # for --predictions / eval_diff.py
    skipped_llm = 0

    t0 = time.time()
    for r in strict:
        title = r["title"]
        body = r.get("body") or title
        lab_pub = r.get("label_publish")
        if lab_pub is None:
            continue

        raw = RawArticle(url=r.get("url") or "https://x.example/a",
                          title=title, body=body, source_name="eval",
                          source_url="https://x.example/")
        # Stage 1: hard blacklist → reject
        if blacklist_hit(raw, bl).hit:
            pred_pub, pred_sec = False, ""
        elif args.fast:
            # heuristic-only proxy: heuristic_section gives a section →
            # treat as publish; None → "defer" (count as publish, since
            # the LLM is what rejects — fast mode can't model that).
            h = heuristic_section(title=title, body_excerpt=body, domain="")
            pred_pub = True
            pred_sec = h.section if h else ""
        else:
            ck = hashlib.sha1(
                f"{_PROMPT_FP}|{title}|{body[:600]}".encode("utf-8")
            ).hexdigest()
            if ck in cache:
                rv = cache[ck]
            elif llm_used < llm_budget:
                try:
                    review, u = client.editorial_review(
                        title=title, body=body, sections=sections,
                        portal_country="Russia")
                    spent += u.cost_usd
                    rv = {"pub": bool(review.should_publish),
                          "sec": review.section or ""}
                    cache[ck] = rv
                    cache_dirty = True
                    llm_used += 1
                except Exception as e:  # noqa: BLE001
                    print(f"  LLM err {type(e).__name__} — skipped")
                    skipped_llm += 1
                    continue
            else:
                skipped_llm += 1
                continue
            pred_pub = rv["pub"]
            pred_sec = rv["sec"]
            if pred_pub:
                h = heuristic_section(title=title, body_excerpt=body,
                                      domain="")
                if h and h.section in section_names:
                    pred_sec = h.section

        # per-row prediction snapshot (stable id so eval_diff can align
        # the same article across two runs / classifier versions)
        row_id = r.get("id") or hashlib.sha1(
            title.encode("utf-8")).hexdigest()[:16]
        per_row.append({
            "id": row_id,
            "title": title[:120],
            "lab_pub": bool(lab_pub),
            "pred_pub": bool(pred_pub),
            "lab_sec": r.get("label_section") or "",
            "pred_sec": pred_sec or "",
        })

        # tally publish confusion
        if lab_pub and pred_pub:
            tp += 1
        elif lab_pub and not pred_pub:
            fn += 1
            false_rejects.append(r)
        elif (not lab_pub) and pred_pub:
            fp += 1
            false_accepts.append(r)
        else:
            tn += 1

        # section accuracy on rows both sides publish & label has section
        if lab_pub and pred_pub and r.get("label_section"):
            sec_tot += 1
            if pred_sec == r["label_section"]:
                sec_ok += 1
            else:
                sec_misses.append(
                    {**r, "pred_sec": pred_sec})

    if cache_dirty:
        LLM_CACHE.write_text(
            json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    recall = tp / (tp + fn) if (tp + fn) else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    frr = fn / (tp + fn) if (tp + fn) else 0.0
    sec_acc = sec_ok / sec_tot if sec_tot else 0.0
    mode = "FAST(heuristic)" if args.fast else "FULL(+LLM)"

    print(f"\n=== EVAL {mode} — {len(strict)} strict rows "
          f"({time.time()-t0:.0f}s, ${spent:.4f}, "
          f"llm={llm_used} cache={len(cache)} skip={skipped_llm}) ===")
    print(f"  publish-recall     {recall:6.1%}  (tp={tp} fn={fn})")
    print(f"  false-reject-rate  {frr:6.1%}   ← expensive errors")
    print(f"  publish-precision  {precision:6.1%}  (tp={tp} fp={fp}) "
          f"[on {tp+fp} bot-publishes incl. {fp+tn} labeled-neg pool]")
    print(f"  section-accuracy   {sec_acc:6.1%}  (ok={sec_ok}/{sec_tot})")

    print(f"\n--- false REJECTS (editor published, bot killed) "
          f"[{len(false_rejects)}], first {args.diff} ---")
    for r in false_rejects[:args.diff]:
        print(f"  [{r.get('label_section','?')}] {r['title'][:78]}")
    print(f"\n--- false ACCEPTS (editor said no, bot publishes) "
          f"[{len(false_accepts)}], first {args.diff} ---")
    for r in false_accepts[:args.diff]:
        print(f"  «{r.get('editor_comment','')[:60]}» | "
              f"{r['title'][:60]}")
    print(f"\n--- section MISSES [{len(sec_misses)}], first {args.diff} ---")
    for r in sec_misses[:args.diff]:
        print(f"  {r['label_section']} → got {r['pred_sec']!r} | "
              f"{r['title'][:60]}")

    rec = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "tag": args.tag, "mode": mode, "prompt_fp": _PROMPT_FP,
        "n": len(strict), "recall": round(recall, 4),
        "precision": round(precision, 4), "frr": round(frr, 4),
        "section_acc": round(sec_acc, 4),
        "tp": tp, "fp": fp, "tn": tn, "fn": fn,
        "sec_ok": sec_ok, "sec_tot": sec_tot,
    }
    with HISTORY.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"\nhistory ← {HISTORY.name} (tag={args.tag!r})")

    if args.predictions:
        Path(args.predictions).write_text(
            json.dumps({
                "prompt_fp": _PROMPT_FP, "tag": args.tag, "mode": mode,
                "metrics": rec, "rows": per_row,
            }, ensure_ascii=False), encoding="utf-8")
        print(f"predictions → {args.predictions} ({len(per_row)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
