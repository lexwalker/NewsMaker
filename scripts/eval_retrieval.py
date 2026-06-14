"""Retrieval-classifier eval — run the precedent-based judge over the
FROZEN eval set and emit predictions in eval_harness format, so
eval_diff can compare it head-to-head against the monster-prompt baseline
(peer-review §7, migration step 2: "run retrieval alongside the monster
prompt on the frozen eval set; compare recall/precision/sections").

This is the go/no-go for the whole retrieval direction. It DECIDES nothing
in production — it classifies eval_set_v2 with EditorialJudge (k nearest
editor precedents as few-shot, NO rule-monster prompt, NO heuristic_section
override) and writes data/eval_retrieval.json.

Honesty guards:
  * self-exclusion — a candidate never retrieves its own exact-title
    precedent (16 eval rows are in the 88-negative base; without this they
    would trivially self-predict "reject").
  * same row selection + ids as eval_harness, so eval_diff aligns 1:1.
  * the judge's REAL precedent base only (4758 pos + 88 neg) — no eval
    rows added as precedents, so no train/test leakage. If thin negatives
    hurt it, that's an honest finding (→ motivates the labelling ritual).

Usage:
  python scripts/eval_retrieval.py            # all labeled rows (~371, ~$0.4)
  python scripts/eval_retrieval.py --limit 40 # cheap smoke
  python scripts/eval_diff.py data/eval_baseline.json data/eval_retrieval.json
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from news_agent.core.editorial_judge import EditorialJudge  # noqa: E402

EVAL = DATA / "eval_set_v2.jsonl"
DECISIONS = DATA / "editor_decisions.json"
VECTORS = DATA / "editor_decisions_vectors.npz"
OUT = DATA / "eval_retrieval.json"
CACHE = DATA / "eval_retrieval_cache.json"


def _load_jsonl(p: Path) -> list[dict]:
    return [json.loads(ln) for ln in p.read_text(encoding="utf-8").splitlines()
            if ln.strip()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--k-pos", type=int, default=6)
    ap.add_argument("--k-neg", type=int, default=5)
    args = ap.parse_args()

    if not DECISIONS.exists():
        print("data/editor_decisions.json missing")
        return 2
    rows = _load_jsonl(EVAL)
    strict = [r for r in rows if not r.get("soft")]
    labeled = [r for r in strict if r.get("label_publish") is not None]
    print(f"eval_set_v2: {len(rows)} rows, {len(strict)} strict, "
          f"{len(labeled)} labeled")

    cache = {}
    if CACHE.exists():
        try:
            cache = json.loads(CACHE.read_text(encoding="utf-8"))
        except ValueError:
            cache = {}

    judge = EditorialJudge(DECISIONS, vectors_cache=VECTORS,
                           k_pos=args.k_pos, k_neg=args.k_neg)
    print(f"precedent base: {len(judge.pos)} pos, {len(judge.neg)} neg")

    tp = fp = tn = fn = 0
    sec_ok = sec_tot = 0
    none_verdicts = errors = 0
    per_row = []
    cost = 0.0
    t0 = time.time()
    n = 0
    for r in labeled:
        if args.limit and n >= args.limit:
            break
        n += 1
        rid = r.get("id") or hashlib.sha1(
            r["title"].encode("utf-8")).hexdigest()[:16]
        title, body = r["title"], r.get("body") or r["title"]
        lab_pub = bool(r.get("label_publish"))
        lab_sec = r.get("label_section") or ""

        if rid in cache:
            v = cache[rid]
        else:
            jv = judge.judge(title, body, exclude_title=title)
            cost += jv.cost_usd
            if jv.error:
                errors += 1
            v = {"pub": jv.advisory_publish, "sec": jv.advisory_section or "",
                 "err": jv.error}
            cache[rid] = v
            if n % 25 == 0:
                print(f"  {n}/{len(labeled)}  ${cost:.3f}  "
                      f"(errs {errors})", flush=True)

        if v["pub"] is None:
            none_verdicts += 1
        pred_pub = bool(v["pub"])      # None → reject (conservative)
        pred_sec = v["sec"]

        if lab_pub and pred_pub:
            tp += 1
        elif lab_pub and not pred_pub:
            fn += 1
        elif (not lab_pub) and pred_pub:
            fp += 1
        else:
            tn += 1
        if lab_pub and pred_pub and lab_sec:
            sec_tot += 1
            if pred_sec == lab_sec:
                sec_ok += 1

        per_row.append({
            "id": rid, "title": title[:120],
            "lab_pub": lab_pub, "pred_pub": pred_pub,
            "lab_sec": lab_sec, "pred_sec": pred_sec,
        })

    CACHE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    recall = tp / (tp + fn) if (tp + fn) else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    frr = fn / (tp + fn) if (tp + fn) else 0.0
    sec_acc = sec_ok / sec_tot if sec_tot else 0.0
    metrics = {"recall": round(recall, 4), "precision": round(precision, 4),
               "frr": round(frr, 4), "section_acc": round(sec_acc, 4),
               "tp": tp, "fp": fp, "tn": tn, "fn": fn,
               "sec_ok": sec_ok, "sec_tot": sec_tot}

    fp_marker = "retrieval-" + hashlib.sha1(
        f"kpos{args.k_pos}kneg{args.k_neg}".encode()).hexdigest()[:8]
    OUT.write_text(json.dumps({
        "prompt_fp": fp_marker, "tag": "retrieval", "mode": "RETRIEVAL(+LLM)",
        "metrics": metrics, "rows": per_row,
    }, ensure_ascii=False), encoding="utf-8")

    print(f"\n=== RETRIEVAL eval — {n} rows ({time.time()-t0:.0f}s, "
          f"${cost:.3f}) ===")
    print(f"  publish-recall     {recall:6.1%}  (tp={tp} fn={fn})")
    print(f"  false-reject-rate  {frr:6.1%}")
    print(f"  publish-precision  {precision:6.1%}  (tp={tp} fp={fp})")
    print(f"  section-accuracy   {sec_acc:6.1%}  (ok={sec_ok}/{sec_tot})")
    print(f"  no-verdict (None→reject): {none_verdicts}, errors: {errors}")
    print(f"\npredictions → {OUT.name}")
    print(f"compare: python scripts/eval_diff.py data/eval_baseline.json {OUT.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
