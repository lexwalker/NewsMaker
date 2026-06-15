"""Leave-block-out A/B: does enriching the negative base (88 → 186) make
the retrieval classifier reject more junk WITHOUT a clean-test leakage?

The honest problem: the editor comments we harvested as negatives ARE the
eval set, so re-measuring on eval_set_v2 would leak. Instead we hold out
ONE whole push block (09.06) as the test and remove all its titles from
BOTH base arms, so no test row is in its own precedent base.

Test set = the 09.06 block's editor verdicts (from editor_markup_full.jsonl):
  keep (publish=True)  = ok / note / wrong_section / primary_source
  reject (publish=False) = "reject" (content/taste)
  EXCLUDED             = stale_dup (recency rejection — not a content test)

Arms (identical except the negative base):
  A = editor_decisions.json.bak  (original 88 negatives)
  B = editor_decisions.json      (enriched 186 negatives)
Both exclude every 09.06 title from retrieval (leave-block-out).

If B rejects more of the content-rejects (higher precision) without losing
the keeps (recall), the enrichment helped — measured cleanly.

Usage: python scripts/eval_heldout_negatives.py
"""

from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from news_agent.core.editorial_judge import EditorialJudge  # noqa: E402

MARKUP = DATA / "editor_markup_full.jsonl"
BASE_B = DATA / "editor_decisions.json"       # enriched (186)
BASE_A = DATA / "editor_decisions.json.bak"   # original (88)
TEST_BLOCK = "09.06.2026"
KEEP_CATS = {"ok", "note", "wrong_section", "primary_source"}
REJECT_CATS = {"reject"}


def _norm(t: str) -> str:
    return " ".join(str(t).lower().split())[:80]


def run_arm(label, decisions_path, test, exclude_titles, cache):
    judge = EditorialJudge(decisions_path, vectors_cache=cache, k_pos=6, k_neg=5)
    tp = fp = tn = fn = 0
    cost = 0.0
    flips = []
    for t in test:
        v = judge.judge(t["title"], exclude_titles=exclude_titles)
        cost += v.cost_usd
        pred = bool(v.advisory_publish)   # None → reject
        gold = t["gold_pub"]
        if gold and pred:
            tp += 1
        elif gold and not pred:
            fn += 1
        elif (not gold) and pred:
            fp += 1
        else:
            tn += 1
        flips.append((t["title"], gold, pred, v.reason[:60]))
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    print(f"\n[{label}]  base={decisions_path.name}")
    print(f"  precision {prec:5.1%}  recall {rec:5.1%}  "
          f"(tp={tp} fp={fp} fn={fn} tn={tn})  ${cost:.3f}")
    return {"prec": prec, "rec": rec, "tp": tp, "fp": fp, "fn": fn,
            "tn": tn, "flips": flips}


def main() -> int:
    if not BASE_A.exists():
        print("editor_decisions.json.bak missing — run harvest_negatives "
              "--write first (it makes the backup).")
        return 2
    marks = [json.loads(l) for l in MARKUP.read_text(encoding="utf-8").splitlines()
             if l.strip()]
    block = [m for m in marks if m["block"] == TEST_BLOCK]
    test = []
    for m in block:
        if m["cat"] in KEEP_CATS:
            test.append({"title": m["title"], "gold_pub": True})
        elif m["cat"] in REJECT_CATS:
            test.append({"title": m["title"], "gold_pub": False})
        # stale_dup excluded
    exclude = {_norm(m["title"]) for m in block}
    n_keep = sum(1 for t in test if t["gold_pub"])
    n_rej = sum(1 for t in test if not t["gold_pub"])
    print(f"Held-out test = block {TEST_BLOCK}: {len(test)} rows "
          f"({n_keep} keep, {n_rej} content-reject; "
          f"{len(block)-len(test)} stale_dup excluded)")
    print(f"Both arms exclude all {len(exclude)} block titles from retrieval.")

    a = run_arm("A · 88 neg", BASE_A, test, exclude, DATA / "_ab_a.npz")
    b = run_arm("B · 186 neg", BASE_B, test, exclude, DATA / "_ab_b.npz")

    print("\n" + "=" * 56)
    print("A/B (enrichment effect, leakage-free):")
    print(f"  precision: {a['prec']:.1%} → {b['prec']:.1%} "
          f"({(b['prec']-a['prec'])*100:+.1f}pp)")
    print(f"  recall:    {a['rec']:.1%} → {b['rec']:.1%} "
          f"({(b['rec']-a['rec'])*100:+.1f}pp)")
    # which content-rejects did B newly catch?
    a_pred = {f[0]: f[2] for f in a["flips"]}
    newly = [f for f in b["flips"]
             if not f[1] and not f[2] and a_pred.get(f[0]) is True]
    if newly:
        print(f"\n  B newly REJECTS {len(newly)} junk that A published:")
        for title, _g, _p, reason in newly[:8]:
            print(f"    «{title[:46]}»  ({reason})")
    lost = [f for f in b["flips"]
            if f[1] and not f[2] and a_pred.get(f[0]) is True]
    if lost:
        print(f"\n  ⚠ B newly REJECTS {len(lost)} GOOD rows A kept (recall cost):")
        for title, _g, _p, reason in lost[:8]:
            print(f"    «{title[:46]}»  ({reason})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
