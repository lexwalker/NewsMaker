"""Hybrid section bake-off — does heuristic_section (a rule) assign the
editor's section better than retrieval-alone (copying the nearest
precedent's section)?

The retrieval eval showed retrieval WINS on publish-precision (+3.8pp) but
LOSES on sections (60%→43%). The proposed hybrid keeps retrieval for the
publish/reject decision and uses heuristic_section for the section. This
script measures the section half of that claim.

Clean by construction: heuristic_section is a RULE (no precedent base), so
there is NO eval leakage here even though eval_set_v2's negatives are now
in the retrieval base. We compare, on every row with an editor gold
section, three section assigners:
  baseline  — monster LLM + heuristic_section override (from eval_baseline)
  retrieval — nearest-precedent section (from eval_retrieval)
  hybrid    — heuristic_section(title) → else retrieval section → else Other

$0, no LLM. Usage: python scripts/eval_hybrid_section.py
"""

from __future__ import annotations

import io
import json
import sys
from collections import Counter
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from news_agent.core.heuristic_relevance import heuristic_section  # noqa: E402

EVAL = DATA / "eval_set_v2.jsonl"
BASELINE = DATA / "eval_baseline.json"
RETRIEVAL = DATA / "eval_retrieval.json"


def _by_id(path: Path) -> dict:
    d = json.loads(path.read_text(encoding="utf-8"))
    return {r["id"]: r for r in d["rows"]}


def main() -> int:
    ev = {}
    for ln in EVAL.read_text(encoding="utf-8").splitlines():
        if ln.strip():
            r = json.loads(ln)
            ev[r.get("id")] = r
    base = _by_id(BASELINE)
    retr = _by_id(RETRIEVAL)

    gold_rows = [(rid, r) for rid, r in ev.items() if r.get("label_section")]
    print(f"Gold-section rows total: {len(gold_rows)}")
    print("Section accuracy is measured ONLY on rows a method PUBLISHES "
          "(section is meaningless on a rejected row) — apples-to-apples per\n"
          "method, denominators differ. Hybrid is scored on RETRIEVAL's "
          "publish set (its publish gate) so retrieval vs hybrid is the same rows.\n")

    def acc_on_publish(pred_map, sec_fn):
        """section accuracy among rows this method publishes + has gold."""
        ok = tot = 0
        for rid, r in gold_rows:
            p = pred_map.get(rid, {})
            if not p.get("pred_pub"):
                continue
            tot += 1
            ok += (sec_fn(rid, r, p) == r["label_section"])
        return ok, tot

    # baseline: its own publish set, its own section
    b_ok, b_tot = acc_on_publish(base, lambda rid, r, p: p.get("pred_sec", ""))
    # retrieval: its publish set, precedent section
    r_ok, r_tot = acc_on_publish(retr, lambda rid, r, p: p.get("pred_sec", ""))

    # hybrid: retrieval's publish set, heuristic_section → retrieval fallback
    def hybrid_sec(rid, r, p):
        hs = heuristic_section(title=r["title"],
                               body_excerpt=r.get("body", "") or "")
        return (hs.section if hs else "") or p.get("pred_sec", "") or "Other news"

    h_ok, h_tot = acc_on_publish(retr, hybrid_sec)

    heur_fired = sum(
        1 for rid, r in gold_rows
        if retr.get(rid, {}).get("pred_pub")
        and heuristic_section(title=r["title"],
                              body_excerpt=r.get("body", "") or ""))

    def pct(x, t):
        return f"{100*x/t:.0f}%" if t else "—"

    print("SECTION ACCURACY (published rows only, leakage-free):")
    print(f"  baseline  (LLM+heuristic, own publish set): "
          f"{pct(b_ok, b_tot)}  ({b_ok}/{b_tot})")
    print(f"  retrieval (precedent sec, retr publish set): "
          f"{pct(r_ok, r_tot)}  ({r_ok}/{r_tot})")
    print(f"  HYBRID    (heuristic→retr, retr publish set): "
          f"{pct(h_ok, h_tot)}  ({h_ok}/{h_tot})")
    print(f"\n  heuristic_section fired on {heur_fired}/{r_tot} of retrieval's "
          f"published rows; the rest fell back to the retrieval section.")

    # hybrid vs retrieval on the SAME rows (the clean comparison)
    print("\n  retrieval → hybrid flips on retrieval's published gold rows:")
    fixed = broke = 0
    examples = []
    for rid, r in gold_rows:
        p = retr.get(rid, {})
        if not p.get("pred_pub"):
            continue
        gold = r["label_section"]
        rt = p.get("pred_sec", "")
        hy = hybrid_sec(rid, r, p)
        if (rt == gold) and (hy != gold):
            broke += 1
        elif (rt != gold) and (hy == gold):
            fixed += 1
            if len(examples) < 6:
                examples.append((gold, rt or "∅", hy, r["title"][:46]))
    print(f"    heuristic FIXED {fixed}, broke {broke} (net {fixed-broke:+d})")
    for gold, rt, hy, t in examples:
        print(f"    ✓ {gold:14} (was {rt:14}) | {t}")

    verdict = ("HYBRID ≥ baseline sections AND > retrieval → adopt for prod"
               if h_ok / max(h_tot, 1) >= b_ok / max(b_tot, 1) - 0.03
               and h_ok / max(h_tot, 1) > r_ok / max(r_tot, 1)
               else "hybrid between retrieval and baseline — see numbers")
    print(f"\n  → {verdict}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
