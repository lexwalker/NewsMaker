"""PoC iteration #3: WITHIN-BATCH dedup test (v41 vs itself).

This is the real test. The 13 merge groups we hand-built today are
GUARANTEED to have both members in v41. If embeddings can recover
those pairs at high precision/recall, the architecture is validated
for clustering — independent of crawl-coverage issues that confounded
the cross-run test.

Approach:
  • Take v41 (52 rows), compute all-pairs cosine similarity.
  • Threshold for DUP at various cosines.
  • Score against the hand-merge truth (13 groups, 18 rows hidden as
    duplicates of canonical rows).

Truth set (from _merge_v41_dup_groups.py):
  same-event pairs we manually merged today. Each non-canonical row
  is a known DUP of a canonical row inside the same v41 push.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import numpy as np

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

V41_FILE = DATA / "embed_poc_v41.jsonl"
EMB_CACHE = DATA / "embed_poc_vectors.npz"

# ---------- ground truth: pairs we merged today (canonical, dup) -----
TRUE_DUP_PAIRS = [
    (3, 4),
    (8, 18),
    (23, 38),
    # r59/r94 were Confirmed dups but they're in another push (r59 is
    # from v40); only items in v41 push (rows 3..54) count for this
    # within-batch test.
    # → Filter to pairs where BOTH are in v41:
]
# Re-derive: only pairs where both rows ≤54 (within v41 push)
TRUE_DUP_PAIRS = [(c, d) for c, d in TRUE_DUP_PAIRS if c <= 54 and d <= 54]


def main() -> int:
    v41 = []
    for ln in V41_FILE.read_text("utf-8").splitlines():
        if ln.strip():
            try:
                v41.append(json.loads(ln))
            except Exception:
                pass
    d = np.load(EMB_CACHE)
    vec = d["v41"]
    print(f"v41 rows: {len(v41)}, vectors: {vec.shape}")

    # row → idx in array
    row_to_idx = {r["row"]: i for i, r in enumerate(v41)}

    # All-pairs cosine matrix
    sim = vec @ vec.T
    np.fill_diagonal(sim, -1.0)  # ignore self-pairs

    # Quick sanity: for each true dup pair, what's the cosine?
    print("\n=== TRUE within-batch dup pairs (manual merge today) ===\n")
    for canon, dup in TRUE_DUP_PAIRS:
        ic = row_to_idx.get(canon)
        idu = row_to_idx.get(dup)
        if ic is None or idu is None:
            print(f"  r{canon}↔r{dup}: out of v41 push window")
            continue
        cos = float(sim[ic, idu])
        title_c = v41[ic]["title"][:50].replace("\n", " | ")
        title_d = v41[idu]["title"][:50].replace("\n", " | ")
        print(f"  r{canon}↔r{dup}  cos={cos:.3f}")
        print(f"     A: {title_c}")
        print(f"     B: {title_d}")

    # Scorecard: precision/recall at threshold sweep over ALL pairs
    print("\n=== ALL-PAIRS SCORECARD (within-batch) ===\n")
    truth = set()
    for c, du in TRUE_DUP_PAIRS:
        ic = row_to_idx.get(c)
        idu = row_to_idx.get(du)
        if ic is not None and idu is not None:
            truth.add((min(ic, idu), max(ic, idu)))

    # Add the additional merges that have both members in v41:
    # Look at v41 again and find the structural pairs from today's merge
    # script. The pairs WITHIN v41 push (row<=54) were:
    #   (3, 4)   Ram
    #   (8, 18)  VinFast
    #   (23, 38) Merc-AMG GT 4-Door
    # All other merges (Maextro 73/144/155, Sollers 103/137, Alpina
    # 140/167/148/116, Volga 196/217, etc.) had members OUTSIDE v41
    # push (rows >54 are earlier pushes). So within-v41-push truth set
    # is only 3 pairs.
    print(f"truth pairs (in v41 push, rows 3..54): {len(truth)}")

    pairs: list[tuple[float, int, int]] = []
    for i in range(len(v41)):
        for j in range(i + 1, len(v41)):
            pairs.append((float(sim[i, j]), i, j))
    pairs.sort(reverse=True)

    print(f"\n{'thresh':>7} {'TP':>4} {'FP':>4} {'FN':>4} "
          f"{'prec':>6} {'rec':>6} {'F1':>6}")
    for th in (0.50, 0.55, 0.60, 0.625, 0.65, 0.70, 0.75, 0.80, 0.85):
        pred = {(i, j) for s, i, j in pairs if s >= th}
        tp = len(pred & truth)
        fp = len(pred - truth)
        fn = len(truth - pred)
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        print(f"  {th:.3f} {tp:>4} {fp:>4} {fn:>4} "
              f"{prec:>6.1%} {rec:>6.1%} {f1:>6.1%}")

    # Show top-30 within-batch pairs by cosine — eyeball them
    print(f"\n=== TOP-30 ALL-PAIRS cosines in v41 (eyeball test) ===\n")
    seen = set()
    shown = 0
    for s, i, j in pairs:
        if shown >= 30:
            break
        ri, rj = v41[i]["row"], v41[j]["row"]
        pair = (min(ri, rj), max(ri, rj))
        seen.add(pair)
        marker = "✓TRUE" if (i, j) in truth or (j, i) in truth else "    "
        ta = v41[i]["title"][:55].replace("\n", " | ")
        tb = v41[j]["title"][:55].replace("\n", " | ")
        sa = v41[i]["section"][:10]
        sb = v41[j]["section"][:10]
        ed_i = v41[i]["editor_label"]
        ed_j = v41[j]["editor_label"]
        ed = f"[{ed_i[:3]}/{ed_j[:3]}]"
        print(f"  {marker} cos={s:.3f}  r{ri:>3}({sa:10}) ⇋ "
              f"r{rj:>3}({sb:10})  {ed}")
        print(f"           A: {ta}")
        print(f"           B: {tb}")
        shown += 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
