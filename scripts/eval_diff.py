"""Eval diff — show what FLIPPED between two classifier snapshots
(peer-review §5: regression CI against the "маятник").

Workflow around any prompt/heuristic change:

  # 1. snapshot current behaviour BEFORE editing
  python scripts/eval_harness.py --predictions data/eval_baseline.json --tag before
  # 2. make your prompt / heuristic change
  # 3. snapshot the new behaviour
  python scripts/eval_harness.py --predictions data/eval_after.json --tag after
  # 4. see exactly what moved
  python scripts/eval_diff.py data/eval_baseline.json data/eval_after.json

Prints aggregate metric deltas + the exact rows that regressed
(right→wrong) and improved (wrong→right) on the publish and section axes.
Exit code is non-zero when anything regressed, so it can gate a change:
no prompt/heuristic edit ships if a labelled row silently broke.

Pure file I/O; all logic lives in news_agent.core.eval_diff (unit-tested).
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from news_agent.core.eval_diff import (  # noqa: E402
    diff_predictions,
    is_regression,
    metric_deltas,
    parse_rows,
)


def _load(path: Path) -> dict:
    if not path.exists():
        print(f"missing snapshot: {path}")
        print("  produce it with: python scripts/eval_harness.py "
              f"--predictions {path}")
        sys.exit(2)
    return json.loads(path.read_text(encoding="utf-8"))


def _arrow(name: str, before: float, after: float, delta: float) -> str:
    # frr is "lower is better"; the rest "higher is better"
    better = (delta < 0) if name == "frr" else (delta > 0)
    worse = (delta > 0) if name == "frr" else (delta < 0)
    mark = "▲ better" if better and abs(delta) > 1e-9 else (
        "▼ WORSE" if worse and abs(delta) > 1e-9 else "= same")
    return (f"  {name:14} {before:6.1%} → {after:6.1%}  "
            f"({delta:+.1%})  {mark}")


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    base_path = Path(args[0]) if len(args) > 0 else DATA / "eval_baseline.json"
    after_path = Path(args[1]) if len(args) > 1 else DATA / "eval_after.json"
    show = 30
    for a in sys.argv[1:]:
        if a.startswith("--show="):
            show = int(a.split("=", 1)[1])

    base = _load(base_path)
    after = _load(after_path)

    print(f"=== eval diff: {base_path.name} → {after_path.name} ===")
    print(f"  classifier fp: {base.get('prompt_fp','?')} → "
          f"{after.get('prompt_fp','?')}  "
          f"(tags: {base.get('tag','')!r} → {after.get('tag','')!r})")
    if base.get("prompt_fp") == after.get("prompt_fp"):
        print("  ⚠ same classifier fingerprint — did the change actually "
              "alter the prompt/heuristics?")
    bm, am = base.get("mode", ""), after.get("mode", "")
    if ("FAST" in bm) != ("FAST" in am):
        print(f"  ⚠ MODE MISMATCH: {bm} vs {am} — FAST vs LLM snapshots are "
              "not comparable (fast mode can't reject). Re-snapshot matching.")
    elif bm and am and bm != am:
        print(f"  (comparing {bm} → {am})")

    deltas = metric_deltas(base.get("metrics", {}), after.get("metrics", {}))
    print("\nAGGREGATE METRICS (before → after):")
    for name, b, a, d in deltas:
        print(_arrow(name, b, a, d))

    d = diff_predictions(parse_rows(base.get("rows", [])),
                        parse_rows(after.get("rows", [])))
    print(f"\nAligned {d['n_common']} rows by id.")
    if d["only_baseline"] or d["only_after"]:
        print(f"  ⚠ id drift: {len(d['only_baseline'])} only-in-baseline, "
              f"{len(d['only_after'])} only-in-after (eval set changed?)")

    # Regressions first — these are the маятник.
    pb, sb = d["publish_broke"], d["section_broke"]
    pf, sf = d["publish_fixed"], d["section_fixed"]

    print(f"\n🔴 REGRESSIONS — publish right→wrong: {len(pb)}, "
          f"section right→wrong: {len(sb)}")
    for f in pb[:show]:
        print(f"    pub  {f.before} → {f.after}  | {f.title[:70]}")
    for f in sb[:show]:
        print(f"    sec  {f.before} → {f.after}  | {f.title[:70]}")

    print(f"\n🟢 IMPROVEMENTS — publish wrong→right: {len(pf)}, "
          f"section wrong→right: {len(sf)}")
    for f in pf[:show]:
        print(f"    pub  {f.before} → {f.after}  | {f.title[:70]}")
    for f in sf[:show]:
        print(f"    sec  {f.before} → {f.after}  | {f.title[:70]}")

    regressed = is_regression(d, deltas)
    print("\n" + ("=" * 56))
    if regressed:
        print("VERDICT: 🔴 REGRESSION — a labelled row broke or a metric "
              "dropped.")
        print("  Review the red rows above before shipping this change.")
    else:
        net = (len(pf) + len(sf)) - (len(pb) + len(sb))
        print(f"VERDICT: 🟢 no regression. Net row improvements: {net:+d}.")
    return 1 if regressed else 0


if __name__ == "__main__":
    sys.exit(main())
