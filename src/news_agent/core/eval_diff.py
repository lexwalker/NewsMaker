"""Eval diff — what FLIPPED between two classifier runs (peer-review §5).

The "маятник": a prompt/heuristic fix lands, an unrelated case quietly
regresses, and nobody sees it until the editor complains a week later
("улучшений не вижу"). This compares two per-row prediction snapshots
(baseline vs after) over the frozen labelled eval set and surfaces, at
change-time:

  * aggregate metric deltas (recall / precision / section-accuracy)
  * the exact rows that went right→wrong (regressions) and wrong→right
    (improvements), on both the publish and the section axes.

Pure alignment + diff logic; scripts/eval_diff.py does file I/O. The
snapshots are produced by ``eval_harness.py --predictions PATH``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RowPred:
    id: str
    title: str
    lab_pub: bool
    pred_pub: bool
    lab_sec: str
    pred_sec: str

    @property
    def publish_correct(self) -> bool:
        return self.pred_pub == self.lab_pub

    @property
    def section_judged(self) -> bool:
        """Section is only scored where the editor labelled one AND the
        row is (predicted) published — matching eval_harness."""
        return bool(self.lab_sec) and self.pred_pub

    @property
    def section_correct(self) -> bool:
        return self.section_judged and self.pred_sec == self.lab_sec


@dataclass
class Flip:
    id: str
    title: str
    axis: str          # "publish" | "section"
    before: str        # human description of the before-prediction
    after: str         # human description of the after-prediction


def _index(rows: list[RowPred]) -> dict[str, RowPred]:
    return {r.id: r for r in rows}


def parse_rows(raw: list[dict]) -> list[RowPred]:
    out: list[RowPred] = []
    for d in raw:
        out.append(RowPred(
            id=str(d.get("id", "")),
            title=str(d.get("title", "")),
            lab_pub=bool(d.get("lab_pub")),
            pred_pub=bool(d.get("pred_pub")),
            lab_sec=str(d.get("lab_sec", "")),
            pred_sec=str(d.get("pred_sec", "")),
        ))
    return out


def diff_predictions(
    baseline: list[RowPred], after: list[RowPred]
) -> dict:
    """Align two snapshots by row id and bucket the flips.

    Returns publish_broke / publish_fixed (right→wrong / wrong→right on
    the publish axis), section_broke / section_fixed (likewise on the
    section axis, only where a gold section exists), plus id-set drift.
    """
    b_idx = _index(baseline)
    a_idx = _index(after)
    common = b_idx.keys() & a_idx.keys()

    publish_broke: list[Flip] = []
    publish_fixed: list[Flip] = []
    section_broke: list[Flip] = []
    section_fixed: list[Flip] = []

    for rid in common:
        b, a = b_idx[rid], a_idx[rid]

        # publish axis
        if b.publish_correct != a.publish_correct:
            flip = Flip(
                rid, a.title, "publish",
                f"pub={b.pred_pub} ({'ok' if b.publish_correct else 'wrong'})",
                f"pub={a.pred_pub} ({'ok' if a.publish_correct else 'wrong'})",
            )
            (publish_fixed if a.publish_correct else publish_broke).append(flip)

        # section axis — only where a gold section exists on both sides
        if b.section_judged and a.section_judged:
            if b.section_correct != a.section_correct:
                flip = Flip(
                    rid, a.title, "section",
                    f"{b.pred_sec or '∅'} vs gold {b.lab_sec}",
                    f"{a.pred_sec or '∅'} vs gold {a.lab_sec}",
                )
                (section_fixed if a.section_correct
                 else section_broke).append(flip)

    return {
        "n_common": len(common),
        "only_baseline": sorted(b_idx.keys() - a_idx.keys()),
        "only_after": sorted(a_idx.keys() - b_idx.keys()),
        "publish_broke": publish_broke,
        "publish_fixed": publish_fixed,
        "section_broke": section_broke,
        "section_fixed": section_fixed,
    }


def metric_deltas(base_metrics: dict, after_metrics: dict) -> list[tuple]:
    """Return [(name, before, after, delta)] for the headline metrics."""
    keys = ["recall", "precision", "frr", "section_acc"]
    out = []
    for k in keys:
        b = base_metrics.get(k)
        a = after_metrics.get(k)
        if b is None or a is None:
            continue
        out.append((k, b, a, a - b))
    return out


def is_regression(diff: dict, deltas: list[tuple]) -> bool:
    """True if anything got worse — a row broke or a headline metric
    dropped (frr going UP is worse; the rest going DOWN is worse)."""
    if diff["publish_broke"] or diff["section_broke"]:
        return True
    for name, _b, _a, d in deltas:
        if name == "frr" and d > 1e-9:
            return True
        if name != "frr" and d < -1e-9:
            return True
    return False
