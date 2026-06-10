"""Eval-diff regression detection (peer-review §5, Week2-A).

Pins the row-alignment + flip-bucketing the маятник-guard relies on:
right→wrong is a regression, wrong→right is an improvement, on both the
publish and the section axes.
"""

from news_agent.core.eval_diff import (
    RowPred,
    diff_predictions,
    is_regression,
    metric_deltas,
    parse_rows,
)


def _r(id, lab_pub, pred_pub, lab_sec="", pred_sec="", title="t"):
    return RowPred(id, title, lab_pub, pred_pub, lab_sec, pred_sec)


def test_publish_regression_detected() -> None:
    # row 1: was correct (pub=True, label=True), now wrong (pub=False)
    base = [_r("1", True, True)]
    after = [_r("1", True, False)]
    d = diff_predictions(base, after)
    assert len(d["publish_broke"]) == 1
    assert not d["publish_fixed"]
    assert d["publish_broke"][0].axis == "publish"


def test_publish_improvement_detected() -> None:
    base = [_r("1", True, False)]   # wrong
    after = [_r("1", True, True)]   # fixed
    d = diff_predictions(base, after)
    assert len(d["publish_fixed"]) == 1
    assert not d["publish_broke"]


def test_no_flip_when_both_correct() -> None:
    base = [_r("1", True, True), _r("2", False, False)]
    after = [_r("1", True, True), _r("2", False, False)]
    d = diff_predictions(base, after)
    assert not d["publish_broke"] and not d["publish_fixed"]
    assert not d["section_broke"] and not d["section_fixed"]


def test_section_regression_detected() -> None:
    # published both sides, gold=Confirmed; was right, now wrong
    base = [_r("1", True, True, lab_sec="Confirmed", pred_sec="Confirmed")]
    after = [_r("1", True, True, lab_sec="Confirmed", pred_sec="Other news")]
    d = diff_predictions(base, after)
    assert len(d["section_broke"]) == 1
    assert not d["section_fixed"]


def test_section_only_judged_when_published_and_gold() -> None:
    # not published → section not judged → no section flip even if pred_sec differs
    base = [_r("1", False, False, lab_sec="Confirmed", pred_sec="")]
    after = [_r("1", False, False, lab_sec="Confirmed", pred_sec="Other news")]
    d = diff_predictions(base, after)
    assert not d["section_broke"] and not d["section_fixed"]


def test_id_set_drift_reported() -> None:
    base = [_r("1", True, True), _r("2", True, True)]
    after = [_r("1", True, True), _r("3", True, True)]
    d = diff_predictions(base, after)
    assert d["only_baseline"] == ["2"]
    assert d["only_after"] == ["3"]
    assert d["n_common"] == 1


def test_is_regression_on_broken_row() -> None:
    base = [_r("1", True, True)]
    after = [_r("1", True, False)]
    d = diff_predictions(base, after)
    assert is_regression(d, []) is True


def test_is_regression_on_metric_drop() -> None:
    d = {"publish_broke": [], "section_broke": []}
    # recall dropped → regression
    deltas = [("recall", 0.50, 0.45, -0.05)]
    assert is_regression(d, deltas) is True
    # frr rose → regression
    assert is_regression(d, [("frr", 0.10, 0.15, 0.05)]) is True
    # everything improved → not a regression
    good = [("recall", 0.45, 0.50, 0.05), ("frr", 0.15, 0.10, -0.05)]
    assert is_regression(d, good) is False


def test_metric_deltas_skips_missing() -> None:
    deltas = metric_deltas({"recall": 0.4}, {"recall": 0.5, "precision": 0.6})
    # only recall present on both sides
    assert deltas == [("recall", 0.4, 0.5, 0.5 - 0.4)]


def test_parse_rows_roundtrip() -> None:
    raw = [{"id": "a", "title": "x", "lab_pub": True, "pred_pub": False,
            "lab_sec": "Confirmed", "pred_sec": "Other news"}]
    rows = parse_rows(raw)
    assert rows[0].id == "a"
    assert rows[0].lab_pub is True and rows[0].pred_pub is False
    assert not rows[0].publish_correct
