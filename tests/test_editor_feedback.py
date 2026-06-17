"""Editor-comment precision — one shared definition (scorecard + weekly_kpi)."""

from news_agent.core.editor_feedback import row_errors, precision_from_feedback


def test_row_errors_clean() -> None:
    # approved row, no objection
    assert row_errors({"label_publish": True}) == []


def test_row_errors_dup_and_section() -> None:
    errs = row_errors({"label_dup_cross_run": True, "label_section": "Rumors"})
    assert "дубль" in errs and "не та секция" in errs


def test_row_errors_reject_not_double_counted_as_dup() -> None:
    # a pure reject ("не нужно") with no dup flag
    assert row_errors({"label_publish": False}) == ["не нужно"]
    # a dup that is also publish=False counts as дубль, NOT "не нужно"
    assert row_errors({"label_publish": False, "label_dup_within": True}) == ["дубль"]


def test_precision_from_feedback() -> None:
    rows = [
        {"label_publish": True},                       # clean
        {"label_publish": True},                       # clean
        {"label_publish": False},                      # не нужно
        {"label_dup_cross_run": True},                 # дубль
    ]
    r = precision_from_feedback(rows)
    assert r["hit"] == 2 and r["total"] == 4
    assert abs(r["rate"] - 0.5) < 1e-9
    assert r["by_type"]["не нужно"] == 1 and r["by_type"]["дубль"] == 1
    assert r["is_biased"] is True


def test_precision_empty() -> None:
    r = precision_from_feedback([])
    assert r["total"] == 0 and r["rate"] == 0.0
