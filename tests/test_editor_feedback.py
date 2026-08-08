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


# ---------------------------------------- aug-07: unparsed must not read clean

def test_an_unreadable_comment_is_not_a_success() -> None:
    """The defect this fixes. row_errors returns [] both for «ок» and for a
    wording it has never seen, and precision counted both as clean — so on the
    week of aug 3-7 it reported 108 clean rows where 60 were real: 48 were
    complaints the parser could not read."""
    from news_agent.core.editor_feedback import classify_row
    assert classify_row({"label_publish": True, "editor_comment": "ок"}) == "clean"
    assert classify_row(
        {"label_publish": None,
         "editor_comment": "Speaking with Reuters, professor at Leeds…"}) == "unparsed"


def test_a_fixable_row_is_not_a_wasted_row() -> None:
    """A wrong section is a five-second fix, a duplicate costs the whole read.
    The operator counts the first as a correct row and he is right to."""
    from news_agent.core.editor_feedback import classify_row
    assert classify_row({"label_section": "Факты"}) == "fixable"
    assert classify_row({"label_wrong_primary": True}) == "fixable"
    assert classify_row({"label_dup_cross_run": True}) == "wasted"
    assert classify_row({"label_publish": False}) == "wasted"


def test_unparsed_leaves_the_denominator_too() -> None:
    """Not evidence either way — it must not quietly help or hurt the rate."""
    rows = [
        {"label_publish": True},                    # clean
        {"label_section": "Факты"},                 # fixable → still right
        {"label_dup_cross_run": True},              # wasted
        {"label_publish": None, "editor_comment": "какой-то текст статьи"},
    ]
    r = precision_from_feedback(rows)
    assert r["clean"] == 1 and r["fixable"] == 1 and r["wasted"] == 1
    assert r["unparsed"] == 1
    assert r["hit"] == 2 and r["total"] == 3      # unparsed out of both
    assert r["commented"] == 4


def test_the_editors_real_wordings_are_recognised() -> None:
    """Quoted verbatim from the feed of aug 3-7, where every one of these was
    being scored as a clean row."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    from sync_editor_feedback import parse_comment
    from news_agent.core.editor_feedback import classify_row
    for text in ("было в 1 части", "было вчера", "нет, было",
                 "было и тут и на портале", "3 новости об одном и том же",
                 "там писать нечего", "никакой конкретики", "нечего писать",
                 "не думаю, что нам это нужно", "этот бренд вообще не нужен",
                 "нет, ничего важного", "шакальные фото"):
        rec = parse_comment(text)
        rec.setdefault("editor_comment", text)
        assert classify_row(rec) == "wasted", f"«{text}» снова читается как чистая"
