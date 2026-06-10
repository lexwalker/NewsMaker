"""Canonical verdict → funnel-stage mapping (peer-review §3, Week1-B).

The rejection reason is already logged + persisted in the cache; this
module turns the verdict strings into the funnel's death-stage
decomposition. These tests pin the mapping the miss-funnel relies on.
"""

from news_agent.core.reject_stage import (
    ACCEPTED,
    COLLAPSED,
    FETCH_ERROR,
    S3_HEURISTIC,
    S4_LLM,
    STALE,
    UNKNOWN,
    classify_outcome,
)


def test_accepted_is_not_reject() -> None:
    for v in ("Точно новость", "Возможно новость"):
        o = classify_outcome(v)
        assert o.stage == ACCEPTED
        assert o.is_reject is False


def test_blacklist_is_s3_with_cause() -> None:
    o = classify_outcome("Точно не новость (чёрный список)")
    assert o.stage == S3_HEURISTIC
    assert o.cause == "blacklist"
    assert o.is_reject is True


def test_all_heuristic_subcauses_are_s3() -> None:
    cases = {
        "Точно не новость (не статья)": "not_article",
        "Точно не новость (не авто)": "off_topic",
        "Точно не новость (мульти-новость)": "multi_news",
        "Точно не новость (дзен-листикл)": "dzen_listicle",
        "Точно не новость (поставщик-абстракция)": "supplier_abstract",
    }
    for verdict, cause in cases.items():
        o = classify_outcome(verdict)
        assert o.stage == S3_HEURISTIC, verdict
        assert o.cause == cause, verdict
        assert o.is_reject is True


def test_llm_reject_is_s4() -> None:
    o = classify_outcome("Отклонено LLM")
    assert o.stage == S4_LLM
    assert o.is_reject is True


def test_dups_collapse() -> None:
    for v in (
        "Отклонить (дубль)",
        "Отклонить (дубль финального URL)",
        "Отклонить (обработан ранее)",
    ):
        o = classify_outcome(v)
        assert o.stage == COLLAPSED, v
        assert o.is_reject is True


def test_stale_and_fetch_error() -> None:
    assert classify_outcome("Точно не новость (старая)").stage == STALE
    assert classify_outcome("Отклонить (ошибка загрузки)").stage == FETCH_ERROR
    assert classify_outcome("Отклонить (не удалось извлечь)").stage == FETCH_ERROR


def test_unknown_and_blank() -> None:
    assert classify_outcome("").stage == UNKNOWN
    assert classify_outcome("какой-то новый вердикт").stage == UNKNOWN
    # unknown defaults to reject (safer: a row we can't classify didn't
    # demonstrably reach the table)
    assert classify_outcome("").is_reject is True


def test_whitespace_tolerant() -> None:
    assert classify_outcome("  Точно новость  ").stage == ACCEPTED
