"""Rejection-labelling ritual — routing + stratified sampling (Week2-B)."""

from news_agent.core.labeling import (
    CONFIRMED_NEGATIVE,
    FALSE_REJECT,
    SKIP,
    route_reject_label,
    stratified_sample,
    summarise_labels,
)


def test_route_yes_is_false_reject() -> None:
    for v in ("да", "нужно", "Да", " ДА ", "нужна", "+", "1", "ok"):
        assert route_reject_label(v) == FALSE_REJECT, v


def test_route_no_is_confirmed_negative() -> None:
    for v in ("нет", "не нужно", "Не нужно", "ненужно", "-", "0", "no"):
        assert route_reject_label(v) == CONFIRMED_NEGATIVE, v


def test_route_blank_is_skip() -> None:
    for v in ("", "   ", "?", "хз", None):
        assert route_reject_label(v) == SKIP, repr(v)


def _no_shuffle(lst):
    pass  # deterministic: keep input order


def test_stratified_caps_per_bucket() -> None:
    items = (
        [{"cause": "blacklist", "id": i} for i in range(10)]
        + [{"cause": "off_topic", "id": i} for i in range(3)]
        + [{"cause": "llm", "id": i} for i in range(7)]
    )
    out = stratified_sample(items, lambda x: x["cause"], per_bucket=4,
                            shuffle=_no_shuffle)
    by = {}
    for it in out:
        by[it["cause"]] = by.get(it["cause"], 0) + 1
    assert by["blacklist"] == 4      # capped
    assert by["off_topic"] == 3      # fewer than cap → all
    assert by["llm"] == 4            # capped
    assert len(out) == 11


def test_stratified_excludes_already_labeled() -> None:
    items = [{"cause": "llm", "id": i} for i in range(5)]
    seen = {1, 3}
    out = stratified_sample(items, lambda x: x["cause"], per_bucket=10,
                            shuffle=_no_shuffle,
                            exclude=lambda x: x["id"] in seen)
    ids = {it["id"] for it in out}
    assert ids == {0, 2, 4}


def test_summarise_labels() -> None:
    routed = [
        (CONFIRMED_NEGATIVE, {"title": "a"}),
        (CONFIRMED_NEGATIVE, {"title": "b"}),
        (FALSE_REJECT, {"title": "c", "cause": "blacklist"}),
        (FALSE_REJECT, {"title": "d", "cause": "blacklist"}),
        (FALSE_REJECT, {"title": "e", "cause": "off_topic"}),
        (SKIP, {"title": "f"}),
    ]
    s = summarise_labels(routed)
    assert s["labeled"] == 5
    assert s["confirmed_negative"] == 2
    assert s["false_reject"] == 3
    assert s["skipped"] == 1
    assert s["false_reject_by_cause"] == {"blacklist": 2, "off_topic": 1}
