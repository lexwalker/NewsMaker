from news_agent.core.dedup import title_is_duplicate


def test_exact_match_is_duplicate() -> None:
    assert title_is_duplicate("Toyota unveils 2026 Camry", ["Toyota unveils 2026 Camry"], threshold=0.85)


def test_near_match_is_duplicate() -> None:
    assert title_is_duplicate(
        "Toyota unveils 2026 Camry",
        ["Toyota Unveils the 2026 Camry Sedan"],
        threshold=0.80,
    )


def test_unrelated_title_not_duplicate() -> None:
    assert not title_is_duplicate(
        "BYD overtakes Tesla in global EV sales",
        ["Toyota unveils 2026 Camry"],
        threshold=0.85,
    )


def test_empty_inputs_safe() -> None:
    assert not title_is_duplicate("", [], threshold=0.85)
    assert not title_is_duplicate("anything", [], threshold=0.85)
