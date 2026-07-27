"""Classifier-cache versioning (peer-review §4).

The per-URL classification cache used to be keyed by url_hash alone, so a
prompt/heuristic edit had no effect on already-cached articles — they
short-circuited with the stale verdict. Now every cache row carries a
classifier version; on restore, rule-based verdicts are honoured only
when the version matches, while identity verdicts (dups/stale) stay valid
regardless. These tests pin that contract.
"""

from news_agent.core.cache_version import (
    IDENTITY_VERDICTS,
    RULE_VERDICTS,
    cache_is_authoritative,
    compute_classifier_version,
)


# ── version fingerprint ─────────────────────────────────────────────

def test_version_is_deterministic() -> None:
    a = compute_classifier_version("prompt", b"heuristics")
    b = compute_classifier_version("prompt", b"heuristics")
    assert a == b
    assert len(a) == 8


def test_prompt_edit_bumps_version() -> None:
    base = compute_classifier_version("prompt v1", b"heur")
    edited = compute_classifier_version("prompt v2", b"heur")
    assert base != edited


def test_heuristics_edit_bumps_version() -> None:
    base = compute_classifier_version("prompt", b"heur v1")
    edited = compute_classifier_version("prompt", b"heur v2")
    assert base != edited


def test_extra_parts_are_order_sensitive() -> None:
    """Two binary parts in different order must give different versions
    (guards against accidental commutativity)."""
    one = compute_classifier_version("p", b"AAAA", b"BBBB")
    two = compute_classifier_version("p", b"BBBB", b"AAAA")
    assert one != two


def test_no_extra_parts_still_works() -> None:
    v = compute_classifier_version("just the prompt")
    assert len(v) == 8


# ── authoritative decision ──────────────────────────────────────────

def test_llm_verdict_needs_matching_version() -> None:
    # has_llm_classification=True, fresh section verdict
    assert cache_is_authoritative("Точно новость", True, version_ok=True)
    # version mismatch → NOT authoritative → re-classify
    assert not cache_is_authoritative("Точно новость", True, version_ok=False)


def test_rule_verdict_needs_matching_version() -> None:
    for v in RULE_VERDICTS:
        assert cache_is_authoritative(v, False, version_ok=True), v
        # blacklist/heuristic edit (version bump) → row re-runs fresh,
        # giving a previously-killed article its second chance
        assert not cache_is_authoritative(v, False, version_ok=False), v


def test_identity_verdict_is_version_independent() -> None:
    for v in IDENTITY_VERDICTS:
        # a dup is a dup / stale is stale regardless of rule changes
        assert cache_is_authoritative(v, False, version_ok=True), v
        assert cache_is_authoritative(v, False, version_ok=False), v


def test_dup_specifically_survives_version_bump() -> None:
    """The whole point: after a prompt edit we must NOT re-push a URL we
    already classified-as-dup — that would resurrect every old dup."""
    assert cache_is_authoritative(
        "Отклонить (дубль финального URL)", False, version_ok=False
    )


def test_unknown_verdict_without_llm_not_authoritative() -> None:
    """A fetch-only leftover (no llm_section, not a known verdict) must
    fall through to fresh processing regardless of version."""
    assert not cache_is_authoritative("", False, version_ok=True)
    assert not cache_is_authoritative("какой-то частичный", False, True)


def test_identity_and_rule_sets_disjoint() -> None:
    assert not (IDENTITY_VERDICTS & RULE_VERDICTS)


# --- split stamps + directional prompt changes (jul-27 cost work) ---------

from news_agent.core.cache_version import (  # noqa: E402
    PROMPT_CHANGE_BOTH,
    PROMPT_CHANGE_PUBLISH_ONLY,
    PROMPT_CHANGE_REJECT_ONLY,
    compute_split_versions,
    strip_python_noise,
)


def test_comment_edit_does_not_move_heuristics_version() -> None:
    src = b"def f(x):\n    return x + 1\n"
    _, h1 = compute_split_versions("P", src)
    _, h2 = compute_split_versions("P", src + b"\n# a harmless comment\n")
    assert h1 == h2


def test_docstring_edit_does_not_move_heuristics_version() -> None:
    a = b'def f(x):\n    """One doc."""\n    return x\n'
    b = b'def f(x):\n    """Another doc, rewritten."""\n    return x\n'
    assert compute_split_versions("P", a)[1] == compute_split_versions("P", b)[1]


def test_real_code_edit_moves_heuristics_version() -> None:
    a = b"def f(x):\n    return x + 1\n"
    b = b"def f(x):\n    return x + 2\n"
    assert compute_split_versions("P", a)[1] != compute_split_versions("P", b)[1]


def test_prompt_and_heuristics_versions_are_independent() -> None:
    src = b"def f(x):\n    return x\n"
    p1, h1 = compute_split_versions("PROMPT A", src)
    p2, h2 = compute_split_versions("PROMPT B", src)
    assert p1 != p2 and h1 == h2


def test_unparseable_source_falls_back_to_raw_bytes() -> None:
    bad = b"def f(:\n  ???\n"
    assert strip_python_noise(bad) == bad


def test_rule_verdict_survives_prompt_only_change() -> None:
    # A constitution edit must not invalidate cheap heuristic verdicts.
    assert cache_is_authoritative(
        "Точно не новость (не авто)", False, False,
        prompt_ok=False, heuristics_ok=True)


def test_llm_verdict_survives_heuristics_only_change() -> None:
    # …and a heuristics edit must not invalidate the expensive LLM cache.
    assert cache_is_authoritative(
        "Точно новость", True, False, prompt_ok=True, heuristics_ok=False)


def test_reject_only_change_keeps_cached_llm_rejects() -> None:
    assert cache_is_authoritative(
        "Отклонено LLM", True, False, prompt_ok=False, heuristics_ok=True,
        prompt_change=PROMPT_CHANGE_REJECT_ONLY)
    # …but accepted rows must re-run — a new reject rule may kill them.
    assert not cache_is_authoritative(
        "Точно новость", True, False, prompt_ok=False, heuristics_ok=True,
        prompt_change=PROMPT_CHANGE_REJECT_ONLY)


def test_publish_only_change_keeps_cached_accepts() -> None:
    assert cache_is_authoritative(
        "Точно новость", True, False, prompt_ok=False, heuristics_ok=True,
        prompt_change=PROMPT_CHANGE_PUBLISH_ONLY)
    assert not cache_is_authoritative(
        "Отклонено LLM", True, False, prompt_ok=False, heuristics_ok=True,
        prompt_change=PROMPT_CHANGE_PUBLISH_ONLY)


def test_default_both_invalidates_every_llm_verdict() -> None:
    for verdict in ("Точно новость", "Отклонено LLM"):
        assert not cache_is_authoritative(
            verdict, True, False, prompt_ok=False, heuristics_ok=True,
            prompt_change=PROMPT_CHANGE_BOTH)


def test_legacy_rows_without_split_stamps_use_combined_version() -> None:
    # Cache written before this feature: prompt_ok/heuristics_ok are None.
    assert cache_is_authoritative("Точно новость", True, True)
    assert not cache_is_authoritative("Точно новость", True, False)


def test_identity_verdicts_ignore_every_stamp() -> None:
    assert cache_is_authoritative(
        "Отклонить (дубль)", False, False, prompt_ok=False, heuristics_ok=False)
