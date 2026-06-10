"""Classifier-cache versioning.

The per-URL classification cache is keyed by ``url_hash`` alone. Without a
version stamp, a prompt or heuristic edit had **zero** effect on already
cached articles — they short-circuited with the stale verdict, so the only
way to observe a rule change was to wait for genuinely fresh articles
(same-day re-runs were degenerate). This module stamps every cache row
with a classifier version = ``sha256(prompt + heuristics source)[:8]`` and,
on restore, honours rule-based verdicts only when the stamp matches the
live version; on a mismatch the row re-runs through the current
heuristics + LLM.

Kept as pure, dependency-free domain logic so it is unit-testable without
importing the batch script (which rebinds ``sys.stdout`` at import time).
"""

from __future__ import annotations

import hashlib

# Identity verdicts are facts about URL/time, not classification rules —
# always authoritative regardless of classifier version (a dup is a dup;
# a stale-by-date article is still stale).
IDENTITY_VERDICTS = frozenset({
    "Отклонить (дубль)",
    "Отклонить (дубль финального URL)",
    "Отклонить (обработан ранее)",
    "Точно не новость (старая)",
})

# Rule verdicts are heuristic outputs (looks_like_article /
# is_auto_or_economy / blacklist). They short-circuit the LLM but DO
# depend on the heuristics module → honoured only when the version
# matches; otherwise the row re-runs, so a blacklist edit gives a
# previously-killed article its second chance (peer-review §3 S3).
RULE_VERDICTS = frozenset({
    "Точно не новость (не статья)",
    "Точно не новость (не авто)",
    "Точно не новость (чёрный список)",
})


def compute_classifier_version(prompt: str, *parts: bytes) -> str:
    """Return an 8-hex-char fingerprint of the classifier.

    ``prompt`` is the editorial-review system prompt; ``parts`` are any
    extra binary inputs that change the classification (e.g. the
    ``heuristic_relevance.py`` source bytes). Editing any input changes
    the fingerprint, which invalidates stale cache rows on the next run.
    """
    h = hashlib.sha256()
    h.update(prompt.encode("utf-8"))
    for p in parts:
        h.update(p)
    return h.hexdigest()[:8]


def cache_is_authoritative(
    cached_verdict: str, has_llm_classification: bool, version_ok: bool
) -> bool:
    """Whether a cached row may short-circuit fresh processing.

    Identity verdicts are independent of the classifier and always win.
    Rule verdicts and LLM classifications depend on the prompt/heuristics,
    so they short-circuit only when the cached classifier version matches
    the live one (``version_ok``); on a mismatch the row re-runs fresh.
    """
    if cached_verdict in IDENTITY_VERDICTS:
        return True
    if has_llm_classification or cached_verdict in RULE_VERDICTS:
        return version_ok
    return False
