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


def _hash8(*chunks: bytes) -> str:
    h = hashlib.sha256()
    for c in chunks:
        h.update(c)
    return h.hexdigest()[:8]


def strip_python_noise(source: bytes) -> bytes:
    """Source with comments, docstrings and blank lines removed.

    The cost audit's #1 own-goal: editing a COMMENT in
    heuristic_relevance.py bumped the classifier version and invalidated
    the whole (expensive) LLM cache. Comments cannot change a verdict, so
    they must not change the fingerprint. Tokenize-based, so a '#' inside
    a string literal is preserved.
    """
    import io as _io
    import tokenize

    try:
        toks = list(tokenize.tokenize(_io.BytesIO(source).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return source  # unparseable → fall back to the raw bytes
    out: list[str] = []
    prev_type = tokenize.INDENT
    for tok in toks:
        if tok.type in (tokenize.COMMENT, tokenize.NL, tokenize.ENCODING):
            continue
        if tok.type == tokenize.STRING and prev_type in (
            tokenize.INDENT, tokenize.DEDENT, tokenize.NEWLINE,
        ):
            continue  # docstring in statement position
        out.append(tok.string)
        if tok.type != tokenize.NEWLINE or out[-1:] != ["\n"]:
            prev_type = tok.type
    return "".join(out).encode("utf-8")


def compute_split_versions(prompt: str, heuristics_source: bytes) -> tuple[str, str]:
    """(prompt_version, heuristics_version) — the two INDEPENDENT inputs.

    A single combined stamp made every prompt edit invalidate the cheap
    rule verdicts and — far worse — every heuristics edit invalidate the
    EXPENSIVE LLM verdicts, which cost ~$2 to rebuild per run. The two
    verdict families depend on different inputs, so they get different
    stamps; the heuristics stamp ignores comments/docstrings.
    """
    return _hash8(prompt.encode("utf-8")), _hash8(strip_python_noise(heuristics_source))


# Direction of a prompt change, declared by whoever edits the constitution.
# A rule that only ADDS a reject cannot turn a cached reject into a publish,
# so cached LLM REJECTS stay valid and only the (far fewer) accepted rows
# re-run — measured jul-27: 150 accepted vs 759 LLM-rejected in 2 days, i.e.
# ~5x fewer paid calls for a reject-only edit. "both" = the safe default.
PROMPT_CHANGE_BOTH = "both"
PROMPT_CHANGE_REJECT_ONLY = "reject_only"    # new/《wider》 reject rule
PROMPT_CHANGE_PUBLISH_ONLY = "publish_only"  # rescue / new publish route

_LLM_REJECT_VERDICTS = frozenset({"Отклонено LLM"})


def llm_cache_survives_prompt_change(cached_verdict: str, direction: str) -> bool:
    """Whether a cached LLM verdict survives a prompt change of ``direction``.

    Reject-only edits cannot rescue anything → cached rejects stay valid.
    Publish-only edits cannot newly reject anything → cached accepts stay
    valid. Anything else (or an unknown direction) → nothing survives.
    """
    is_reject = cached_verdict in _LLM_REJECT_VERDICTS
    if direction == PROMPT_CHANGE_REJECT_ONLY:
        return is_reject
    if direction == PROMPT_CHANGE_PUBLISH_ONLY:
        return not is_reject
    return False


def cache_is_authoritative(
    cached_verdict: str,
    has_llm_classification: bool,
    version_ok: bool,
    *,
    prompt_ok: bool | None = None,
    heuristics_ok: bool | None = None,
    prompt_change: str = PROMPT_CHANGE_BOTH,
) -> bool:
    """Whether a cached row may short-circuit fresh processing.

    Identity verdicts are independent of the classifier and always win.

    With the SPLIT stamps (``prompt_ok`` / ``heuristics_ok`` given, i.e. the
    cache row carries them) each verdict family is checked against the input
    it actually depends on: rule verdicts ← heuristics, LLM verdicts ←
    prompt. A stale-prompt LLM row can still survive when the prompt change
    was declared reject-only / publish-only and the verdict is on the side
    that such a change cannot flip.

    Legacy rows (no split stamps) fall back to the combined ``version_ok``,
    so nothing changes for cache written before this feature.
    """
    if cached_verdict in IDENTITY_VERDICTS:
        return True
    split = prompt_ok is not None and heuristics_ok is not None
    if cached_verdict in RULE_VERDICTS:
        return bool(heuristics_ok) if split else version_ok
    if has_llm_classification:
        if not split:
            return version_ok
        if prompt_ok:
            return True
        return llm_cache_survives_prompt_change(cached_verdict, prompt_change)
    return False
