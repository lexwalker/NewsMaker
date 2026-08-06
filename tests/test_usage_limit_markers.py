"""Recognising a dead account on the FIRST refusal, not the fifth.

Two mechanisms stop a pass when the API stops answering. The generic one counts
five consecutive failures of any wording; the fast one recognises the wording
and stops immediately. The generic rule is the safety net and always worked —
but every call it spends is retried three times inside the client, so the
difference between the two is roughly twenty doomed round-trips.

Until aug-06 the fast path did not know the wording the API actually returns
when an account runs out of money, which is the way these runs die in practice:
twice on aug-06 alone. The real 400 from the 13:35 hot run is the first case
below, quoted from logs/run_hot_20260806_133507.log.
"""

from __future__ import annotations

import pytest

from news_agent.core.editorial_pass import looks_like_usage_limit


@pytest.mark.parametrize("message", [
    # The exact 400 that killed the aug-06 13:35 run at candidate 95 of 282.
    "Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', "
    "'message': 'Your credit balance is too low to access the Anthropic API. "
    "Please go to Plans & Billing to upgrade or purchase credits.'}}",
    "Your credit balance is too low",
    "insufficient credits remaining",
    "This request would exceed your organization's usage limit",
    "You have reached your specified API usage limits",
])
def test_a_dead_account_is_recognised_at_once(message) -> None:
    assert looks_like_usage_limit(message)


@pytest.mark.parametrize("message", [
    "Connection error",
    "Error code: 529 - overloaded_error",
    "Error code: 429 - rate_limit_error: please retry",
    "Error code: 404 - model not found: claude-sonnet-4-7",
    "read timeout",
    "",
])
def test_transient_and_unrelated_failures_are_left_to_the_counter(message) -> None:
    """These can succeed on the next call, so aborting the whole pass on the
    first one would throw away a run over a network hiccup — the jul-29 lesson,
    where a single Connection error killed a chain and cost ~10 clusters."""
    assert not looks_like_usage_limit(message)


def test_matching_is_case_insensitive() -> None:
    assert looks_like_usage_limit("YOUR CREDIT BALANCE IS TOO LOW")


def test_none_is_not_a_limit() -> None:
    assert not looks_like_usage_limit(None)  # type: ignore[arg-type]
