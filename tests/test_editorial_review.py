"""Tests for the consolidated editorial_review LLM call.

These cover the EditorialReview model + the prompt-builder helpers.
The actual LLM call is provider-specific and tested via integration in
the batch_fetch_test smoke run; here we only test:
  - The Pydantic model accepts/rejects expected shapes
  - The schema matches the model fields
  - Prompt builders produce correct output structure
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from news_agent.adapters.llm.base import (
    EDITORIAL_REVIEW_SCHEMA,
    EDITORIAL_REVIEW_SYSTEM,
    build_editorial_review_system,
    build_editorial_review_user,
)
from news_agent.core.models import EditorialReview, SectionDefinition


# ----------------------------------------------------- EditorialReview model

def test_review_publish_with_full_fields() -> None:
    r = EditorialReview(
        should_publish=True,
        section="Confirmed",
        region="Global",
        confidence=0.85,
        reason="Brand-confirmed model launch",
    )
    assert r.should_publish is True
    assert r.section == "Confirmed"
    assert r.region == "Global"


def test_review_reject_minimal_fields() -> None:
    r = EditorialReview(
        should_publish=False,
        confidence=0.9,
        reason="Yellow-press framing",
    )
    assert r.should_publish is False
    assert r.section == ""
    assert r.region is None


def test_review_confidence_bounds() -> None:
    with pytest.raises(ValidationError):
        EditorialReview(should_publish=True, confidence=1.5, reason="too high")
    with pytest.raises(ValidationError):
        EditorialReview(should_publish=True, confidence=-0.1, reason="too low")


def test_review_invalid_region_coerces_instead_of_raising() -> None:
    """An out-of-vocabulary region must NOT sink the verdict (jun-23).

    This asserted `raises(ValidationError)` until jul-30. The behaviour was
    deliberately inverted by _tolerant_region: one stray region string used to
    fail the whole EditorialReview, and since verdict+reason+event_signature
    arrive in ONE call, a lost verdict lost all three. None is safe —
    downstream reads it as Global."""
    r = EditorialReview(
        should_publish=True,
        section="Confirmed",
        region="Worldwide",  # not in the Region literal
        confidence=0.8,
        reason="x",
    )
    assert r.region is None
    assert r.should_publish is True  # the rest of the verdict survived


def test_review_region_is_case_normalised() -> None:
    assert EditorialReview(should_publish=True, region="global",
                           confidence=0.5, reason="x").region == "Global"


# ------------------------------------------------------------- schema

def test_schema_required_fields() -> None:
    assert "should_publish" in EDITORIAL_REVIEW_SCHEMA["required"]
    assert "confidence" in EDITORIAL_REVIEW_SCHEMA["required"]
    assert "reason" in EDITORIAL_REVIEW_SCHEMA["required"]


def test_schema_section_optional() -> None:
    """section + region are optional in schema (only required when publish)."""
    assert "section" not in EDITORIAL_REVIEW_SCHEMA["required"]
    assert "region" not in EDITORIAL_REVIEW_SCHEMA["required"]
    # but still in properties
    assert "section" in EDITORIAL_REVIEW_SCHEMA["properties"]
    assert "region" in EDITORIAL_REVIEW_SCHEMA["properties"]


# ------------------------------------------------------------- prompt builders

def test_system_prompt_contains_editor_rules() -> None:
    """Sanity: prompt must mention key editor rules."""
    system = build_editorial_review_system(
        sections=[SectionDefinition(name="Confirmed", description="Facts")],
        portal_country="Russia",
    )
    # Section name embedded
    assert "Confirmed" in system
    assert "Russia" in system
    # Key rules from editor's review surface
    assert "Local specifics" in system
    assert "Rumors" in system
    assert "should_publish" in system  # output schema cue
    # Portal country localised
    assert "iff the news is specifically about Russia" in system


def test_user_prompt_truncates_body() -> None:
    long_body = "x" * 5000
    out = build_editorial_review_user("Test title", long_body)
    assert "Title: Test title" in out
    assert len(out) < 5000  # body truncated to 4000


def test_system_prompt_baseline_length() -> None:
    """Prompt should be substantial — encodes 4 rounds of editor feedback."""
    assert len(EDITORIAL_REVIEW_SYSTEM) > 8000  # ~12k chars expected


# ----------------------------------- regression: known editor cases (textual)
#
# These guard that five rules the editor asked for IN PERSON survive prompt
# rewrites. They asserted the pre-constitution prompt's exact strings
# («корпоративный отпуск», "price drops", "asroad", "model launches",
# "Formula"/"NASCAR") and so all five broke on the jun-19 constitution — while
# every rule itself survived, reworded. They went unnoticed because the whole
# suite could not run at all (see core/console.py). Rewritten jul-30 against
# the constitution's own vocabulary; each still fails if the RULE is dropped.


def test_prompt_rejects_corporate_boilerplate() -> None:
    """Editor row 121: a corporate vacation notice is not a publishable item.
    The constitution generalised it to the whole boilerplate class."""
    assert "corporate boilerplate" in EDITORIAL_REVIEW_SYSTEM
    assert "personnel appointments" in EDITORIAL_REVIEW_SYSTEM


def test_prompt_rejects_per_model_price_moves() -> None:
    """Editor row 129: per-model price drops / dealer offers are not posted."""
    assert "per-model price changes" in EDITORIAL_REVIEW_SYSTEM
    # A model's official PRICE ANNOUNCEMENT stays publishable — the rule is
    # about routine repricing, and collapsing the two would lose real news.
    assert "pricing" in EDITORIAL_REVIEW_SYSTEM


def test_prompt_rejects_aggregator_without_brand_source() -> None:
    """Editor's asroad.org warning («99% репост») — the constitution states it
    as the general class instead of naming the one domain."""
    assert "агрегатор" in EDITORIAL_REVIEW_SYSTEM
    assert "без официального источника" in EDITORIAL_REVIEW_SYSTEM


def test_prompt_carries_launch_lifecycle_vocabulary() -> None:
    """Phase-1 launch-lifecycle awareness: the event vocabulary must still
    distinguish a first market launch / sales start from a teaser."""
    assert "first market launch" in EDITORIAL_REVIEW_SYSTEM
    assert "sales start" in EDITORIAL_REVIEW_SYSTEM


def test_prompt_rejects_motorsport_coverage() -> None:
    """Motorsport is rejected — but a CAR unveiled at a race is still a car,
    and that carve-out must survive with the rule."""
    assert "motorsport race results" in EDITORIAL_REVIEW_SYSTEM
    assert "not motorsport coverage" in EDITORIAL_REVIEW_SYSTEM
