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


def test_review_invalid_region() -> None:
    with pytest.raises(ValidationError):
        EditorialReview(
            should_publish=True,
            section="Confirmed",
            region="Worldwide",  # not in Region literal
            confidence=0.8,
            reason="x",
        )


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

def test_prompt_mentions_corporate_vacation_rule() -> None:
    """Editor row 121: vacation NOT a publishable item."""
    assert "корпоративный отпуск" in EDITORIAL_REVIEW_SYSTEM


def test_prompt_mentions_per_model_price_drops() -> None:
    """Editor row 129: per-model price drops not posted."""
    assert "price drops" in EDITORIAL_REVIEW_SYSTEM.lower()


def test_prompt_mentions_asroad_warning() -> None:
    """Editor warning about asroad.org being 99% repost."""
    assert "asroad" in EDITORIAL_REVIEW_SYSTEM.lower()


def test_prompt_mentions_stage_lifecycle() -> None:
    """Phase 1 launch lifecycle awareness."""
    assert "model launches" in EDITORIAL_REVIEW_SYSTEM.lower()


def test_prompt_mentions_motorsport_reject() -> None:
    """Motorsport always rejected."""
    assert "Formula" in EDITORIAL_REVIEW_SYSTEM
    assert "NASCAR" in EDITORIAL_REVIEW_SYSTEM
