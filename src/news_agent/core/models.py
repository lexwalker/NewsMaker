"""Pydantic domain models shared across layers."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

Portal = Literal["RU", "UZ", "KZ"]
Region = Literal["Local", "Global"]


class Source(BaseModel):
    """A monitored source (RSS feed or HTML index page)."""

    model_config = ConfigDict(frozen=True)

    name: str
    url: str
    type: Literal["rss", "html"] = "html"
    is_active: bool = True
    requires_js: bool = False
    rate_limit_rps: float | None = None
    language: str | None = None  # ISO 639-1, e.g. "ru", "en"


class RawArticle(BaseModel):
    """Output of a fetcher — raw, untrusted, unclassified."""

    url: str
    title: str
    body: str
    html: str | None = None
    published_at: datetime | None = None
    image_url: str | None = None
    images: list[str] = Field(default_factory=list)
    outbound_links: list[str] = Field(default_factory=list)
    source_name: str
    source_url: str
    source_language: str | None = None


class Candidate(BaseModel):
    """Article that survived freshness + dedup and is headed for classification."""

    raw: RawArticle
    url_hash: str
    canonical_url: str
    source_domain: str


class Classification(BaseModel):
    """LLM structured output for the section classifier."""

    section: str
    region: Region
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""


class RelevanceCheck(BaseModel):
    """LLM structured output for the cheap automotive/economy filter."""

    is_automotive_or_economy: bool
    reason: str = ""


class TitlePair(BaseModel):
    """English + Russian title pair with source-language code."""

    english: str
    russian: str
    source_language: str  # uppercase ISO code, e.g. "EN", "RU", "UZ"


class ClassifiedNews(BaseModel):
    """Fully-enriched article ready to be written to Sheets."""

    candidate: Candidate
    titles: TitlePair
    classification: Classification
    primary_source_url: str
    primary_source_domain: str
    primary_source_confidence: Literal["high", "medium", "low"]
    image_url: str | None
    llm_provider: str


class OutputRow(BaseModel):
    """Exactly the 13 columns appended to the output sheet, in order."""

    start_date: str  # ISO 8601 UTC
    section: str
    name: str  # English title
    localized_title: str  # "{en} / {ru} ({LANG})"
    announcement_image: str
    region: Region
    country: str  # "Russia [7]"
    primary_source_url: str
    primary_source_domain: str
    aggregator_url: str
    published_at: str
    llm_provider: str
    confidence: float

    def as_row(self) -> list[str | float]:
        return [
            self.start_date,
            self.section,
            self.name,
            self.localized_title,
            self.announcement_image,
            self.region,
            self.country,
            self.primary_source_url,
            self.primary_source_domain,
            self.aggregator_url,
            self.published_at,
            self.llm_provider,
            round(self.confidence, 3),
        ]

    @staticmethod
    def header() -> list[str]:
        return [
            "Start date",
            "Section",
            "Name",
            "Localized title",
            "Announcement image",
            "Region",
            "Country",
            "Primary source URL",
            "Primary source domain",
            "Aggregator URL",
            "Published at",
            "LLM provider",
            "Confidence",
        ]


class SectionDefinition(BaseModel):
    """Loaded from config/sections.yaml or the Sheets tab."""

    name: str
    description: str = ""
    examples: list[str] = Field(default_factory=list)


class FewShotExample(BaseModel):
    """Training example pulled from the curated News tab."""

    title: str
    section: str
    region: Region | None = None


class LLMUsage(BaseModel):
    """Per-call token accounting."""

    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    latency_ms: int = 0
    provider: str = ""
    model: str = ""


class RunSummary(BaseModel):
    """End-of-run stats written to the log."""

    portal: Portal
    sources_total: int = 0
    sources_ok: int = 0
    sources_failed: int = 0
    candidates_found: int = 0
    candidates_after_filters: int = 0
    rows_written: int = 0
    total_cost_usd: float = 0.0
    aborted: bool = False
    abort_reason: str | None = None


__all__ = [
    "Candidate",
    "Classification",
    "ClassifiedNews",
    "FewShotExample",
    "HttpUrl",
    "LLMUsage",
    "OutputRow",
    "Portal",
    "RawArticle",
    "Region",
    "RelevanceCheck",
    "RunSummary",
    "SectionDefinition",
    "Source",
    "TitlePair",
]
