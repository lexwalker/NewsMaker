"""Load YAML configs with Pydantic validation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

from news_agent.core.models import SectionDefinition

CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


def _read_yaml(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class SourcesSchema(BaseModel):
    """Maps sheet columns → Source fields."""

    name: str = "name"
    url: str = "url"
    type: str = "type"
    is_active: str = "is_active"
    language: str | None = "language"


class SourceOverride(BaseModel):
    """Per-URL overrides from sources_overrides.yaml."""

    url: str
    requires_js: bool = False
    rate_limit_rps: float | None = None
    language: str | None = None
    rss_url: str | None = None


class BrandDomainEntry(BaseModel):
    brand: str
    aliases: list[str] = Field(default_factory=list)
    domains: list[str]


class PrimarySourceCues(BaseModel):
    """Cue phrases per language for primary-source detection."""

    phrases: dict[str, list[str]] = Field(default_factory=dict)
    press_release_hosts: list[str] = Field(default_factory=list)
    mirror_hosts: list[str] = Field(default_factory=list)


def load_sections() -> list[SectionDefinition]:
    data = _read_yaml(CONFIG_DIR / "sections.yaml") or {}
    items = data.get("sections", [])
    return [SectionDefinition(**item) for item in items]


def load_sources_schema() -> SourcesSchema:
    data = _read_yaml(CONFIG_DIR / "sources_schema.yaml") or {}
    return SourcesSchema(**(data.get("columns") or {}))


def load_sources_overrides() -> list[SourceOverride]:
    data = _read_yaml(CONFIG_DIR / "sources_overrides.yaml") or {}
    items = data.get("overrides", [])
    return [SourceOverride(**item) for item in items]


def load_brand_domains() -> list[BrandDomainEntry]:
    data = _read_yaml(CONFIG_DIR / "brand_domains.yaml") or {}
    items = data.get("brands", [])
    return [BrandDomainEntry(**item) for item in items]


def load_primary_source_cues() -> PrimarySourceCues:
    data = _read_yaml(CONFIG_DIR / "primary_source_cues.yaml") or {}
    return PrimarySourceCues(
        phrases=data.get("phrases", {}),
        press_release_hosts=data.get("press_release_hosts", []),
        mirror_hosts=data.get("mirror_hosts", []),
    )


def load_whitelist_domains() -> set[str]:
    """Domains the editor has historically trusted (≥10 published items)."""
    data = _read_yaml(CONFIG_DIR / "whitelist_domains.yaml") or {}
    return {d.strip().lower() for d in data.get("domains", []) if d}


class Blacklist(BaseModel):
    """Hard-reject rules from the editorial team (see blacklist.yaml)."""

    topic_phrases_ru: list[str] = Field(default_factory=list)
    topic_phrases_en: list[str] = Field(default_factory=list)
    domains: list[str] = Field(default_factory=list)

    def all_phrases(self) -> list[str]:
        return [*self.topic_phrases_ru, *self.topic_phrases_en]


class HttpQuirks(BaseModel):
    """Per-domain HTTP workarounds (see config/http_quirks.yaml)."""

    ssl_insecure: set[str] = Field(default_factory=set)
    url_rewrites: dict[str, str] = Field(default_factory=dict)
    playwright_domains: set[str] = Field(default_factory=set)
    impersonate_domains: set[str] = Field(default_factory=set)


def load_http_quirks() -> HttpQuirks:
    data = _read_yaml(CONFIG_DIR / "http_quirks.yaml") or {}
    return HttpQuirks(
        ssl_insecure={d.strip().lower() for d in data.get("ssl_insecure", []) if d},
        url_rewrites={str(k): str(v) for k, v in (data.get("url_rewrites") or {}).items()},
        playwright_domains={d.strip().lower() for d in data.get("playwright_domains", []) if d},
        impersonate_domains={d.strip().lower() for d in data.get("impersonate_domains", []) if d},
    )


def load_blacklist() -> Blacklist:
    data = _read_yaml(CONFIG_DIR / "blacklist.yaml") or {}
    return Blacklist(
        topic_phrases_ru=[s.lower() for s in data.get("topic_phrases_ru", []) if s],
        topic_phrases_en=[s.lower() for s in data.get("topic_phrases_en", []) if s],
        domains=[d.lower() for d in data.get("domains", []) if d],
    )


class SourceQuality(BaseModel):
    """Per-source editorial quality flags (see config/source_quality.yaml)."""

    low_quality: set[str] = Field(default_factory=set)
    high_quality: set[str] = Field(default_factory=set)

    def is_low_quality(self, url: str | None) -> bool:
        """True if ``url`` matches a low-quality entry by exact URL or domain.

        Telegram channel URLs are matched as full ``t.me/<channel>``
        strings; everything else falls back to a domain comparison.
        """
        if not url:
            return False
        u = url.strip().lower().rstrip("/")
        # Exact URL match (t.me channels)
        for entry in self.low_quality:
            e = entry.strip().lower().rstrip("/")
            if u == e or u.startswith(e + "/"):
                return True
        # Domain match
        from news_agent.core.urls import domain_of  # avoid circular import
        host = domain_of(url)
        for entry in self.low_quality:
            e = entry.strip().lower().rstrip("/")
            if "/" in e:
                continue  # URL-only entry, already checked above
            if host == e or host.endswith("." + e):
                return True
        return False


def load_source_quality() -> SourceQuality:
    data = _read_yaml(CONFIG_DIR / "source_quality.yaml") or {}
    return SourceQuality(
        low_quality={s.strip() for s in data.get("low_quality", []) if s},
        high_quality={s.strip() for s in data.get("high_quality", []) if s},
    )


__all__ = [
    "Blacklist",
    "BrandDomainEntry",
    "HttpQuirks",
    "PrimarySourceCues",
    "SourceOverride",
    "SourceQuality",
    "SourcesSchema",
    "load_blacklist",
    "load_brand_domains",
    "load_http_quirks",
    "load_primary_source_cues",
    "load_sections",
    "load_source_quality",
    "load_sources_overrides",
    "load_sources_schema",
    "load_whitelist_domains",
]
