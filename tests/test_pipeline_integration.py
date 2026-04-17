"""End-to-end pipeline with mocked Sheets, fetcher and LLM.

Asserts that a row would be written with the expected shape when a fresh
article makes it through all filters.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from news_agent.adapters.llm.base import LLMClient  # noqa: F401 (protocol for typing)
from news_agent.core.models import (
    Classification,
    FewShotExample,
    LLMUsage,
    RawArticle,
    RelevanceCheck,
    SectionDefinition,
    Source,
    TitlePair,
)
from news_agent.pipeline import run as pipeline_mod
from news_agent.settings import Settings


class FakeLLM:
    provider_name = "fake"
    model = "fake-1"

    def is_automotive(self, title: str, body_excerpt: str) -> tuple[RelevanceCheck, LLMUsage]:
        return RelevanceCheck(is_automotive_or_economy=True, reason="ok"), _usage()

    def classify_section(self, **_: Any) -> tuple[Classification, LLMUsage]:
        return Classification(section="facts", region="Global", confidence=0.9, reasoning="ok"), _usage()

    def translate_title(self, *, title: str, source_language_hint: str | None) -> tuple[TitlePair, LLMUsage]:
        return TitlePair(english=title, russian="Тойота представила Camry", source_language="EN"), _usage()


def _usage() -> LLMUsage:
    return LLMUsage(input_tokens=100, output_tokens=20, cost_usd=0.0005, provider="fake", model="fake-1")


class FakeSheets:
    def __init__(self) -> None:
        self.written: list[Any] = []
        self.header_ensured = False

    def read_sources(self, *_: Any, **__: Any) -> list[Source]:
        return [Source(name="Example", url="https://example.com/news/", type="html", is_active=True, language="en")]

    def read_sections(self, *_: Any, **__: Any) -> list[SectionDefinition]:
        return []

    def read_few_shots(self, *_: Any, **__: Any) -> list[FewShotExample]:
        return []

    def read_existing_titles(self, *_: Any, **__: Any) -> list[str]:
        return []

    def ensure_output_header(self, tab: str) -> None:
        self.header_ensured = True

    def append_rows(self, tab: str, rows: list[Any]) -> int:
        self.written.extend(rows)
        return len(rows)


class FakeHTMLFetcher:
    def fetch(self, source: Source, max_items: int) -> list[RawArticle]:
        return [
            RawArticle(
                url="https://example.com/news/toyota-camry-2026",
                title="Toyota unveils 2026 Camry",
                body="Toyota today announced the 2026 Camry. According to the press release, "
                "pricing starts at $29,500. More at https://pressroom.toyota.com/camry.",
                published_at=datetime.now(timezone.utc),
                image_url="https://cdn.example.com/camry.jpg",
                outbound_links=["https://pressroom.toyota.com/camry"],
                source_name=source.name,
                source_url=source.url,
                source_language=source.language,
            )
        ]


class FakeRSS(FakeHTMLFetcher):
    pass


@pytest.fixture
def patched_pipeline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Settings, FakeSheets]:
    settings = Settings(
        spreadsheet_id="test",
        google_service_account_json=tmp_path / "sa.json",
        sqlite_path=tmp_path / "dedup.sqlite",
        log_dir=tmp_path / "logs",
        anthropic_api_key="x",
        openai_api_key="x",
    )
    # Avoid the service-account file existence check.
    fake_sheets = FakeSheets()
    monkeypatch.setattr(pipeline_mod, "SheetsClient", lambda *a, **kw: fake_sheets)
    monkeypatch.setattr(pipeline_mod, "make_llm_client", lambda *_a, **_kw: FakeLLM())
    monkeypatch.setattr(pipeline_mod, "HTMLFetcher", lambda *_a, **_kw: FakeHTMLFetcher())
    monkeypatch.setattr(pipeline_mod, "RSSFetcher", lambda *_a, **_kw: FakeRSS())
    monkeypatch.setattr(pipeline_mod, "make_http_client", lambda *_a, **_kw: None)
    monkeypatch.setattr(pipeline_mod, "RateLimiter", lambda *_a, **_kw: _NoopRate())
    monkeypatch.setattr(pipeline_mod, "RobotsCache", lambda *_a, **_kw: None)
    return settings, fake_sheets


class _NoopRate:
    def set_rate(self, *_a: Any, **_kw: Any) -> None:
        pass

    def wait(self, *_a: Any, **_kw: Any) -> None:
        pass


def test_pipeline_writes_row_for_fresh_article(
    patched_pipeline: tuple[Settings, FakeSheets],
) -> None:
    settings, sheets = patched_pipeline
    summary = pipeline_mod.run_pipeline(settings, "RU", dry_run=False, limit_per_source=5)

    assert summary.sources_total == 1
    assert summary.sources_ok == 1
    assert summary.rows_written == 1
    assert len(sheets.written) == 1
    row = sheets.written[0]
    assert row.section == "facts"
    assert row.country == "Russia [7]"
    assert row.name == "Toyota unveils 2026 Camry"
    assert "Тойота" in row.localized_title
    assert row.primary_source_domain == "pressroom.toyota.com"


def test_pipeline_is_idempotent(
    patched_pipeline: tuple[Settings, FakeSheets],
) -> None:
    settings, sheets = patched_pipeline
    pipeline_mod.run_pipeline(settings, "RU", limit_per_source=5)
    summary = pipeline_mod.run_pipeline(settings, "RU", limit_per_source=5)
    assert summary.rows_written == 0
    assert len(sheets.written) == 1  # only the first run wrote
