"""LLMClient protocol and shared prompt-building helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Protocol

from news_agent.core.models import (
    Classification,
    FewShotExample,
    LLMUsage,
    RelevanceCheck,
    SectionDefinition,
    TitlePair,
)


class LLMCallResult(Protocol):
    """Bundle returned by every LLM call for bookkeeping."""

    usage: LLMUsage


class LLMClient(Protocol):
    """Provider-agnostic LLM facade."""

    provider_name: str
    model: str

    def is_automotive(self, title: str, body_excerpt: str) -> tuple[RelevanceCheck, LLMUsage]:
        ...

    def classify_section(
        self,
        *,
        title: str,
        body: str,
        sections: list[SectionDefinition],
        few_shots: list[FewShotExample],
        portal_country: str,
    ) -> tuple[Classification, LLMUsage]:
        ...

    def translate_title(
        self, *, title: str, source_language_hint: str | None
    ) -> tuple[TitlePair, LLMUsage]:
        ...


# -------------------------------------------------- shared prompt components
RELEVANCE_SYSTEM = (
    "You are a strict binary filter for an automotive news aggregator. "
    "Return true only if the article is about the automotive industry, "
    "automotive market economy, car brands, dealer networks, auto regulation, "
    "or closely auto-adjacent macroeconomics that directly affects car sales or prices. "
    "Return false for general politics, sports, non-auto tech, entertainment, etc."
)

CLASSIFY_SYSTEM = (
    "You classify automotive / economy news into one of a fixed list of sections. "
    "You also decide whether the news is specifically about the portal's country (Local) "
    "or not (Global). You return structured JSON only. "
    "If the news is NOT automotive or auto-economy at all, still pick the closest section "
    "but set confidence ≤ 0.2."
)

TRANSLATE_SYSTEM = (
    "You produce an English and a Russian version of a news headline. "
    "Preserve meaning; do not editorialise. Use natural, neutral news-wire style. "
    "Also identify the source language as a two-letter ISO code (uppercase)."
)


def build_classify_user_prompt(
    *,
    title: str,
    body: str,
    sections: list[SectionDefinition],
    few_shots: list[FewShotExample],
    portal_country: str,
) -> str:
    """Legacy one-shot prompt (kept for OpenAI path which has no explicit cache)."""
    sections_block = "\n".join(
        f"- {s.name}: {s.description.strip()}" for s in sections
    )
    few_shot_block = ""
    if few_shots:
        lines = [f"  • [{fs.section}] {fs.title}" for fs in few_shots[:20]]
        few_shot_block = "Few-shot examples from curated news:\n" + "\n".join(lines) + "\n\n"
    body_trunc = body[:4000]
    valid = ", ".join(s.name for s in sections)
    return (
        f"{few_shot_block}"
        f"Portal country: {portal_country}.\n"
        f"Task: classify the news below into exactly one of: {valid}.\n"
        f"Also set region='Local' iff the news is specifically about {portal_country}.\n\n"
        f"Sections:\n{sections_block}\n\n"
        f"Title: {title}\n\nBody:\n{body_trunc}"
    )


# ---------- cache-friendly split: static system + dynamic user ----------
# Static part is identical across all 89 articles in a single batch run.
# Anthropic prompt caching gives it 90% discount on reads.

def build_classify_system(
    sections: list[SectionDefinition],
    few_shots: list[FewShotExample],
    portal_country: str,
) -> str:
    sections_block = "\n".join(
        f"- {s.name}: {s.description.strip()}" for s in sections
    )
    few_shot_block = ""
    if few_shots:
        lines = [f"  • [{fs.section}] {fs.title}" for fs in few_shots[:20]]
        few_shot_block = "\nFew-shot examples from curated news:\n" + "\n".join(lines)
    valid = ", ".join(s.name for s in sections)
    return (
        f"{CLASSIFY_SYSTEM}\n\n"
        f"Portal country: {portal_country}.\n"
        f"Task: classify every news item that follows into exactly one of: {valid}.\n"
        f"Also set region='Local' iff the news is specifically about {portal_country}.\n\n"
        f"Sections:\n{sections_block}"
        f"{few_shot_block}"
    )


def build_classify_user(title: str, body: str) -> str:
    return f"Title: {title}\n\nBody:\n{body[:4000]}"


def prompt_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\n---\n")
    return h.hexdigest()[:16]


# JSON Schema reused by both providers
CLASSIFY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["section", "region", "confidence", "reasoning"],
    "properties": {
        "section": {"type": "string"},
        "region": {"type": "string", "enum": ["Local", "Global"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "reasoning": {"type": "string"},
    },
}

RELEVANCE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["is_automotive_or_economy", "reason"],
    "properties": {
        "is_automotive_or_economy": {"type": "boolean"},
        "reason": {"type": "string"},
    },
}

TRANSLATE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["english", "russian", "source_language"],
    "properties": {
        "english": {"type": "string"},
        "russian": {"type": "string"},
        "source_language": {
            "type": "string",
            "pattern": "^[A-Z]{2}$",
        },
    },
}


def dumps(obj: object) -> str:
    return json.dumps(obj, ensure_ascii=False)
