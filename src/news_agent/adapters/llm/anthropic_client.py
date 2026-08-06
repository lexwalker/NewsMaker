"""Anthropic Claude implementation of LLMClient."""

from __future__ import annotations

import json
import time
from typing import Any

from anthropic import Anthropic, APIStatusError, RateLimitError
from pydantic import ValidationError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from news_agent.adapters.llm.base import (
    CLASSIFY_SCHEMA,
    EDITORIAL_REVIEW_BATCH_SCHEMA,
    EDITORIAL_REVIEW_SCHEMA,
    MATCH_PUBLISHED_SCHEMA,
    MATCH_PUBLISHED_SYSTEM,
    PICK_PRIMARY_SCHEMA,
    PICK_PRIMARY_SYSTEM,
    RELEVANCE_SCHEMA,
    RELEVANCE_SYSTEM,
    TRANSLATE_SCHEMA,
    TRANSLATE_SYSTEM,
    build_classify_system,
    build_classify_user,
    build_editorial_review_batch_user,
    build_editorial_review_system,
    build_editorial_review_user,
    prompt_hash,
)
from news_agent.adapters.llm.pricing import estimate_cost_with_cache
from news_agent.core.models import (
    Classification,
    EditorialReview,
    FewShotExample,
    LLMUsage,
    RelevanceCheck,
    SectionDefinition,
    TitlePair,
)
from news_agent.logging_setup import get_logger

log = get_logger("llm.anthropic")

def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, RateLimitError):
        return True
    if isinstance(exc, APIStatusError):
        status = getattr(exc, "status_code", None)
        return isinstance(status, int) and status >= 500
    return False


_RETRY = retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=15),
    retry=retry_if_exception(_should_retry),
)


def _tool(name: str, description: str, schema: dict[str, Any]) -> dict[str, Any]:
    return {"name": name, "description": description, "input_schema": schema}


class AnthropicLLMClient:
    provider_name = "anthropic"

    def __init__(self, api_key: str, model: str) -> None:
        self._client = Anthropic(api_key=api_key)
        self.model = model

    # ----------------------------------------------------------------- calls
    def is_automotive(self, title: str, body_excerpt: str) -> tuple[RelevanceCheck, LLMUsage]:
        tool = _tool(
            "record_relevance",
            "Record whether the news is automotive/economy relevant.",
            RELEVANCE_SCHEMA,
        )
        user = f"Title: {title}\n\nExcerpt:\n{body_excerpt[:500]}"
        data, usage = self._tool_call(
            system=RELEVANCE_SYSTEM, user=user, tool=tool, max_tokens=200
        )
        return RelevanceCheck.model_validate(data), usage

    def classify_section(
        self,
        *,
        title: str,
        body: str,
        sections: list[SectionDefinition],
        few_shots: list[FewShotExample],
        portal_country: str,
    ) -> tuple[Classification, LLMUsage]:
        tool = _tool(
            "classify_news",
            "Classify news into one section and locality.",
            CLASSIFY_SCHEMA,
        )
        # cache-friendly split: the sections + few-shots prefix is identical
        # across all articles in a batch, so we push it to `system` where the
        # ephemeral cache_control makes cache_read input ~90% cheaper.
        system = build_classify_system(sections, few_shots, portal_country)
        user = build_classify_user(title, body)
        data, usage = self._tool_call(
            system=system, user=user, tool=tool, max_tokens=500
        )
        return Classification.model_validate(data), usage

    def editorial_review(
        self,
        *,
        title: str,
        body: str,
        sections: list[SectionDefinition],
        portal_country: str,
    ) -> tuple[EditorialReview, LLMUsage]:
        """Consolidated editorial decision: replaces is_automotive +
        classify_section in a single call. Returns publish/skip + section
        + region + confidence + reason, encoding the editor's mental model
        from 4 rounds of feedback (300+ rules) in one prompt.
        """
        tool = _tool(
            "record_editorial_review",
            "Record the editor's verdict: should we publish this? "
            "If yes, in which section and region.",
            EDITORIAL_REVIEW_SCHEMA,
        )
        system = build_editorial_review_system(sections, portal_country)
        user = build_editorial_review_user(title, body)
        data, usage = self._tool_call(
            system=system, user=user, tool=tool, max_tokens=600
        )
        # Tolerate missing optional fields when should_publish=False
        if not data.get("should_publish"):
            data.setdefault("section", "")
            data.setdefault("region", None)
            data.setdefault("confidence", 0.5)
        # event_signature is schema-required but be defensive: a model
        # that omits it (or returns null) must not break the call —
        # dedup degrades to the lexical layers, never errors.
        es = data.get("event_signature")
        if not isinstance(es, dict):
            data["event_signature"] = None
        return EditorialReview.model_validate(data), usage

    @staticmethod
    def _coerce_review(data: dict[str, Any]) -> EditorialReview:
        """Fill the same optional gaps editorial_review tolerates."""
        if not data.get("should_publish"):
            data.setdefault("section", "")
            data.setdefault("region", None)
            data.setdefault("confidence", 0.5)
        if not isinstance(data.get("event_signature"), dict):
            data["event_signature"] = None
        return EditorialReview.model_validate(data)

    def editorial_review_batch(
        self,
        *,
        items: list[tuple[str, str]],
        sections: list[SectionDefinition],
        portal_country: str,
    ) -> tuple[list[EditorialReview | None], LLMUsage]:
        """Judge several articles in one call, cutting the count of times the
        8.2k-token constitution has to be read.

        Returns a list POSITIONALLY ALIGNED with ``items``; an entry is None when
        the model skipped that article or returned something that fails
        validation. Callers must judge those singly — a silent None would drop a
        story, and dropping it is exactly what the caller cannot notice.

        The whole call raising is left to propagate: the caller already knows how
        to fall back, and swallowing it here would hide a usage limit from the
        circuit breaker.
        """
        out: list[EditorialReview | None] = [None] * len(items)
        if not items:
            return out, LLMUsage(
                input_tokens=0, output_tokens=0, cost_usd=0.0,
                latency_ms=0, provider="anthropic", model=self.model)
        tool = _tool(
            "record_editorial_reviews",
            "Record one verdict per numbered article: should we publish it, "
            "and if so in which section and region.",
            EDITORIAL_REVIEW_BATCH_SCHEMA,
        )
        system = build_editorial_review_system(sections, portal_country)
        user = build_editorial_review_batch_user(items)
        data, usage = self._tool_call(
            system=system, user=user, tool=tool,
            # The single call gets 600 for one article; a batch needs room for
            # every verdict or the tail is truncated into a parse failure.
            max_tokens=min(8000, 400 * len(items) + 200),
        )
        for entry in (data.get("verdicts") or []):
            if not isinstance(entry, dict):
                continue
            n = entry.get("n")
            # A hallucinated or duplicated number must not overwrite a good
            # verdict or land on the wrong article: accept 1..len once each.
            if not isinstance(n, int) or not (1 <= n <= len(items)):
                continue
            if out[n - 1] is not None:
                continue
            try:
                out[n - 1] = self._coerce_review(
                    {k: v for k, v in entry.items() if k != "n"})
            except ValidationError:
                log.warning("batch review: unusable verdict for article %s", n)
        missing = sum(1 for r in out if r is None)
        if missing:
            log.warning(
                "batch review: %s of %s articles came back without a verdict",
                missing, len(items))
        return out, usage

    def translate_title(
        self, *, title: str, source_language_hint: str | None
    ) -> tuple[TitlePair, LLMUsage]:
        tool = _tool(
            "record_titles",
            "Record EN/RU translation of the headline.",
            TRANSLATE_SCHEMA,
        )
        hint = f" (source language hint: {source_language_hint})" if source_language_hint else ""
        user = f"Headline{hint}:\n{title}"
        data, usage = self._tool_call(
            system=TRANSLATE_SYSTEM, user=user, tool=tool, max_tokens=300
        )
        return TitlePair.model_validate(data), usage

    def pick_primary_source(
        self, *, title: str, body_excerpt: str, candidates: list[str]
    ) -> tuple[str | None, LLMUsage]:
        """Arbitrate the true primary among contested outbound links.

        ``candidates`` is the non-empty list from
        primary_source.arbitration_candidates. Returns (chosen_url, usage) where
        chosen_url is one of ``candidates`` or None ("none of these / keep the
        deterministic pick"). Anti-hallucination: the model returns a 1-based
        INDEX; we map it back to the caller's own list, so a fabricated URL is
        impossible and an out-of-range index degrades safely to None."""
        if not candidates:
            return None, LLMUsage(
                input_tokens=0, output_tokens=0, cost_usd=0.0,
                latency_ms=0, provider="anthropic", model=self.model)
        numbered = "\n".join(
            f"{i}. {url}" for i, url in enumerate(candidates, start=1))
        user = (
            f"Headline: {title}\n\n"
            f"Body excerpt:\n{body_excerpt[:1800]}\n\n"
            f"Outbound links:\n{numbered}\n\n"
            "Which numbered link is this article's original primary source? "
            "Answer 0 if none of them is."
        )
        tool = _tool(
            "record_primary",
            "Record which numbered outbound link is the original primary source.",
            PICK_PRIMARY_SCHEMA,
        )
        data, usage = self._tool_call(
            system=PICK_PRIMARY_SYSTEM, user=user, tool=tool, max_tokens=200
        )
        idx = data.get("index")
        if not isinstance(idx, int) or not (1 <= idx <= len(candidates)):
            return None, usage  # 0 / missing / hallucinated index → no override
        return candidates[idx - 1], usage

    def same_published_event(
        self, *, fresh: str, candidates: list[str]
    ) -> tuple[int | None, LLMUsage]:
        """Dup arbitration vs near-miss prior publications (advisory).

        Same anti-hallucination contract as pick_primary_source: the model
        answers with a 1-based INDEX into the caller's list; 0/out-of-range
        degrades to None (no dup)."""
        if not candidates:
            return None, LLMUsage(
                input_tokens=0, output_tokens=0, cost_usd=0.0,
                latency_ms=0, provider="anthropic", model=self.model)
        numbered = "\n".join(
            f"{i}. {c}" for i, c in enumerate(candidates, start=1))
        user = (
            f"Fresh story:\n{fresh}\n\n"
            f"Already published earlier:\n{numbered}\n\n"
            "Which numbered earlier story is the SAME news event? "
            "Answer 0 if none of them is."
        )
        tool = _tool(
            "record_dup",
            "Record which already-published story is the same news event.",
            MATCH_PUBLISHED_SCHEMA,
        )
        data, usage = self._tool_call(
            system=MATCH_PUBLISHED_SYSTEM, user=user, tool=tool, max_tokens=200
        )
        idx = data.get("index")
        if not isinstance(idx, int) or not (1 <= idx <= len(candidates)):
            return None, usage
        return idx, usage

    # --------------------------------------------------------------- private
    @_RETRY
    def _tool_call(
        self,
        *,
        system: str,
        user: str,
        tool: dict[str, Any],
        max_tokens: int,
    ) -> tuple[dict[str, Any], LLMUsage]:
        """Call Claude with prompt caching enabled on `system` + `tool`.

        The static prefix (system + tool schema) is identical across all
        articles in one batch run, so Anthropic prompt caching gives it a
        ~90% discount on every hit after the first.
        """
        ph = prompt_hash(system, user, json.dumps(tool))
        t0 = time.monotonic()
        system_blocks = [
            {
                "type": "text",
                "text": system,
                "cache_control": {"type": "ephemeral"},
            }
        ]
        tool_cached = {**tool, "cache_control": {"type": "ephemeral"}}
        resp = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system_blocks,
            tools=[tool_cached],
            tool_choice={"type": "tool", "name": tool["name"]},
            messages=[{"role": "user", "content": user}],
        )
        latency_ms = int((time.monotonic() - t0) * 1000)
        data: dict[str, Any] = {}
        for block in resp.content:
            if getattr(block, "type", None) == "tool_use" and getattr(block, "name", "") == tool["name"]:
                raw = getattr(block, "input", None) or {}
                if isinstance(raw, dict):
                    data = raw
                break
        if not data:
            raise ValidationError.from_exception_data(
                "AnthropicResponse",
                [{"type": "missing", "loc": ("tool_use",), "msg": "no tool_use block", "input": None}],
            )
        in_tok = getattr(resp.usage, "input_tokens", 0) or 0
        out_tok = getattr(resp.usage, "output_tokens", 0) or 0
        cache_create = getattr(resp.usage, "cache_creation_input_tokens", 0) or 0
        cache_read = getattr(resp.usage, "cache_read_input_tokens", 0) or 0
        cost = estimate_cost_with_cache(
            "anthropic", self.model, in_tok, out_tok, cache_create, cache_read
        )
        usage = LLMUsage(
            input_tokens=in_tok + cache_create + cache_read,  # total for reporting
            output_tokens=out_tok,
            cost_usd=cost,
            latency_ms=latency_ms,
            provider="anthropic",
            model=self.model,
        )
        log.info(
            "llm.call",
            provider="anthropic",
            model=self.model,
            prompt_hash=ph,
            input_tokens=in_tok,
            cache_creation=cache_create,
            cache_read=cache_read,
            output_tokens=out_tok,
            cost_usd=round(cost, 5),
            latency_ms=latency_ms,
        )
        return data, usage
