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
    CLASSIFY_SYSTEM,
    RELEVANCE_SCHEMA,
    RELEVANCE_SYSTEM,
    TRANSLATE_SCHEMA,
    TRANSLATE_SYSTEM,
    build_classify_user_prompt,
    prompt_hash,
)
from news_agent.adapters.llm.pricing import estimate_cost
from news_agent.core.models import (
    Classification,
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
        user = build_classify_user_prompt(
            title=title,
            body=body,
            sections=sections,
            few_shots=few_shots,
            portal_country=portal_country,
        )
        data, usage = self._tool_call(
            system=CLASSIFY_SYSTEM, user=user, tool=tool, max_tokens=500
        )
        return Classification.model_validate(data), usage

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
        ph = prompt_hash(system, user, json.dumps(tool))
        t0 = time.monotonic()
        resp = self._client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system,
            tools=[tool],
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
        usage = LLMUsage(
            input_tokens=in_tok,
            output_tokens=out_tok,
            cost_usd=estimate_cost("anthropic", self.model, in_tok, out_tok),
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
            output_tokens=out_tok,
            cost_usd=round(usage.cost_usd, 5),
            latency_ms=latency_ms,
        )
        return data, usage
