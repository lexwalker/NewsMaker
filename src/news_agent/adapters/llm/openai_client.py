"""OpenAI implementation of LLMClient."""

from __future__ import annotations

import json
import time
from typing import Any

from openai import APIStatusError, OpenAI, RateLimitError
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
from news_agent.adapters.llm.pricing import estimate_cost_with_cache as estimate_cost
from news_agent.core.models import (
    Classification,
    FewShotExample,
    LLMUsage,
    RelevanceCheck,
    SectionDefinition,
    TitlePair,
)
from news_agent.logging_setup import get_logger

log = get_logger("llm.openai")

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


class OpenAILLMClient:
    provider_name = "openai"

    def __init__(self, api_key: str, model: str) -> None:
        self._client = OpenAI(api_key=api_key)
        self.model = model

    def is_automotive(self, title: str, body_excerpt: str) -> tuple[RelevanceCheck, LLMUsage]:
        data, usage = self._json_call(
            system=RELEVANCE_SYSTEM,
            user=f"Title: {title}\n\nExcerpt:\n{body_excerpt[:500]}",
            schema_name="relevance",
            schema=RELEVANCE_SCHEMA,
            max_tokens=200,
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
        user = build_classify_user_prompt(
            title=title,
            body=body,
            sections=sections,
            few_shots=few_shots,
            portal_country=portal_country,
        )
        data, usage = self._json_call(
            system=CLASSIFY_SYSTEM,
            user=user,
            schema_name="classification",
            schema=CLASSIFY_SCHEMA,
            max_tokens=500,
        )
        return Classification.model_validate(data), usage

    def translate_title(
        self, *, title: str, source_language_hint: str | None
    ) -> tuple[TitlePair, LLMUsage]:
        hint = f" (source language hint: {source_language_hint})" if source_language_hint else ""
        data, usage = self._json_call(
            system=TRANSLATE_SYSTEM,
            user=f"Headline{hint}:\n{title}",
            schema_name="title_pair",
            schema=TRANSLATE_SCHEMA,
            max_tokens=300,
        )
        return TitlePair.model_validate(data), usage

    # --------------------------------------------------------------- private
    @_RETRY
    def _json_call(
        self,
        *,
        system: str,
        user: str,
        schema_name: str,
        schema: dict[str, Any],
        max_tokens: int,
    ) -> tuple[dict[str, Any], LLMUsage]:
        ph = prompt_hash(system, user, json.dumps(schema))
        t0 = time.monotonic()
        resp = self._client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "schema": schema,
                    "strict": True,
                },
            },
            max_tokens=max_tokens,
            temperature=0.2,
        )
        latency_ms = int((time.monotonic() - t0) * 1000)
        content = resp.choices[0].message.content or "{}"
        try:
            data = json.loads(content)
        except json.JSONDecodeError:
            data = {}
        in_tok = resp.usage.prompt_tokens if resp.usage else 0
        out_tok = resp.usage.completion_tokens if resp.usage else 0
        usage = LLMUsage(
            input_tokens=in_tok,
            output_tokens=out_tok,
            cost_usd=estimate_cost("openai", self.model, in_tok, out_tok),
            latency_ms=latency_ms,
            provider="openai",
            model=self.model,
        )
        log.info(
            "llm.call",
            provider="openai",
            model=self.model,
            prompt_hash=ph,
            input_tokens=in_tok,
            output_tokens=out_tok,
            cost_usd=round(usage.cost_usd, 5),
            latency_ms=latency_ms,
        )
        return data, usage
