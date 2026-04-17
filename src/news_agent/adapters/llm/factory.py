"""Resolve a concrete LLMClient from settings."""

from __future__ import annotations

from news_agent.adapters.llm.anthropic_client import AnthropicLLMClient
from news_agent.adapters.llm.base import LLMClient
from news_agent.adapters.llm.openai_client import OpenAILLMClient
from news_agent.settings import Settings


def make_llm_client(s: Settings, provider_override: str | None = None) -> LLMClient:
    provider = (provider_override or s.llm_provider).lower()
    if provider == "anthropic":
        if not s.anthropic_api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not set in .env")
        return AnthropicLLMClient(s.anthropic_api_key, s.anthropic_model)
    if provider == "openai":
        if not s.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY not set in .env")
        return OpenAILLMClient(s.openai_api_key, s.openai_model)
    raise ValueError(f"Unknown LLM provider: {provider}")
