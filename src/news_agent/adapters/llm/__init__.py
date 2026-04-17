"""LLM adapter layer: provider-agnostic interface + factory."""

from news_agent.adapters.llm.base import LLMClient
from news_agent.adapters.llm.factory import make_llm_client

__all__ = ["LLMClient", "make_llm_client"]
