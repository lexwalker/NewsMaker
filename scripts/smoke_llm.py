"""Quick sanity-check that the configured LLM provider actually works.

Runs one minimal classification against a hard-coded sample article,
prints the structured output plus token usage and cost.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402


SAMPLE_TITLE = "BYD reaches 16 millionth NEV production milestone with new Denza D9"
SAMPLE_BODY = (
    "BYD Company has produced its 16 millionth new-energy vehicle (NEV), marking a "
    "new milestone for the Chinese automaker. The latest unit to roll off the line was "
    "a Denza D9 MPV, produced at BYD's Shenzhen plant. The company said it plans to "
    "keep expanding its global footprint, with new markets in Europe and Latin America "
    "targeted for 2026 launches."
)


def main() -> int:
    s = get_settings()
    if not s.anthropic_api_key and s.llm_provider == "anthropic":
        print("ANTHROPIC_API_KEY is not set in .env", file=sys.stderr)
        return 2
    if not s.openai_api_key and s.llm_provider == "openai":
        print("OPENAI_API_KEY is not set in .env", file=sys.stderr)
        return 2

    client = make_llm_client(s)
    sections = load_sections()
    print(f"Provider: {client.provider_name}  model: {client.model}")
    print(f"Sections loaded: {len(sections)}")
    print(f"\nTitle: {SAMPLE_TITLE}\n")

    # 1) relevance
    rel, u1 = client.is_automotive(SAMPLE_TITLE, SAMPLE_BODY)
    print(f"1) relevance: is_automotive_or_economy={rel.is_automotive_or_economy}")
    print(f"   reason: {rel.reason}")
    print(f"   usage: {u1.input_tokens} in / {u1.output_tokens} out / "
          f"${u1.cost_usd:.5f} / {u1.latency_ms} ms")

    # 2) classify
    cls, u2 = client.classify_section(
        title=SAMPLE_TITLE,
        body=SAMPLE_BODY,
        sections=sections,
        few_shots=[],
        portal_country="Russia",
    )
    print(f"\n2) classification:")
    print(f"   section = {cls.section!r}")
    print(f"   region  = {cls.region}")
    print(f"   confidence = {cls.confidence}")
    print(f"   reasoning: {cls.reasoning}")
    print(f"   usage: {u2.input_tokens} in / {u2.output_tokens} out / "
          f"${u2.cost_usd:.5f} / {u2.latency_ms} ms")

    # 3) translate
    tp, u3 = client.translate_title(title=SAMPLE_TITLE, source_language_hint="en")
    print(f"\n3) translation:")
    print(f"   english: {tp.english}")
    print(f"   russian: {tp.russian}")
    print(f"   source language: {tp.source_language}")
    print(f"   usage: {u3.input_tokens} in / {u3.output_tokens} out / "
          f"${u3.cost_usd:.5f} / {u3.latency_ms} ms")

    total = u1.cost_usd + u2.cost_usd + u3.cost_usd
    print(f"\nTotal for one article (3 LLM calls): ${total:.5f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
