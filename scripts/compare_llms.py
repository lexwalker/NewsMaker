"""Run the same fixture set through both LLM providers.

Outputs `reports/llm_comparison.md` with per-item classifications, a summary
agreement rate, per-provider cost, and latency.
"""

from __future__ import annotations

import json
import statistics
from pathlib import Path
from typing import Any

from news_agent.adapters.llm import make_llm_client
from news_agent.adapters.llm.base import LLMClient
from news_agent.core.config_loader import load_sections
from news_agent.logging_setup import configure_logging
from news_agent.settings import get_settings

FIXTURES = Path(__file__).parent / "fixtures" / "eval_articles.json"
REPORT_DIR = Path("reports")
REPORT_FILE = REPORT_DIR / "llm_comparison.md"


def run_one(client: LLMClient, article: dict[str, Any], sections: list[Any]) -> dict[str, Any]:
    cls, usage = client.classify_section(
        title=article["title"],
        body=article["body"],
        sections=sections,
        few_shots=[],
        portal_country="Russia",
    )
    return {
        "section": cls.section,
        "region": cls.region,
        "confidence": cls.confidence,
        "tokens_in": usage.input_tokens,
        "tokens_out": usage.output_tokens,
        "cost_usd": usage.cost_usd,
        "latency_ms": usage.latency_ms,
    }


def main() -> None:
    s = get_settings()
    configure_logging(s.log_dir, s.log_level)
    if not s.anthropic_api_key or not s.openai_api_key:
        raise SystemExit("Both ANTHROPIC_API_KEY and OPENAI_API_KEY required for comparison")

    sections = load_sections()
    items = json.loads(FIXTURES.read_text(encoding="utf-8"))

    anthropic = make_llm_client(s, provider_override="anthropic")
    openai = make_llm_client(s, provider_override="openai")

    rows: list[dict[str, Any]] = []
    agree = 0
    for art in items:
        a = run_one(anthropic, art, sections)
        o = run_one(openai, art, sections)
        rows.append({"article": art, "anthropic": a, "openai": o})
        if a["section"] == o["section"]:
            agree += 1

    REPORT_DIR.mkdir(exist_ok=True)

    def _avg(key: str, which: str) -> float:
        return statistics.mean(r[which][key] for r in rows) if rows else 0.0

    lines = [
        "# LLM comparison report",
        "",
        f"- Items: **{len(rows)}**",
        f"- Section-agreement rate: **{agree / len(rows) * 100:.1f}%** ({agree}/{len(rows)})",
        "",
        "## Per-provider totals",
        "",
        "| Provider | Total cost (USD) | Avg tokens in | Avg tokens out | Avg latency (ms) |",
        "|---|---:|---:|---:|---:|",
        f"| anthropic ({anthropic.model}) | "
        f"{sum(r['anthropic']['cost_usd'] for r in rows):.5f} | "
        f"{_avg('tokens_in', 'anthropic'):.0f} | "
        f"{_avg('tokens_out', 'anthropic'):.0f} | "
        f"{_avg('latency_ms', 'anthropic'):.0f} |",
        f"| openai ({openai.model}) | "
        f"{sum(r['openai']['cost_usd'] for r in rows):.5f} | "
        f"{_avg('tokens_in', 'openai'):.0f} | "
        f"{_avg('tokens_out', 'openai'):.0f} | "
        f"{_avg('latency_ms', 'openai'):.0f} |",
        "",
        "## Per-article results",
        "",
        "| ID | Expected | Anthropic | OpenAI | Agree |",
        "|---|---|---|---|:---:|",
    ]
    for r in rows:
        a = r["article"]
        got_a = r["anthropic"]
        got_o = r["openai"]
        mark = "✅" if got_a["section"] == got_o["section"] else "❌"
        lines.append(
            f"| {a['id']} | {a.get('expected_section') or '—'} | "
            f"{got_a['section']}/{got_a['region']} | "
            f"{got_o['section']}/{got_o['region']} | {mark} |"
        )

    REPORT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {REPORT_FILE}")


if __name__ == "__main__":
    main()
