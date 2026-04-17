"""Running-cost tracker with a hard cap."""

from __future__ import annotations

from dataclasses import dataclass

from news_agent.core.models import LLMUsage


class BudgetExceeded(RuntimeError):
    pass


@dataclass
class BudgetTracker:
    cap_usd: float
    spent_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    calls: int = 0

    def record(self, usage: LLMUsage) -> None:
        self.spent_usd += usage.cost_usd
        self.input_tokens += usage.input_tokens
        self.output_tokens += usage.output_tokens
        self.calls += 1
        if self.spent_usd > self.cap_usd:
            raise BudgetExceeded(
                f"LLM cost cap exceeded: ${self.spent_usd:.4f} > ${self.cap_usd:.4f}"
            )

    def snapshot(self) -> dict[str, float | int]:
        return {
            "calls": self.calls,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "spent_usd": round(self.spent_usd, 5),
            "cap_usd": self.cap_usd,
        }
