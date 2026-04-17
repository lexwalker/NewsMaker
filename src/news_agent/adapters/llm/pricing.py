"""Per-million-token pricing used for the cost cap.

These are defaults — tune to match your contract. Both providers publish
latest pricing on their website. We deliberately keep this in code (not env)
so a misconfig cannot silently zero-out cost tracking.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Price:
    input_per_mtok: float
    output_per_mtok: float


# Conservative ceilings — prefer over-estimating.
ANTHROPIC_PRICES: dict[str, Price] = {
    "claude-sonnet-4-5": Price(3.00, 15.00),
    "claude-sonnet-4-6": Price(3.00, 15.00),
    "claude-opus-4-5": Price(15.00, 75.00),
    "claude-opus-4-7": Price(15.00, 75.00),
    "claude-haiku-4-5": Price(1.00, 5.00),
}

OPENAI_PRICES: dict[str, Price] = {
    "gpt-4o": Price(2.50, 10.00),
    "gpt-4o-mini": Price(0.15, 0.60),
    "gpt-4.1": Price(2.00, 8.00),
    "gpt-4.1-mini": Price(0.40, 1.60),
}


def estimate_cost(provider: str, model: str, input_tokens: int, output_tokens: int) -> float:
    table = ANTHROPIC_PRICES if provider == "anthropic" else OPENAI_PRICES
    p = table.get(model)
    if p is None:
        # Unknown model → assume the most expensive known price so the cap is conservative.
        p = max(table.values(), key=lambda x: x.input_per_mtok + x.output_per_mtok) if table else Price(5.0, 15.0)
    return (input_tokens / 1_000_000) * p.input_per_mtok + (
        output_tokens / 1_000_000
    ) * p.output_per_mtok
