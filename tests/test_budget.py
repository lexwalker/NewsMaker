import pytest

from news_agent.core.budget import BudgetExceeded, BudgetTracker
from news_agent.core.models import LLMUsage


def _usage(cost: float) -> LLMUsage:
    return LLMUsage(input_tokens=1, output_tokens=1, cost_usd=cost, provider="test", model="x")


def test_accumulates_without_cap_hit() -> None:
    t = BudgetTracker(cap_usd=1.0)
    t.record(_usage(0.3))
    t.record(_usage(0.4))
    assert t.spent_usd == pytest.approx(0.7)
    assert t.calls == 2


def test_raises_on_cap_breach() -> None:
    t = BudgetTracker(cap_usd=0.5)
    t.record(_usage(0.3))
    with pytest.raises(BudgetExceeded):
        t.record(_usage(0.3))
