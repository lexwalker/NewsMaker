"""_health_check stuck-rows branch — exercised for REAL, not monkeypatched.

The aug-05 NameError (`len(candidates)` — a variable local to _run_llm_pass)
lived on the exact path the jul-29 fix introduced: llm_ran=True, no abort,
stragglers present. The only other test touching _health_check stubs the whole
function out (test_fetch_resume_integration), so the branch had zero coverage.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import batch_fetch_test as bf  # noqa: E402


def _stuck_row(i: int) -> bf.ArticleRow:
    # A fresh candidate the LLM pass never classified: candidate verdict,
    # not from cache, llm_relevance left empty.
    return bf.ArticleRow(
        source_idx=i, source_url=f"https://s{i}.example", article_idx=0,
        article_url=f"https://s{i}.example/a", verdict="Точно новость",
    )


def _call(rows: list, n_candidates: int) -> list[str]:
    return bf._health_check(
        [], rows,
        llm_ran=True, llm_aborted="",
        llm_candidates=n_candidates,
        dedup_enabled=False, prev_state={},
    )


def test_lone_straggler_warns_but_does_not_alarm():
    # jul-29 scenario verbatim: 1 stuck row out of 344 candidates must NOT
    # degrade the run (and must not crash computing the share).
    assert _call([_stuck_row(1)], 344) == []


def test_stuck_block_over_both_floors_alarms():
    # 5 stuck of 20 candidates: above STUCK_ROWS_ABORT and STUCK_SHARE_ABORT.
    rows = [_stuck_row(i) for i in range(5)]
    alarms = _call(rows, 20)
    assert any("unclassified after a 'completed' LLM pass" in a for a in alarms)


def test_row_floor_alone_is_not_enough():
    # 4 stuck of 4000: over the row floor, under the 2% share → both limits
    # must be exceeded, so the chain continues.
    rows = [_stuck_row(i) for i in range(4)]
    assert _call(rows, 4000) == []


def test_aborted_pass_still_alarms_regardless_of_share():
    # An aborted pass is an alarm before any share arithmetic.
    alarms = bf._health_check(
        [], [_stuck_row(1)],
        llm_ran=True, llm_aborted="budget exceeded: $5",
        llm_candidates=344,
        dedup_enabled=False, prev_state={},
    )
    assert any("LLM pass aborted" in a for a in alarms)
