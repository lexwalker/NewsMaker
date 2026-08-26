"""Refusing to run when the pipeline is busy — and when the probe is blind.

On aug-25 two 8-worker backfills and a multi-gigabyte install landed on top of
a running full fetch and the machine needed a power cycle. The guard the runs
use for each other never covered ad-hoc work.

The subtle half is the probe itself: `wmic` is gone from recent Windows 11 and
returns an empty list rather than failing, so a check built on it answers
"idle" on a fully loaded machine. That is the one wrong answer that causes the
damage, so an unreadable process list must read as BUSY.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.core.pipeline_busy import busy_stages, require_idle  # noqa: E402

import pytest  # noqa: E402


def test_a_running_fetch_is_busy() -> None:
    lines = [r"C:\Python\python.exe C:\NewsMaker\scripts\batch_fetch_test.py"]
    assert busy_stages(lines) == ["batch_fetch_test"]


def test_the_push_stages_count_too(  ) -> None:
    """Clustering and push follow the fetch inside one chain; the machine is
    still the pipeline's until the chain ends."""
    assert busy_stages([r"python scripts\build_news_clusters.py --use-llm-editor"]) \
        == ["build_news_"]
    assert busy_stages([r"python scripts\build_news_sheet.py"]) == ["build_news_"]


def test_recovery_counts(  ) -> None:
    assert busy_stages([r"python scripts\retry_failed_llm.py"]) == ["retry_failed_llm"]


def test_an_idle_machine_is_idle() -> None:
    assert busy_stages([r"python -m pytest", r"python scripts\weekly_kpi.py"]) == []


def test_no_python_at_all_is_idle() -> None:
    assert busy_stages([]) == []


def test_an_unreadable_process_list_reads_as_BUSY(monkeypatch) -> None:
    """The wmic trap. A blind probe must not grant permission — refusing to
    run is recoverable, freezing the machine mid-fetch is not."""
    from news_agent.core import pipeline_busy as pb
    monkeypatch.setattr(pb, "_command_lines", lambda *a, **k: None)
    assert pb.busy_stages() == ["<не удалось проверить>"]


def test_a_blind_probe_blocks_require_idle(monkeypatch) -> None:
    """The consequence that matters: an unreadable list must STOP the job,
    not merely be reported."""
    from news_agent.core import pipeline_busy as pb
    monkeypatch.setattr(pb, "_command_lines", lambda *a, **k: None)
    with pytest.raises(SystemExit):
        pb.require_idle("тяжёлая задача")


def test_several_stages_are_all_reported(  ) -> None:
    lines = [r"python scripts\batch_fetch_test.py", r"python scripts\build_news_sheet.py"]
    assert busy_stages(lines) == ["batch_fetch_test", "build_news_"]


def test_a_stage_is_not_reported_twice(  ) -> None:
    lines = [r"python scripts\build_news_clusters.py", r"python scripts\build_news_sheet.py"]
    assert busy_stages(lines) == ["build_news_"]


def test_require_idle_raises_when_busy(monkeypatch) -> None:
    from news_agent.core import pipeline_busy as pb
    monkeypatch.setattr(pb, "busy_stages", lambda: ["batch_fetch_test"])
    with pytest.raises(SystemExit) as e:
        require_idle("добор тел")
    assert "batch_fetch_test" in str(e.value) and "добор тел" in str(e.value)


def test_require_idle_is_silent_when_free(monkeypatch) -> None:
    from news_agent.core import pipeline_busy as pb
    monkeypatch.setattr(pb, "busy_stages", lambda: [])
    require_idle("что угодно")
