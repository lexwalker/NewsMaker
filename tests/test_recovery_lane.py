"""Which lane's window a recovery is allowed to advance.

Two chains run against the same spreadsheet and the same cache, each with its
own window: the full chain in state.json, the hot chain in state_hot.json.
Advancing the wrong one is not cosmetic — the window is what decides how far
back the NEXT run looks, so moving the full lane's window because a hot run
finished would step it over hours nobody fetched, and those stories are gone
with no record that they ever existed.

That hazard is exactly why the hot lane had no recovery at all: the abort hint
was suppressed for `--hot` because consuming it would have advanced state.json.
On aug-06 a hot run died on an empty balance at candidate 95 of 282 and had to
be finished by hand. The hint now names its own state file, so the suppression
could go.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def retry():
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "scripts"))
    import retry_failed_llm
    return retry_failed_llm


def test_a_hot_hint_advances_the_hot_lane(retry) -> None:
    p = retry._hint_state_path({"state_file": "state_hot.json"})
    assert p.name == "state_hot.json"
    assert p.parent.name == "data"


def test_a_full_hint_advances_the_full_lane(retry) -> None:
    assert retry._hint_state_path({"state_file": "state.json"}).name == "state.json"


def test_an_old_hint_without_a_lane_is_full_lane(retry) -> None:
    """Hints written before aug-06 carry no name, and they are full-lane by
    construction — the hot lane had no recovery to write one."""
    assert retry._hint_state_path({}).name == "state.json"
    assert retry._hint_state_path(None).name == "state.json"


@pytest.mark.parametrize("evil", [
    "../../../etc/state.json",
    "C:\\Windows\\state.json",
    "sub/dir/state.json",
])
def test_a_hint_cannot_point_outside_data(retry, evil) -> None:
    """The hint is a file this process wrote, but it is still input: only a
    bare filename is honoured, so a malformed or tampered one cannot make the
    recovery write somewhere unexpected."""
    p = retry._hint_state_path({"state_file": evil})
    assert p.parent.name == "data"
    assert p.name == "state.json"


def test_the_batch_writes_the_lane_into_its_hint() -> None:
    """The producer half of the contract: without state_file in the hint, the
    consumer would silently fall back to the full lane."""
    root = Path(__file__).resolve().parents[1]
    src = (root / "scripts" / "batch_fetch_test.py").read_text(encoding="utf-8")
    # The file also DELETES this path at run start; we want the write.
    guard = src.find("if (alarms and RUN_WINDOW is not None")
    assert guard > 0, "не нашёл условие записи подсказки"
    block = src[guard:guard + 1800]
    assert "llm_abort_recovery.json" in block
    assert '"state_file": args.state_path.name' in block
    # …and the hot lane is no longer excluded from writing one.
    assert "not args.hot" not in block
