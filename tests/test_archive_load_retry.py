"""The archive read must survive a transient Sheets failure.

On aug-18 a single 503 from Google emptied the published archive. The floor
alarm fired exactly as designed — an empty archive means already-published
stories would reach the editor — and the 08:00 run aborted having published
nothing. The guard was right; the read underneath it was the problem.

Every other Sheets call in this codebase retries with backoff. This one, whose
failure stops the whole chain, did not. These tests pin both halves: a blip is
retried, and a genuinely empty archive is still reported empty rather than
being papered over.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


class _Values:
    def __init__(self, outer):
        self.outer = outer

    def get(self, **kw):
        return self

    def execute(self):
        self.outer.calls += 1
        if self.outer.calls <= self.outer.fail_times:
            raise RuntimeError(f"503 Service Unavailable (call {self.outer.calls})")
        return {"values": self.outer.payload}


class FakeSvc:
    """Fails `fail_times` times, then returns `payload`."""

    def __init__(self, fail_times: int, payload: list):
        self.fail_times, self.payload, self.calls = fail_times, payload, 0

    def spreadsheets(self):
        return self

    def values(self):
        return _Values(self)


@pytest.fixture(scope="module")
def pa(monkeypatch_session=None):
    import published_archive
    return published_archive


def _rows():
    """One header + one archive row: section, _, _, EN title, RU title, date …
    URL sits at index 11 (column L)."""
    header = ["Раздел"] * 18
    row = [""] * 18
    row[0], row[3], row[4] = "Other news", "BMW X5 unveiled in Munich", "BMW X5 представлен"
    row[5] = "2026-08-17T10:00:00+00:00"
    row[11] = "https://example.com/bmw-x5"
    return [header, row]


def test_a_transient_failure_is_retried_not_swallowed(pa, monkeypatch) -> None:
    """Two 503s then success: the archive loads, and the caller never sees the
    empty-set path that would abort the run."""
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda *_: None)
    svc = FakeSvc(fail_times=2, payload=_rows())
    urls, recent, all_titles = pa.load_published_index(svc, "sheet-id")
    assert svc.calls == 3, "should have retried twice before succeeding"
    assert urls, "archive must be populated after a successful retry"


def test_it_gives_up_after_five_attempts(pa, monkeypatch) -> None:
    """Not infinite: a genuinely dead API still returns empty sets so the
    floor alarm can stop the run rather than the process hanging."""
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda *_: None)
    svc = FakeSvc(fail_times=99, payload=_rows())
    urls, recent, all_titles = pa.load_published_index(svc, "sheet-id")
    assert svc.calls == 5
    assert (urls, recent, all_titles) == (set(), set(), set())


def test_a_genuinely_empty_archive_is_not_retried_away(pa, monkeypatch) -> None:
    """An empty READ is a real answer, not a failure: one call, empty result,
    and the floors downstream decide. Retrying it would hide a wiped tab."""
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda *_: None)
    svc = FakeSvc(fail_times=0, payload=[])
    urls, recent, all_titles = pa.load_published_index(svc, "sheet-id")
    assert svc.calls == 1
    assert (urls, recent, all_titles) == (set(), set(), set())


def test_first_call_success_costs_no_extra_calls(pa, monkeypatch) -> None:
    import time as _t
    monkeypatch.setattr(_t, "sleep", lambda *_: None)
    svc = FakeSvc(fail_times=0, payload=_rows())
    pa.load_published_index(svc, "sheet-id")
    assert svc.calls == 1
