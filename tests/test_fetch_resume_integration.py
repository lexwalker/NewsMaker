"""End-to-end crash/resume: drive the REAL batch main() with external I/O
mocked, crash it mid-fetch, then re-run and prove it resumes without
re-fetching completed sources.

This exercises the actual wiring — fingerprint compute, begin/append/load/clear,
the skip-by-index loop, ArticleRow round-trip, tab reuse, window adoption — not
just the checkpoint module in isolation.
"""

from __future__ import annotations

import contextlib
import io
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import batch_fetch_test as bf  # noqa: E402

URLS = [f"https://src{i}.example/feed" for i in range(1, 6)]  # 5 sources
ROWS_PER_SOURCE = 2
CRASH_AT = 3  # power outage strikes while fetching source 3


class _Outage(BaseException):
    """Stand-in for a hard process kill (power outage). A BaseException, so the
    fetch loop's ``except Exception`` does NOT catch it (a real outage isn't a
    catchable error) — it unwinds main() exactly like the process dying. Not
    KeyboardInterrupt/SystemExit, which pytest special-cases and which would
    tear down the capture machinery."""


def _main_quiet(argv):
    """Run batch main() with its verbose stdout/stderr sunk to a throwaway
    buffer. main() prints heavily; on Windows, writing that volume to pytest's
    capture buffer trips a known teardown ValueError ("I/O operation on closed
    file"). Sinking it keeps these tests green under default capture too."""
    with contextlib.redirect_stdout(io.StringIO()), \
            contextlib.redirect_stderr(io.StringIO()):
        return bf.main(argv)


class _FakeSvc:
    """Barely-used: allocate/read/write are all patched. Present so any stray
    svc.spreadsheets() chain doesn't explode."""
    def spreadsheets(self):
        return self
    def __getattr__(self, _n):
        def _any(*a, **k):
            return self
        return _any
    def execute(self):
        return {}


class _DummyClientCM:
    def __enter__(self):
        return object()
    def __exit__(self, *a):
        return False


def _seed_state(path: Path):
    # A prior successful run so compute_window yields an incremental window with
    # a real previous_run_at anchor (what the fingerprint keys on).
    path.write_text(json.dumps({
        "last_run_at": "2026-07-14T20:44:13+00:00",
        "last_run_window_start": "2026-07-14T07:53:01+00:00",
        "last_run_status": "ok",
        "articles_tab": "ТЕСТ статьи v46",
    }), encoding="utf-8")


@pytest.fixture
def wired(tmp_path, monkeypatch):
    state_path = tmp_path / "state.json"
    runs_log = tmp_path / "runs.log"
    _seed_state(state_path)

    calls: list[tuple[int, int]] = []      # (run_id, source_idx) process_source saw
    writes: list[tuple[str, int]] = []     # (tab, n_rows) write_articles saw
    alloc_count = {"n": 0}
    run_id = {"n": 0}

    def fake_process(client, u, i, article_rows):
        calls.append((run_id["n"], i))
        if run_id["n"] == 1 and i == CRASH_AT:
            raise _Outage("simulated power outage")
        for k in range(ROWS_PER_SOURCE):
            article_rows.append(bf.ArticleRow(
                source_idx=i, source_url=u, article_idx=k,
                article_url=f"{u}/a{k}", title=f"S{i}A{k}",
                verdict="Возможно новость", body_excerpt="x" * 40,
                primary_candidates=[], launch_stages=[],
            ))
        return bf.SourceResult(url=u, detected_type="html", http_status=200,
                               news_like=ROWS_PER_SOURCE,
                               articles_attempted=ROWS_PER_SOURCE)

    def fake_alloc(svc):
        alloc_count["n"] += 1
        return ("ТЕСТ v99", "ТЕСТ статьи v99")

    def fake_write_articles(svc, run_ts, rows, tab):
        writes.append((tab, len(rows)))

    monkeypatch.setattr(bf, "sheets_client", lambda: _FakeSvc())
    monkeypatch.setattr(bf, "read_active_sources", lambda svc, limit: list(URLS))
    monkeypatch.setattr(bf, "allocate_new_tabs", fake_alloc)
    monkeypatch.setattr(bf, "process_source", fake_process)
    monkeypatch.setattr(bf, "make_client", lambda: _DummyClientCM())
    monkeypatch.setattr(bf, "write_report", lambda *a, **k: None)
    monkeypatch.setattr(bf, "write_articles", fake_write_articles)
    monkeypatch.setattr(bf, "_health_check", lambda *a, **k: [])
    monkeypatch.setattr(bf, "TELEGRAM_SEED_URLS", [])
    monkeypatch.setattr(bf, "ENABLE_NHTSA_RECALLS", False)
    monkeypatch.setattr(bf, "SQLITE_PATH", tmp_path / "cache.sqlite")

    argv = ["--no-llm", "--no-playwright", "--no-published-dedup",
            "--state-path", str(state_path), "--runs-log", str(runs_log)]
    return {"argv": argv, "calls": calls, "writes": writes,
            "alloc_count": alloc_count, "run_id": run_id,
            "ckpt": bf.fetch_checkpoint.checkpoint_path(state_path),
            "state_path": state_path}


def test_crash_then_resume_skips_completed_sources(wired):
    ckpt = wired["ckpt"]

    # --- Run 1: crashes while fetching source 3 (power outage) ---------------
    wired["run_id"]["n"] = 1
    with pytest.raises(_Outage):
        _main_quiet(wired["argv"])

    # Checkpoint survives with sources 1 and 2 done (3 crashed before append).
    assert ckpt.exists(), "checkpoint must survive the crash"
    run1_sources = [i for (rid, i) in wired["calls"] if rid == 1]
    assert run1_sources == [1, 2, 3]  # 3 was attempted then crashed

    # --- Run 2: same params, state not advanced → must RESUME ---------------
    wired["run_id"]["n"] = 2
    wired["alloc_count"]["n"] = 0
    rc = _main_quiet(wired["argv"])
    assert rc == 0

    run2_sources = [i for (rid, i) in wired["calls"] if rid == 2]
    # Sources 1 and 2 were restored from checkpoint → NOT re-fetched.
    assert 1 not in run2_sources and 2 not in run2_sources
    # Only the remaining sources are fetched on resume.
    assert run2_sources == [3, 4, 5]

    # No new tab allocated on resume — the crashed run's tab is reused.
    assert wired["alloc_count"]["n"] == 0

    # Final write covers ALL 5 sources' rows (2 restored + 3 re-fetched sources).
    assert wired["writes"], "run 2 must write articles"
    tab, n_rows = wired["writes"][-1]
    assert tab == "ТЕСТ статьи v99"          # same tab the crashed run allocated
    assert n_rows == 5 * ROWS_PER_SOURCE     # 10 rows total, none lost, none dup

    # Checkpoint cleared after the successful Sheets write.
    assert not ckpt.exists(), "checkpoint must be cleared once rows are durable"


def test_gap_from_swallowed_append_is_refetched_not_dropped(wired, monkeypatch):
    """Red-team regression: append_source is best-effort. If source 2's append
    fails (swallowed) but 1 and 3 persist, then a crash at 4, the resume must
    re-fetch ONLY the gap (2) + the tail — NOT skip source 2 by max(done_idx)
    and silently lose its rows (that was the confirmed silent-data-loss bug)."""
    ckpt = wired["ckpt"]
    real_append = bf.fetch_checkpoint.append_source

    def flaky_append(path, *, src_idx, source_result, rows):
        if wired["run_id"]["n"] == 1 and src_idx == 2:
            raise OSError("simulated AV/indexer lock on append")  # swallowed by batch
        return real_append(path, src_idx=src_idx, source_result=source_result, rows=rows)

    monkeypatch.setattr(bf.fetch_checkpoint, "append_source", flaky_append)

    # Run 1: source 2's append fails (gap), crash at source 4.
    global CRASH_AT
    _orig_crash = CRASH_AT
    CRASH_AT = 4
    try:
        wired["run_id"]["n"] = 1
        with pytest.raises(_Outage):
            _main_quiet(wired["argv"])
    finally:
        CRASH_AT = _orig_crash

    # Only sources 1 and 3 persisted (2's append was swallowed).
    fp = bf.fetch_checkpoint.fingerprint(
        classifier_version=bf.CLASSIFIER_VERSION,
        previous_run_at=datetime.fromisoformat("2026-07-14T20:44:13+00:00"),
        urls=list(URLS), max_lookback_hours=48, overlap_minutes=120,
        max_articles=bf.MAX_ARTICLES, hot=False)
    loaded = bf.fetch_checkpoint.load(ckpt, fp)
    assert loaded is not None and loaded.done_idx == {1, 3}

    # Run 2: must re-fetch the gap (2) and the tail (4,5); keep 1,3 restored.
    wired["run_id"]["n"] = 2
    wired["alloc_count"]["n"] = 0
    rc = _main_quiet(wired["argv"])
    assert rc == 0
    run2 = sorted(i for (rid, i) in wired["calls"] if rid == 2)
    assert run2 == [2, 4, 5], f"gap (2) + tail must be re-fetched, got {run2}"
    assert 1 not in run2 and 3 not in run2  # persisted sources not re-fetched
    tab, n_rows = wired["writes"][-1]
    assert n_rows == 5 * ROWS_PER_SOURCE  # all 5 sources present, none lost/dup
    assert not ckpt.exists()


def test_incompatible_checkpoint_falls_back_to_full_fetch(wired):
    """Red-team regression: if a checkpoint's rows can't be reconstructed into
    ArticleRow (e.g. a field was added/removed since it was written, with no
    classifier_version bump so the fingerprint still matches), the run must NOT
    crash — it discards the checkpoint and does a clean full fetch."""
    ckpt = wired["ckpt"]

    # Run 1: crash → valid checkpoint (sources 1, 2).
    wired["run_id"]["n"] = 1
    with pytest.raises(_Outage):
        _main_quiet(wired["argv"])
    assert ckpt.exists()

    # Corrupt a restored row with a field ArticleRow(**d) can't accept.
    lines = ckpt.read_text(encoding="utf-8").splitlines()
    for j, ln in enumerate(lines):
        rec = json.loads(ln)
        if rec.get("kind") == "src" and rec.get("rows"):
            rec["rows"][0]["field_that_no_longer_exists"] = 1
            lines[j] = json.dumps(rec, ensure_ascii=False)
            break
    ckpt.write_text("\n".join(lines) + "\n", encoding="utf-8")

    # Run 2: reconstruction fails → discard + full fresh fetch (no crash).
    wired["run_id"]["n"] = 2
    wired["alloc_count"]["n"] = 0
    rc = _main_quiet(wired["argv"])
    assert rc == 0
    fetched = sorted(i for (rid, i) in wired["calls"] if rid == 2)
    assert fetched == [1, 2, 3, 4, 5]          # every source fetched fresh
    assert wired["alloc_count"]["n"] == 1      # a new tab was allocated
    assert not ckpt.exists()                    # bad checkpoint cleared


def test_no_checkpoint_means_full_fresh_fetch(wired):
    # Sanity: with no prior checkpoint, a normal run fetches every source once
    # and allocates a fresh tab.
    wired["run_id"]["n"] = 2  # never crash
    rc = _main_quiet(wired["argv"])
    assert rc == 0
    fetched = sorted(i for (rid, i) in wired["calls"])
    assert fetched == [1, 2, 3, 4, 5]
    assert wired["alloc_count"]["n"] == 1
    assert not wired["ckpt"].exists()
