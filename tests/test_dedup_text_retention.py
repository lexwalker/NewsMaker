"""How much article text we keep, and who is allowed to shrink it.

The dedup work needs facts — prices, ranges, dates, model codes — and those
sit further into an article than the 600 characters we used to store. A sample
of 18 freshly-parsed articles put the median body at 3208 characters, so 600
kept 19% of the story and 61-72% of rows yielded no facts to match on at all.

Two properties are pinned here. The stored slice must be independent of the
slice we send to the model, because one is a disk cost and the other is a
per-call token cost. And a writer holding a short slice must never overwrite a
long one — the recovery path rebuilds rows from the sheet, whose lede cell is
capped at 300 characters, so "newest wins" quietly destroyed the column.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))


@pytest.fixture(scope="module")
def bft():
    import batch_fetch_test
    return batch_fetch_test


# ------------------------------------------------- the two slices are separate

def test_the_stored_slice_is_larger_than_the_model_slice(bft) -> None:
    """If these ever converge, someone has either started paying LLM rates for
    dedup text or gone back to deduping on a fragment."""
    assert bft.DEDUP_TEXT_CHARS == 3000
    src = (ROOT / "scripts" / "batch_fetch_test.py").read_text(encoding="utf-8")
    assert "row.body_excerpt = article.body[:1000]" in src, \
        "the model's slice must stay at 1000 — it is a cost knob"
    assert src.count("row.body_full = article.body[:DEDUP_TEXT_CHARS]") == 2, \
        "both fetch paths must fill the stored slice"


def test_the_model_never_receives_the_long_slice(bft) -> None:
    """body_full exists to be stored. If it reaches a judging call, every
    article's token cost roughly triples."""
    src = (ROOT / "scripts" / "batch_fetch_test.py").read_text(encoding="utf-8")
    for line in src.splitlines():
        if "body_full" in line and "=" not in line.split("body_full")[0]:
            assert not any(k in line for k in ("editorial_review", "is_automotive",
                                               "translate", "items=")), line


def test_the_row_falls_back_to_the_short_slice(bft) -> None:
    """A row built by a path that never set body_full still stores something —
    the write must degrade, not drop the text on the floor."""
    src = (ROOT / "scripts" / "batch_fetch_test.py").read_text(encoding="utf-8")
    assert 'getattr(row, "body_full", "") or row.body_excerpt' in src,         "read it tolerantly: _cache_entry_for also sees rows rebuilt elsewhere"
    assert "[:DEDUP_TEXT_CHARS]" in src


# ------------------------------------------------ a short write cannot shrink it

def _write(db, url_hash, lede):
    from news_agent.adapters.storage import DedupStore
    st = DedupStore(db)
    st.mark_many_with_cache(
        [(url_hash, f"https://e/{url_hash}", "T", "2026-08-21", "e", "portal", None, lede)],
    )


def _read(db, url_hash):
    con = sqlite3.connect(str(db))
    try:
        r = con.execute("SELECT lede_text FROM seen_articles WHERE url_hash=?",
                        (url_hash,)).fetchone()
        return r[0] if r else None
    finally:
        con.close()


def test_a_shorter_write_does_not_overwrite_a_longer_one(tmp_path) -> None:
    """The recovery-path regression, in one test: full text first, sheet-sized
    text second, full text must survive."""
    db = tmp_path / "c.sqlite"
    _write(db, "h1", "ц" * 3000)
    _write(db, "h1", "ц" * 300)
    got = _read(db, "h1")
    assert len(got) == 3000, f"short write shrank the stored text to {len(got)}"


def test_a_longer_write_does_replace_a_shorter_one(tmp_path) -> None:
    """The other direction must still work: a run that finally parses the full
    body upgrades a row stored from a fragment."""
    db = tmp_path / "c.sqlite"
    _write(db, "h2", "ц" * 300)
    _write(db, "h2", "ц" * 3000)
    assert len(_read(db, "h2")) == 3000


def test_a_null_write_keeps_what_is_there(tmp_path) -> None:
    """Unchanged from the COALESCE it replaces: a writer with no text at all
    must not erase the column."""
    db = tmp_path / "c.sqlite"
    _write(db, "h3", "ц" * 1200)
    _write(db, "h3", None)
    got = _read(db, "h3")
    assert got is not None and len(got) == 1200
