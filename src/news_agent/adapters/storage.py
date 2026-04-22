"""SQLite dedup cache + classification cache + run log.

Now stores a JSON-serialised snapshot of each article's final classification
(verdict, section, region, translated titles, primary source) so that a
second batch-run encountering the same URL can restore those fields
instead of calling the LLM again.

Sheets is still the system-of-record for the published rows. This DB is
for fast recognition of seen URLs and their cheap "reconstitution".
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_articles (
    url_hash        TEXT PRIMARY KEY,
    canonical_url   TEXT NOT NULL,
    title           TEXT NOT NULL,
    published_at    TEXT,
    first_seen_at   TEXT NOT NULL,
    last_seen_at    TEXT,
    source_domain   TEXT NOT NULL,
    portal          TEXT NOT NULL,
    cached_row_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_seen_portal ON seen_articles(portal);
CREATE INDEX IF NOT EXISTS idx_seen_first_seen ON seen_articles(first_seen_at);

CREATE TABLE IF NOT EXISTS run_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    TEXT NOT NULL,
    portal        TEXT NOT NULL,
    summary_json  TEXT
);
"""


class DedupStore:
    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        with self._conn() as c:
            c.executescript(SCHEMA)
            # Idempotent migration for databases created before the cache
            # columns existed.
            cols = {
                r["name"]
                for r in c.execute("PRAGMA table_info(seen_articles)").fetchall()
            }
            if "last_seen_at" not in cols:
                c.execute("ALTER TABLE seen_articles ADD COLUMN last_seen_at TEXT")
            if "cached_row_json" not in cols:
                c.execute(
                    "ALTER TABLE seen_articles ADD COLUMN cached_row_json TEXT"
                )

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # ----------------------------------------------------- lookup
    def has(self, url_hash: str) -> bool:
        with self._conn() as c:
            row = c.execute(
                "SELECT 1 FROM seen_articles WHERE url_hash = ?", (url_hash,)
            ).fetchone()
        return row is not None

    def has_any(self, url_hashes: list[str]) -> set[str]:
        if not url_hashes:
            return set()
        with self._conn() as c:
            qmarks = ",".join(["?"] * len(url_hashes))
            rows = c.execute(
                f"SELECT url_hash FROM seen_articles WHERE url_hash IN ({qmarks})",  # noqa: S608
                url_hashes,
            ).fetchall()
        return {r["url_hash"] for r in rows}

    def load_cache(self, portal: str) -> dict[str, dict[str, Any]]:
        """Return {url_hash → cached row fields} for everything seen on
        ``portal`` that has cached_row_json set. Entries without a cached
        blob (pre-migration rows) are skipped — they'll be re-classified
        on next contact, which is the correct one-off degradation."""
        out: dict[str, dict[str, Any]] = {}
        with self._conn() as c:
            rows = c.execute(
                "SELECT url_hash, cached_row_json FROM seen_articles "
                "WHERE portal = ? AND cached_row_json IS NOT NULL AND cached_row_json != ''",
                (portal,),
            ).fetchall()
        for r in rows:
            try:
                out[r["url_hash"]] = json.loads(r["cached_row_json"])
            except (ValueError, TypeError):
                continue
        return out

    # ----------------------------------------------------- write
    def mark_many(
        self,
        entries: list[tuple[str, str, str, str | None, str, str]],
    ) -> None:
        """Legacy signature (no cached JSON). Kept for the production
        pipeline/run.py which hasn't been ported yet."""
        if not entries:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            (h, url, title, pub, now, now, dom, portal, None)
            for (h, url, title, pub, dom, portal) in entries
        ]
        self._upsert(rows)

    def mark_many_with_cache(
        self,
        entries: list[tuple[str, str, str, str | None, str, str, str]],
    ) -> None:
        """entries: (url_hash, canonical_url, title, published_at, source_domain,
        portal, cached_row_json). UPSERT so ``last_seen_at`` and the cache
        JSON get refreshed on repeat runs."""
        if not entries:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            (h, url, title, pub, now, now, dom, portal, cached)
            for (h, url, title, pub, dom, portal, cached) in entries
        ]
        self._upsert(rows)

    def _upsert(self, rows: list[tuple]) -> None:  # type: ignore[type-arg]
        with self._conn() as c:
            c.executemany(
                "INSERT INTO seen_articles ("
                "url_hash, canonical_url, title, published_at, "
                "first_seen_at, last_seen_at, source_domain, portal, cached_row_json"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(url_hash) DO UPDATE SET "
                "  last_seen_at    = excluded.last_seen_at, "
                "  cached_row_json = COALESCE(excluded.cached_row_json, seen_articles.cached_row_json)",
                rows,
            )

    def log_run(self, portal: str, summary_json: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as c:
            c.execute(
                "INSERT INTO run_log (started_at, portal, summary_json) VALUES (?, ?, ?)",
                (now, portal, summary_json),
            )
