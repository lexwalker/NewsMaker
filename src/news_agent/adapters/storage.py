"""SQLite dedup cache + run log. Strictly a cache — Sheets is canonical."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen_articles (
    url_hash       TEXT PRIMARY KEY,
    canonical_url  TEXT NOT NULL,
    title          TEXT NOT NULL,
    published_at   TEXT,
    first_seen_at  TEXT NOT NULL,
    source_domain  TEXT NOT NULL,
    portal         TEXT NOT NULL
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

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

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

    def mark_many(
        self,
        entries: list[tuple[str, str, str, str | None, str, str]],
    ) -> None:
        """entries: (url_hash, canonical_url, title, published_at_iso, source_domain, portal)."""
        if not entries:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = [(h, url, title, pub, now, dom, portal) for (h, url, title, pub, dom, portal) in entries]
        with self._conn() as c:
            c.executemany(
                "INSERT OR IGNORE INTO seen_articles "
                "(url_hash, canonical_url, title, published_at, first_seen_at, source_domain, portal) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

    def log_run(self, portal: str, summary_json: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as c:
            c.execute(
                "INSERT INTO run_log (started_at, portal, summary_json) VALUES (?, ?, ?)",
                (now, portal, summary_json),
            )
