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
import re
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
    cached_row_json TEXT,
    lede_text       TEXT
);
CREATE INDEX IF NOT EXISTS idx_seen_portal ON seen_articles(portal);
CREATE INDEX IF NOT EXISTS idx_seen_first_seen ON seen_articles(first_seen_at);

CREATE TABLE IF NOT EXISTS run_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at    TEXT NOT NULL,
    portal        TEXT NOT NULL,
    summary_json  TEXT
);

-- Layer 3 (active press-release retrieval) cache.
-- Search results AND verification verdicts cached by query hash so
-- repeat queries hit $0. TTL is 24h by default (press releases are
-- immutable once published).
CREATE TABLE IF NOT EXISTS press_search_cache (
    query_hash      TEXT PRIMARY KEY,
    query_json      TEXT NOT NULL,
    results_json    TEXT NOT NULL,
    verified_url    TEXT,
    verified_conf   REAL,
    cached_at       TEXT NOT NULL,
    ttl_hours       INTEGER NOT NULL DEFAULT 24
);
CREATE INDEX IF NOT EXISTS idx_press_cache_cached_at
    ON press_search_cache(cached_at);
"""


# The push diverts a cluster whose reason carries this — see
# recent_pushed_titles, which must not count diverted rows as "published".
_DUP_HINT_RE = re.compile(r"возможно дуб", re.I)


def _blob_carries_dup_hint(blob: dict) -> bool:
    """True when the cached row carried a dup hint — the dedicated
    ``dup_hint`` field (clean-persist format, aug-05) or the legacy hint
    baked into llm_reason. Rows like this were DIVERTED by the push, not
    published: no anti-repeat store may treat them as evidence."""
    return bool((blob.get("dup_hint") or "").strip()) or bool(
        _DUP_HINT_RE.search(blob.get("llm_reason") or ""))


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
            if "lede_text" not in cols:
                # Lede text — unblocks cross-run dedup quality. Pre-fix
                # we only had `title` for history lookups, which made
                # short / generic titles falsely match. See PoC notes
                # in scripts/_embed_dedup_poc_run.py.
                c.execute(
                    "ALTER TABLE seen_articles ADD COLUMN lede_text TEXT"
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

    def recent_brand_models(
        self, portal: str, *, days: int = 30
    ) -> dict[str, tuple[str, str]]:
        """Plan P3-D (advisory): {normalised brand+model → (last_seen_at,
        canonical_url)} for everything classified on ``portal`` whose
        ``last_seen_at`` is within the last ``days``.

        Read-only. Used purely to annotate the editor-facing reason with
        a "we wrote about this model recently" hint — it never changes
        publish / section decisions, so a stale or missing value is
        harmless. Rows whose cached JSON lacks ``launch_brand_model``
        (pre-P3-D snapshots) are silently skipped.
        """
        from datetime import timedelta

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat()
        out: dict[str, tuple[str, str]] = {}
        with self._conn() as c:
            rows = c.execute(
                "SELECT canonical_url, last_seen_at, cached_row_json "
                "FROM seen_articles "
                "WHERE portal = ? AND last_seen_at IS NOT NULL "
                "AND last_seen_at >= ? "
                "AND cached_row_json IS NOT NULL AND cached_row_json != ''",
                (portal, cutoff),
            ).fetchall()
        # Canonicalise brand in keys so KGM ↔ SsangYong, BMW Alpina ↔
        # Alpina, Vw ↔ Volkswagen merge into one bucket. Without this,
        # v41 audit showed "kgm torres" vs "ssangyong torres" not
        # matching.
        from news_agent.core.brand_canonical import (
            aliases_for,
            canonicalize_brand,
        )

        for r in rows:
            try:
                blob = json.loads(r["cached_row_json"])
            except (ValueError, TypeError):
                continue
            bm = (blob.get("launch_brand_model") or "").strip()
            if not bm:
                continue
            # If we can identify the brand from the string, replace the
            # brand portion with its canonical form. Otherwise lowercase.
            canon = canonicalize_brand(bm)
            if canon:
                rest = bm.lower()
                for variant in aliases_for(canon):
                    if rest.startswith(variant):
                        rest = rest[len(variant):].lstrip(" -")
                        break
                bm_key = f"{canon.lower()} {rest}".strip()
            else:
                bm_key = bm.lower()
            prev = out.get(bm_key)
            # Keep the most recent sighting per model.
            if prev is None or r["last_seen_at"] > prev[0]:
                out[bm_key] = (r["last_seen_at"], r["canonical_url"])
        return out

    def recent_event_keys(
        self, portal: str, *, days: int = 30
    ) -> dict[str, tuple[str, str, str]]:
        """Hybrid dedup Stage 2a (advisory): {event_key → (last_seen_at,
        canonical_url, display)} from the LLM semantic event-signature
        persisted by Stage 1.

        event_key = "brand|model|event_type", built ONLY when model is
        non-empty and event_type is specific (not ""/"other") — same
        rule the in-run clusterer uses, so a bare ("geely","","launch")
        never glues unrelated launches. ``display`` = "brand model
        (event_type)" for the human-readable hint.

        Read-only, advisory: never changes publish/section. Pre-Stage-1
        cache rows lack event_* → silently skipped.
        """
        from datetime import timedelta

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat()
        out: dict[str, tuple[str, str, str]] = {}
        with self._conn() as c:
            rows = c.execute(
                "SELECT canonical_url, last_seen_at, cached_row_json "
                "FROM seen_articles "
                "WHERE portal = ? AND last_seen_at IS NOT NULL "
                "AND last_seen_at >= ? "
                "AND cached_row_json IS NOT NULL AND cached_row_json != ''",
                (portal, cutoff),
            ).fetchall()
        from news_agent.core.brand_canonical import canonicalize_brand

        for r in rows:
            try:
                blob = json.loads(r["cached_row_json"])
            except (ValueError, TypeError):
                continue
            eb_raw = (blob.get("event_brand") or "").strip()
            em = (blob.get("event_model") or "").strip().lower()
            et = (blob.get("event_type") or "").strip().lower()
            if not (eb_raw and em and et and et != "other"):
                continue
            # A row that ITSELF carried a dup hint is an echo, not a new
            # sighting of the event — and echoes must not rejuvenate the
            # key's clock. Before this filter (aug-05) every classified
            # echo refreshed last_seen_at, so a hot key (popular
            # model+type) stayed forever younger than the 7-day trust
            # window while anyone kept writing about it: a genuinely new
            # story got a fresh deterministic hint, was diverted, its own
            # row re-warmed the key — and the next outlet's version walked
            # into the same trap. The recent_pushed_titles half-fix (7.1)
            # never covered this store.
            if _blob_carries_dup_hint(blob):
                continue
            # Canonicalise the brand half of the key.
            eb = (canonicalize_brand(eb_raw) or eb_raw).lower()
            key = f"{eb}|{em}|{et}"
            display = f"{eb} {em} ({et})"
            prev = out.get(key)
            if prev is None or r["last_seen_at"] > prev[0]:
                out[key] = (r["last_seen_at"], r["canonical_url"], display)
        return out

    def recent_pushed_titles(self, portal: str, *, days: int = 30) -> set[str]:
        """Normalised titles of rows WE accepted («Точно новость») in the last
        ``days`` — the fuzzy anti-repeat base against OUR OWN recent pushes.

        Why (jul-20 dup-wave): a story we pushed on the 17th resurfaced from
        another outlet on the 19th and no layer could see it — exact URL
        differs (different outlet), exact title differs (rewording), and the
        17th's rows predate event-signature persistence, so the event layer
        was blind too. The editor's archive covers only what THEY published,
        with an ~18h export lag — our own feed history was checked nowhere.
        Read-only, advisory."""
        from datetime import timedelta
        from news_agent.core.primary_source import normalise_title

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat()
        out: set[str] = set()
        with self._conn() as c:
            rows = c.execute(
                "SELECT title, cached_row_json FROM seen_articles "
                "WHERE portal = ? AND last_seen_at IS NOT NULL "
                "AND last_seen_at >= ? "
                "AND cached_row_json IS NOT NULL AND cached_row_json != ''",
                (portal, cutoff),
            ).fetchall()
        for r in rows:
            try:
                blob = json.loads(r["cached_row_json"])
            except (ValueError, TypeError):
                continue
            if blob.get("verdict") != "Точно новость":
                continue
            # A row the push DIVERTED is not something the editor ever saw, and
            # counting it here makes the anti-repeat base self-reinforcing: a
            # story diverted once is recorded as «уже отправляли в фид», so the
            # next outlet's version of it matches, gets diverted, and is
            # recorded again — for ever.
            #
            # Measured aug-04 over 30 days: of 3048 rows accepted as «Точно
            # новость», only 1028 (34%) actually reached the feed, and 1528
            # (50% of the whole base) already carried a dup hint of their own.
            # Half of what this base "remembers publishing" is what it caused
            # to be hidden.
            #
            # The verdict alone cannot tell — diverting happens later, in the
            # push — but the hint the push diverts ON is in the cached row:
            # its own dup_hint field since the aug-05 clean-persist format,
            # baked into llm_reason on legacy blobs. Rows dropped for other
            # reasons (junk flag, never clustered) still leak in; the
            # complete fix is to record actual pushes, which needs a write
            # path the push stage does not have yet.
            if _blob_carries_dup_hint(blob):
                continue
            nt = normalise_title(r["title"] or "")
            if nt:
                out.add(nt)
        return out

    # ----------------------------------------------------- write
    def mark_many(
        self,
        entries: list[tuple[str, str, str, str | None, str, str]],
    ) -> None:
        """Legacy signature (no cached JSON, no lede). Kept for the
        production pipeline/run.py which hasn't been ported yet."""
        if not entries:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = [
            (h, url, title, pub, now, now, dom, portal, None, None)
            for (h, url, title, pub, dom, portal) in entries
        ]
        self._upsert(rows)

    def mark_many_with_cache(
        self,
        entries: list[tuple],
    ) -> None:
        """Upsert articles with optional cached classification + lede.

        Accepted tuple shapes (backward-compatible):
          7-tuple: (url_hash, canonical_url, title, published_at,
                    source_domain, portal, cached_row_json)
                   — pre-lede signature; lede defaults to None.
          8-tuple: (..., cached_row_json, lede_text)
                   — new signature with lede content for cross-run
                     dedup quality.
        UPSERT so ``last_seen_at`` and cache fields refresh on repeats.
        """
        if not entries:
            return
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for e in entries:
            if len(e) == 7:
                h, url, title, pub, dom, portal, cached = e
                lede = None
            elif len(e) == 8:
                h, url, title, pub, dom, portal, cached, lede = e
            else:
                raise ValueError(
                    f"mark_many_with_cache: expected 7-tuple or 8-tuple, "
                    f"got len={len(e)}"
                )
            rows.append(
                (h, url, title, pub, now, now, dom, portal, cached, lede)
            )
        self._upsert(rows)

    def _upsert(self, rows: list[tuple]) -> None:  # type: ignore[type-arg]
        with self._conn() as c:
            c.executemany(
                "INSERT INTO seen_articles ("
                "url_hash, canonical_url, title, published_at, "
                "first_seen_at, last_seen_at, source_domain, portal, "
                "cached_row_json, lede_text"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(url_hash) DO UPDATE SET "
                "  last_seen_at    = excluded.last_seen_at, "
                "  cached_row_json = COALESCE(excluded.cached_row_json, "
                "                              seen_articles.cached_row_json), "
                "  lede_text       = COALESCE(excluded.lede_text, "
                "                              seen_articles.lede_text)",
                rows,
            )

    def recent_for_brand(
        self,
        portal: str,
        brand_canonical: str,
        *,
        days: int = 14,
    ) -> list[dict]:
        """Return recent articles for a canonical brand, with lede text.

        Designed for cluster-level cross-run dedup: LLM-as-editor needs
        to see ALL recent articles of the same brand to spot "уже было"
        cases. We canonicalise both sides (stored event_brand vs
        ``brand_canonical``) so KGM/SsangYong/etc. collapse.

        Each result is a dict with: url, title, lede, ts, domain.
        Pre-lede rows (cached_row_json without lede column populated)
        still surface with lede="" — caller can fall back to title.
        """
        from datetime import timedelta
        from news_agent.core.brand_canonical import canonicalize_brand

        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat()
        canon_target = (canonicalize_brand(brand_canonical) or
                        brand_canonical).strip().lower()
        if not canon_target:
            return []

        out: list[dict] = []
        with self._conn() as c:
            rows = c.execute(
                "SELECT canonical_url, title, last_seen_at, source_domain, "
                "       cached_row_json, lede_text "
                "FROM seen_articles "
                "WHERE portal = ? AND last_seen_at IS NOT NULL "
                "AND last_seen_at >= ? "
                "ORDER BY last_seen_at DESC",
                (portal, cutoff),
            ).fetchall()
        for r in rows:
            # Try to recover brand from cached_row_json first (most
            # reliable), fall back to title-extraction.
            brand_str = ""
            try:
                if r["cached_row_json"]:
                    j = json.loads(r["cached_row_json"])
                    brand_str = (j.get("event_brand") or
                                  j.get("launch_brand_model") or "")
            except (ValueError, TypeError):
                pass
            if not brand_str:
                brand_str = r["title"] or ""
            canon = (canonicalize_brand(brand_str) or "").strip().lower()
            if canon != canon_target:
                continue
            out.append({
                "url": r["canonical_url"],
                "title": r["title"] or "",
                "lede": r["lede_text"] or "",
                "ts": r["last_seen_at"],
                "domain": r["source_domain"] or "",
            })
        return out

    def log_run(self, portal: str, summary_json: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as c:
            c.execute(
                "INSERT INTO run_log (started_at, portal, summary_json) VALUES (?, ?, ?)",
                (now, portal, summary_json),
            )
