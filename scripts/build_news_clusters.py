"""Read 'Точно новость' rows from a vN articles tab, cluster similar
stories (same event covered by multiple sources), and emit JSON for
manual review + sheet ingestion.

Cluster criteria:
  - rapidfuzz title similarity ≥ 0.72 between any two members
  - shares at least one car-brand mention OR shares a strong noun (model,
    location) — guards against unrelated stories with similar wording
  - publication / run-timestamp window of 36 h between earliest & latest

Within a cluster:
  - canonical = press-release host > whitelist domain > earliest published

Run:  python scripts/build_news_clusters.py "ТЕСТ статьи v18"
Output: data/clusters_<tab>.json
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from rapidfuzz import fuzz  # noqa: E402

from news_agent.core.config_loader import (  # noqa: E402
    load_brand_domains,
    load_primary_source_cues,
    load_whitelist_domains,
)

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column indices in 'ТЕСТ статьи vN' (matching write_articles())
COL_RUN = 0
COL_TITLE = 1
COL_LEDE = 2
COL_URL = 3
COL_SECTION = 4
COL_REGION = 5
COL_COUNTRY = 6
COL_PUBLISHED = 7
COL_IMAGE = 8
COL_PRIMARY_DOM = 9
COL_PRIMARY_URL = 10
COL_PRIMARY_CONF = 11
COL_NOTE = 12
COL_CONFIDENCE = 13
COL_VERDICT = 14

SIMILARITY_THRESHOLD = 72  # rapidfuzz token_set_ratio scale 0-100
TIME_WINDOW = timedelta(hours=36)


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(row: list[str], i: int) -> str:
    return row[i] if i < len(row) else ""


def _normalise(t: str) -> str:
    """Strip language tags / source suffixes / EN/RU prefixes."""
    if not t:
        return ""
    t = t.lower()
    # Drop language tags
    t = re.sub(r"\([a-zа-я]{2,4}\)\s*$", "", t)
    # Drop "EN: ... \n RU: ..." prefixes
    t = re.sub(r"^en:\s*", "", t)
    t = re.sub(r"\n\s*ru:\s*", " | ", t)
    # Drop trailing source-name (— CarBuzz, | MotorTrend, …)
    t = re.sub(r"\s*[—\-|]\s*[a-zа-я0-9 \.&]+$", "", t)
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    return t


# Brands list — used for the "share a brand" cluster guard.
_BRANDS_LOWER: list[str] = []


def _brand_overlap(a: str, b: str) -> bool:
    """Return True if both titles mention at least one of the same brands."""
    a_brands = {br for br in _BRANDS_LOWER if br in a}
    if not a_brands:
        return False
    b_brands = {br for br in _BRANDS_LOWER if br in b}
    return bool(a_brands & b_brands)


def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    s = s.strip()
    # Try ISO and ISO-with-Z
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except ValueError:
        pass
    # Fallback: try "YYYY-MM-DDTHH:MM"
    try:
        return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:  # noqa: BLE001
        return ""


def _cluster_priority(
    article: dict,
    *,
    press_release_hosts: set[str],
    whitelist: set[str],
) -> tuple[int, datetime]:
    """Lower number = higher priority for becoming canonical of cluster."""
    dom = article["domain"]
    pub = article["pub_dt"] or datetime.max.replace(tzinfo=timezone.utc)
    if dom in press_release_hosts or any(dom.endswith("." + h) for h in press_release_hosts):
        return (0, pub)
    if dom in whitelist:
        return (1, pub)
    return (2, pub)


def cluster_articles(
    articles: list[dict],
    *,
    threshold: int = SIMILARITY_THRESHOLD,
) -> list[list[dict]]:
    """Return groups of articles. Each group covers the same story.

    Algorithm: greedy union-find by title similarity, gated on shared
    brand AND publication window.
    """
    n = len(articles)
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[rj] = ri

    norms = [a["normalised"] for a in articles]
    for i in range(n):
        ti = norms[i]
        if not ti:
            continue
        ai_pub = articles[i]["pub_dt"]
        for j in range(i + 1, n):
            tj = norms[j]
            if not tj:
                continue
            sim = fuzz.token_set_ratio(ti, tj)
            if sim < threshold:
                continue
            # Brand guard
            if not _brand_overlap(ti, tj):
                continue
            # Time window guard (only if both have timestamps)
            aj_pub = articles[j]["pub_dt"]
            if ai_pub and aj_pub and abs((ai_pub - aj_pub).total_seconds()) > TIME_WINDOW.total_seconds():
                continue
            union(i, j)

    groups: dict[int, list[dict]] = {}
    for idx, art in enumerate(articles):
        groups.setdefault(find(idx), []).append(art)
    # Sort each group by priority (canonical first)
    return list(groups.values())


def main() -> int:
    tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ статьи v18"

    brands = load_brand_domains()
    cues = load_primary_source_cues()
    whitelist = load_whitelist_domains()

    global _BRANDS_LOWER
    _BRANDS_LOWER = []
    for b in brands:
        _BRANDS_LOWER.append(b.brand.lower())
        for a in getattr(b, "aliases", []) or []:
            _BRANDS_LOWER.append(a.lower())
    # Drop super-short aliases that produce false positives.
    _BRANDS_LOWER = [b for b in _BRANDS_LOWER if len(b) >= 4]
    press_release_hosts = {h.lower() for h in cues.press_release_hosts}

    svc = _svc()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A2:Q"
    ).execute()
    rows = resp.get("values", []) or []
    print(f"Loaded {len(rows)} rows from '{tab}'.")

    articles: list[dict] = []
    for sheet_idx, r in enumerate(rows, start=2):
        verdict = _get(r, COL_VERDICT)
        if verdict != "Точно новость":
            continue
        url = _get(r, COL_URL)
        title = _get(r, COL_TITLE)
        lede = _get(r, COL_LEDE)
        section = _get(r, COL_SECTION)
        region = _get(r, COL_REGION)
        country = _get(r, COL_COUNTRY)
        published = _get(r, COL_PUBLISHED)
        image_url = _get(r, COL_IMAGE)
        primary_dom = _get(r, COL_PRIMARY_DOM)
        primary_url = _get(r, COL_PRIMARY_URL)
        primary_conf = _get(r, COL_PRIMARY_CONF)
        articles.append({
            "sheet_row": sheet_idx,
            "url": url,
            "domain": _domain(url),
            "title": title,
            "normalised": _normalise(title),
            "lede": lede,
            "section": section,
            "region": region,
            "country": country,
            "published": published,
            "pub_dt": _parse_dt(published),
            "image_url": image_url,
            "primary_dom": primary_dom,
            "primary_url": primary_url,
            "primary_conf": primary_conf,
        })
    print(f"'Точно новость' rows: {len(articles)}")

    groups = cluster_articles(articles)
    print(f"Clusters found: {len(groups)}")

    # Pack output
    out_clusters: list[dict] = []
    singletons = 0
    for grp in groups:
        # Sort by priority — first is canonical
        grp_sorted = sorted(
            grp,
            key=lambda a: _cluster_priority(
                a,
                press_release_hosts=press_release_hosts,
                whitelist=whitelist,
            ),
        )
        canonical = grp_sorted[0]
        if len(grp) == 1:
            singletons += 1
        cluster = {
            "size": len(grp),
            "canonical_title": canonical["title"],
            "canonical_url": canonical["url"],
            "canonical_domain": canonical["domain"],
            "canonical_lede": canonical["lede"],
            "section": canonical["section"],
            "region": canonical["region"],
            "country": canonical["country"],
            "published": canonical["published"],
            "image_url": canonical["image_url"],
            "primary_domain": canonical["primary_dom"],
            "primary_url": canonical["primary_url"],
            "primary_conf": canonical["primary_conf"],
            "members": [
                {
                    "url": a["url"],
                    "domain": a["domain"],
                    "title": a["title"],
                    "sheet_row": a["sheet_row"],
                }
                for a in grp_sorted
            ],
        }
        out_clusters.append(cluster)

    # Sort clusters: largest first (more sources = more important story),
    # then by published desc as tie-breaker.
    out_clusters.sort(
        key=lambda c: (-c["size"], -(_parse_dt(c["published"]) or datetime.min.replace(tzinfo=timezone.utc)).timestamp())
    )

    out_path = ROOT / "data" / f"clusters_{tab.replace(' ', '_')}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(out_clusters, f, ensure_ascii=False, indent=2)

    print()
    print(f"Total clusters: {len(out_clusters)}")
    print(f"  - singletons (1 source): {singletons}")
    print(f"  - multi-source clusters: {len(out_clusters) - singletons}")
    print(f"  - largest cluster size: {max(c['size'] for c in out_clusters)}")
    print(f"Exported to: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
