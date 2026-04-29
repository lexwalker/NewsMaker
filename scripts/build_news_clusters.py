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
from news_agent.core.fuzzy_match import normalise_for_match  # noqa: E402

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

SIMILARITY_THRESHOLD = 65  # rapidfuzz token_set_ratio (0-100). Lowered
                            # from 72 after v22: Avtotor JETOUR triple
                            # ("started preparing"/"began preparations"/
                            # "started preparation") had token_set_ratio
                            # in the 67-71 range. Translit-folded titles
                            # carry less filler so a slightly lower bar
                            # stays safe when the brand-overlap guard is
                            # also enforced.
TIME_WINDOW = timedelta(hours=36)
PROPER_NOUN_OVERLAP_MIN = 2   # require ≥2 shared proper-noun-like tokens
                              # before clustering (model code, brand,
                              # location, etc.) — prevents same-brand
                              # general stories from collapsing.


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(row: list[str], i: int) -> str:
    return row[i] if i < len(row) else ""


def _normalise(t: str) -> str:
    """Aggressive normaliser for fuzzy-match (translit + diacritics + numbers).

    Delegates to :func:`news_agent.core.fuzzy_match.normalise_for_match`
    so the same logic is reused across cluster builder and primary-source
    detection — both need the AvtoVAZ ↔ AvtoVAZ / Промтех ↔ Promtekh
    folding for cross-spelling dedup.
    """
    return normalise_for_match(t)


# Brands list — used for the "share a brand" cluster guard.
_BRANDS_LOWER: list[str] = []


def _brand_overlap(a: str, b: str) -> bool:
    """Return True if both titles mention at least one of the same brands."""
    a_brands = {br for br in _BRANDS_LOWER if br in a}
    if not a_brands:
        return False
    b_brands = {br for br in _BRANDS_LOWER if br in b}
    return bool(a_brands & b_brands)


# Stop-words to ignore when extracting "proper noun" overlap (which is
# more about specific entities than common verbs/articles).
_STOPWORDS = frozenset({
    # english
    "the", "a", "an", "and", "or", "but", "of", "in", "on", "at", "to",
    "for", "with", "from", "by", "as", "is", "are", "was", "were", "be",
    "been", "being", "has", "have", "had", "do", "does", "did", "will",
    "would", "could", "should", "may", "might", "can", "shall",
    "new", "first", "second", "next", "all", "more", "most", "than",
    "this", "that", "these", "those", "its", "their", "his", "her",
    "started", "began", "starts", "begins",
    "preparation", "preparations", "preparing", "produce", "production",
    "launch", "launches", "launched", "launching",
    "announce", "announces", "announced", "announcement",
    "introduce", "introduces", "introduced",
    "make", "makes", "made", "making",
    # russian
    "и", "в", "на", "с", "по", "к", "у", "о", "об", "за", "из", "до",
    "от", "не", "под", "над", "при", "без", "для", "про", "через",
    "это", "этот", "эта", "эти", "тот", "та", "те", "его", "её", "их",
    "новый", "новая", "новое", "новые",
    "запуск", "запустит", "запустила", "запустили",
    "анонсировал", "анонсировала", "представил", "представила", "представили",
    "объявил", "объявила", "объявили",
    "приступил", "приступила", "приступили",
    "начал", "начала", "начали",
    "подготовка", "подготовку", "подготовке", "подготовил", "подготовила",
    "производство", "производства", "производству", "производстве",
    "production", "produce", "produced",
})


def _proper_noun_tokens(t: str) -> set[str]:
    """Extract candidate proper-noun tokens — alphanumeric, len ≥ 2,
    not a stopword. Includes alphanumeric model codes (T1, X3, GV90).
    """
    if not t:
        return set()
    out: set[str] = set()
    for tok in re.split(r"[\s\-_/]+", t.lower()):
        # Strip surrounding punctuation
        tok = re.sub(r"^\W+|\W+$", "", tok)
        if not tok or len(tok) < 2:
            continue
        if tok in _STOPWORDS:
            continue
        # Pure number (year, count) — skip (year matched on both sides
        # would too easily collapse different stories of the same year)
        if tok.isdigit() and len(tok) == 4:
            continue
        out.add(tok)
    return out


def _proper_noun_overlap(a: str, b: str) -> int:
    """Count of shared proper-noun-like tokens between two normalised titles."""
    return len(_proper_noun_tokens(a) & _proper_noun_tokens(b))


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
    primary_urls = [a.get("primary_url", "") for a in articles]
    for i in range(n):
        ti = norms[i]
        ai_pub = articles[i]["pub_dt"]
        pi = primary_urls[i].strip().lower() if primary_urls[i] else ""
        for j in range(i + 1, n):
            tj = norms[j]
            pj = primary_urls[j].strip().lower() if primary_urls[j] else ""

            # Cross-language safety net: same primary URL = same story.
            # Catches cases where RU and EN headlines have <5 token overlap
            # (different verbs/nouns) but cite the same press release.
            primary_match = bool(pi and pj and pi == pj)

            if not ti or not tj:
                if primary_match:
                    union(i, j)
                continue

            sim = fuzz.token_set_ratio(ti, tj)
            if sim < threshold and not primary_match:
                continue
            # Brand guard — skip when primary_match already proved equality
            if not primary_match and not _brand_overlap(ti, tj):
                continue
            # Proper-noun overlap guard — at least 2 shared specific tokens
            # (brand + model OR brand + location, etc). Without this, we
            # collapse "Toyota launched X" + "Toyota launched Y" because
            # they share fuzz score but differ in model.
            if (
                not primary_match
                and _proper_noun_overlap(ti, tj) < PROPER_NOUN_OVERLAP_MIN
            ):
                continue
            # Time window guard (only if both have timestamps)
            aj_pub = articles[j]["pub_dt"]
            if (
                ai_pub
                and aj_pub
                and abs((ai_pub - aj_pub).total_seconds()) > TIME_WINDOW.total_seconds()
            ):
                # Primary-match overrides time window — same press release
                # often gets re-posted days later by aggregators.
                if not primary_match:
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

    # Sort clusters strictly by published timestamp descending — newest first.
    # Multi-source size is no longer a tie-breaker; the editor wants a
    # chronological feed. Undated clusters (where the source page didn't
    # expose a publish time) sink to the end.
    out_clusters.sort(
        key=lambda c: -(_parse_dt(c["published"])
                        or datetime.min.replace(tzinfo=timezone.utc)).timestamp()
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
