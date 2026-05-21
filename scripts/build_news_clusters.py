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

# Force UTF-8 console only when run as a script. Doing this at import
# time corrupts pytest's captured stdout (wraps & later closes its
# buffer → "I/O operation on closed file"). Guarding keeps the module
# import-safe for unit tests.
if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", line_buffering=True
    )
    sys.stderr = io.TextIOWrapper(
        sys.stderr.buffer, encoding="utf-8", line_buffering=True
    )

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
# Phase-1 launch lifecycle (added in apr-2026)
COL_LAUNCH_STAGE = 28  # AC
COL_LAUNCH_BRAND_MODEL = 29  # AD
# LLM editorial review reason (added may-2026)
COL_LLM_REASON = 30  # AE
# Hybrid dedup Stage 1: semantic event-signature (added may-2026)
COL_EVENT_BRAND = 31  # AF
COL_EVENT_MODEL = 32  # AG
COL_EVENT_TYPE = 33   # AH

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
    """Cluster-eligibility check based on brand mentions.

    Decision matrix:
      • Both titles share at least one brand → eligible (same story).
      • Both titles have NO brand → eligible — defer to fuzzy + proper-noun
        guards. Catches market-stat articles like "Sales of global brand
        cars increased 2,4x" vs "Global brand sales increased 2.4-fold"
        (same news, no specific brand named).
      • One has a brand, the other doesn't → NOT eligible (likely
        different stories — one talks about a specific brand, the other
        about a market in general).
    """
    a_brands = {br for br in _BRANDS_LOWER if br in a}
    b_brands = {br for br in _BRANDS_LOWER if br in b}
    if not a_brands and not b_brands:
        # No brand mentions on either side — defer to fuzzy + proper-noun.
        return True
    if not a_brands or not b_brands:
        # Asymmetric: one has brand, one doesn't → different stories.
        return False
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


# Generic slug words that are NEVER a model name. Without this list the
# extractor produced "geely news" for EVERY geely-motors.com/about-geely/
# news/<slug> URL and "xpeng to" for every "xpeng-to-<verb>" — collapsing
# unrelated press releases. A token in this set can't be a model token
# and (when it's the FIRST token after the brand) makes that brand
# position yield nothing; the scan then continues to the next brand
# occurrence in the slug (handles ".../news/geely-introduces-new-geely-
# coolray-in-russia" → "geely coolray").
_SLUG_STOPWORDS = frozenset({
    # path / structural
    "news", "about", "press", "media", "article", "articles", "story",
    "blog", "post", "posts", "category", "tag", "tags", "index", "page",
    "en", "ru", "int", "global", "company", "corporate", "newsroom",
    # articles / prepositions / conjunctions
    "the", "a", "an", "to", "of", "for", "in", "on", "at", "by", "and",
    "or", "with", "from", "as", "its", "it", "is", "are", "was", "were",
    "this", "that", "new", "all", "more", "into", "out", "up", "via",
    # common slug verbs / nouns (not model-distinctive)
    "will", "has", "have", "had", "gets", "get", "got", "make", "makes",
    "report", "reports", "reported", "reporting", "results",
    "reveal", "reveals", "revealed", "revealing",
    "launch", "launches", "launched", "launching",
    "unveil", "unveils", "unveiled", "unveiling",
    "introduce", "introduces", "introduced", "introducing",
    "present", "presents", "presented", "presenting",
    "announce", "announces", "announced", "announcement",
    "start", "starts", "started", "starting", "begins", "began",
    "sales", "sale", "selling", "sell", "sells", "sold",
    "revenue", "profit", "loss", "earnings", "financial",
    "recall", "recalls", "recalled", "campaign",
    "update", "updates", "updated", "updating", "current",
    "record", "took", "take", "takes", "leading", "position", "rating",
    "plant", "plants", "factory", "production", "produce", "produced",
    "deliveries", "delivery", "shipments", "market", "version",
    "model", "models", "lineup", "range", "first", "next", "second",
    "show", "showed", "shows", "showing", "debut", "debuts", "premiere",
    "concept", "prototype", "spotted", "teased", "review", "test",
    "russia", "russian", "china", "chinese", "europe", "european",
})


def _url_model_key(url: str) -> str:
    """Derive a "<brand> <model-token(s)>" key from a URL slug.

    Editor dup case (may-2026): the same Jaguar Type 01 spy-shot story
    had headlines sharing only "Jaguar" ("electric sedan prototype" vs
    "Type 01") so title-fuzz never merged them — but BOTH URLs carry the
    slug ``jaguar-type-01``:
      kolesa.ru/news/jaguar-type-01-pokazalsia-na-novyx-snimkax
      motor1.com/news/796122/jaguar-type-01-new-images
    The slug is the most reliable cross-source signal here.

    Returns ``<brand> <model-token(s)>`` (≤2 model tokens) and ONLY when
    at least one model token carries a digit — real model identifiers are
    digit-bearing codes (``type 01``, ``ix3``, ``ev9``, ``s800``, ``gs4``).
    This high-precision rule:
      • keeps the editor's actual dup case (jaguar-type-01 → "jaguar
        type 01", caught across kolesa/motor1/media.jaguar)
      • eliminates ALL the false collisions the audit surfaced —
        "geely news" (every geely-motors press URL), "xpeng to"
        (every xpeng-to-<verb>), and RU-translit verb slugs like
        "jaguar nazval imia" — which have no digit token → "".
    Generic words are still pre-filtered via _SLUG_STOPWORDS and the scan
    walks EVERY brand occurrence so a structural "about-geely" segment
    doesn't shadow a real "<brand>-<model>" later in the path. A model
    without a digit code (Coolray, Emgrand, Tucson) yields "" and the
    caller falls back to title-fuzz — prior behaviour, never a regression.
    """
    if not url:
        return ""
    try:
        path = urlparse(url).path.lower()
    except Exception:  # noqa: BLE001
        return ""
    toks = [t for t in re.split(r"[^a-z0-9]+", path) if t]
    for bi, tok in enumerate(toks):
        if tok not in _BRANDS_LOWER:
            continue
        # Collect non-stopword tokens UNTIL AND INCLUDING the first
        # digit-bearing one, then stop — the digit token is the model
        # code, anything after it ("long", "wheelbase", "review") is
        # slug noise that would split the same model across sources.
        model_toks: list[str] = []
        has_digit = False
        for nxt in toks[bi + 1 : bi + 4]:
            if nxt in _BRANDS_LOWER or nxt in _SLUG_STOPWORDS:
                break
            if not (2 <= len(nxt) <= 12 or any(c.isdigit() for c in nxt)):
                break
            model_toks.append(nxt)
            if any(c.isdigit() for c in nxt):
                has_digit = True
                break  # model code reached — stop
        if model_toks and has_digit:
            return f"{tok} {' '.join(model_toks)}"
        # otherwise keep scanning for another brand occurrence
    return ""


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
    """Lower number = higher priority for becoming canonical of cluster.

    Uses :mod:`news_agent.core.source_priority` 7-tier ladder, calibrated
    against the may-2026 editor audit (35 «постили пресс / нужен англ»
    complaints out of 197). Tier 0 (OEM-for-brand) when the article's
    brand matches the domain's OEM registry; falls through 1 (regulator)
    → 2 (generic OEM press) → 3 (industry EN) → 4 (whitelist/unknown)
    → 5 (industry RU) → 6 (RU aggregator) → 7 (mirror/social).

    Legacy ``press_release_hosts`` and ``whitelist`` are folded in as
    additional signals at tier 0 / 4 respectively — keeping the
    backward-compatible behaviour where editor-added hosts in those
    sets stay strong.
    """
    from news_agent.core.brand_canonical import canonicalize_brand
    from news_agent.core.source_priority import domain_tier

    dom = article["domain"]
    pub = article["pub_dt"] or datetime.max.replace(tzinfo=timezone.utc)

    # Legacy press_release_hosts override stays as tier 0
    if dom in press_release_hosts or any(
        dom.endswith("." + h) for h in press_release_hosts
    ):
        return (0, pub)

    # Brand hint from launch_brand_model (best) or title fallback
    brand_hint = (article.get("launch_brand_model") or "").strip()
    if not brand_hint:
        brand_hint = (article.get("title") or "")[:120]
    brand = canonicalize_brand(brand_hint)

    tier = domain_tier(dom, brand_canonical=brand)

    # Legacy whitelist promotes an otherwise-unknown domain to tier 4
    # (sits between EN industry primary and RU industry primary).
    if tier >= 4 and dom in whitelist:
        tier = 4

    return (tier, pub)


def _outside_time_window(a_pub, b_pub) -> bool:
    """True iff both timestamps exist AND differ by more than TIME_WINDOW.

    Missing timestamps → False (don't block a merge on absent data; the
    other guards still apply).
    """
    if not a_pub or not b_pub:
        return False
    return abs((a_pub - b_pub).total_seconds()) > TIME_WINDOW.total_seconds()


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
    # brand+model key per article: prefer the upstream Phase-1 column
    # (col AD), but it is empty for anything without a launch-stage
    # keyword (spy shots, "appears in images"). Fall back to the URL-slug
    # extractor so same-subject stories with divergent headlines still
    # collapse. Lower-cased; "" when neither yields a pair.
    brand_models = []
    for a in articles:
        bm = (a.get("launch_brand_model") or "").strip().lower()
        if not bm or " " not in bm:
            slug_bm = _url_model_key(a.get("url", ""))
            if slug_bm:
                bm = slug_bm
        brand_models.append(bm)
    # Hybrid dedup Stage 1: the LLM's semantic event-signature. The
    # strongest cross-headline / cross-language signal — "Jaguar electric
    # sedan prototype spotted" and "Jaguar Type 01 appears in new images"
    # both yield ("jaguar","type 01","spy_shot"). Only trust it when the
    # model is non-empty AND event_type is specific (NOT "other"/"") —
    # otherwise ("geely","","launch") would glue every Geely launch.
    event_keys: list[str] = []
    for a in articles:
        eb = (a.get("event_brand") or "").strip().lower()
        em = (a.get("event_model") or "").strip().lower()
        et = (a.get("event_type") or "").strip().lower()
        if eb and em and et and et != "other":
            event_keys.append(f"{eb}|{em}|{et}")
        else:
            event_keys.append("")
    for i in range(n):
        ti = norms[i]
        ai_pub = articles[i]["pub_dt"]
        pi = primary_urls[i].strip().lower() if primary_urls[i] else ""
        bmi = brand_models[i]
        eki = event_keys[i]
        for j in range(i + 1, n):
            tj = norms[j]
            pj = primary_urls[j].strip().lower() if primary_urls[j] else ""
            bmj = brand_models[j]
            ekj = event_keys[j]

            # Cross-language safety net: same primary URL = same story.
            # Catches cases where RU and EN headlines have <5 token overlap
            # (different verbs/nouns) but cite the same press release.
            primary_match = bool(pi and pj and pi == pj)

            # Same extracted (brand, model) = same subject even when the
            # two headlines word it completely differently. Real editor
            # case (may-2026): "Jaguar electric sedan prototype spotted"
            # (→ Rumors) vs "Jaguar Type 01 appears in new images"
            # (→ Confirmed) — both extract "jaguar type 01" but share
            # only the token "jaguar", so title-fuzz never merged them.
            # Requires a real "<brand> <model>" pair (a space + length),
            # NOT a bare brand, to avoid gluing every Toyota story. Unlike
            # primary_match this does NOT override the 36h time window —
            # the same model written about months apart is a different
            # story; only same-news-cycle coverage should collapse.
            # Needs a real "<brand> <model>" pair: a space (rules out a
            # bare brand) and ≥5 chars total (shortest real pair like
            # "kia ev9"/"bmw m3" is 6-7; 5 is a safe floor that still
            # rejects junk like "a b").
            brand_model_match = bool(
                bmi and bmi == bmj and " " in bmi and len(bmi) >= 5
            )

            # Hybrid Stage 1: identical LLM event-signature = same story
            # even across totally different headlines / languages. Like
            # brand_model_match it bypasses the lexical guards but still
            # respects the 36h window (a model's launch vs its recall
            # months apart are different stories — different event_type
            # would already separate those; same key within 36h = dup).
            event_match = bool(eki and eki == ekj)
            brand_model_match = brand_model_match or event_match

            if not ti or not tj:
                if primary_match or brand_model_match:
                    if not (
                        brand_model_match
                        and not primary_match
                        and _outside_time_window(
                            articles[i]["pub_dt"], articles[j]["pub_dt"]
                        )
                    ):
                        union(i, j)
                continue

            sim = fuzz.token_set_ratio(ti, tj)
            if sim < threshold and not primary_match and not brand_model_match:
                continue
            # Brand guard — skip when primary_match OR brand_model_match
            # already proved equality.
            if (
                not primary_match
                and not brand_model_match
                and not _brand_overlap(ti, tj)
            ):
                continue
            # Proper-noun overlap guard — at least 2 shared specific tokens
            # (brand + model OR brand + location, etc). Without this, we
            # collapse "Toyota launched X" + "Toyota launched Y" because
            # they share fuzz score but differ in model. Bypassed when the
            # extracted (brand, model) is identical (brand_model_match).
            if (
                not primary_match
                and not brand_model_match
                and _proper_noun_overlap(ti, tj) < PROPER_NOUN_OVERLAP_MIN
            ):
                continue
            # Stricter no-brand guard: when neither title names a brand,
            # the fuzzy/proper-noun signals are weaker (generic words like
            # "продажи", "автомобили", "Россия" pass even between unrelated
            # market-stat stories). Require fuzz ≥ 80 AND proper-noun ≥ 4
            # to prevent gluing different stories like "TOP used luxury
            # cars in Russia" + "Sales of global brand cars increased 2,4x"
            # which share enough generic tokens to cross 65 + 2.
            if not primary_match and not brand_model_match:
                ai_brands = {br for br in _BRANDS_LOWER if br in ti}
                bj_brands = {br for br in _BRANDS_LOWER if br in tj}
                if not ai_brands and not bj_brands:
                    if sim < 80:
                        continue
                    if _proper_noun_overlap(ti, tj) < 4:
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


def _apply_llm_editor_pass(
    groups: list[list[dict]],
    articles: list[dict],
) -> tuple[list[list[dict]], dict]:
    """LLM-as-editor post-processor (safe-mode: ADD merges only).

    Takes lexical clusters + flat article list. For each multi-article
    brand group, asks LLM-as-editor to identify same-event merges.
    Applies those merges via union-find on lexical groups so the
    output is always a SUPERSET of the lexical grouping (never splits
    what lexical merged — guarantees we don't regress).

    Validated on v41: 100% dup-recall + 100% distinct-preservation,
    $0.047 per push (see scripts/_llm_editor_poc.py).

    Returns (new_groups, stats_dict).
    """
    from news_agent.core.llm_editor import (
        cluster_group,
        group_articles_by_brand,
    )

    stats = {
        "brand_groups_total": 0,
        "brand_groups_called": 0,
        "events_returned": 0,
        "merges_applied": 0,
        "cross_run_dups": 0,
        "llm_errors": 0,
        "cost_usd": 0.0,
        "elapsed_s": 0.0,
    }

    # Brand bucket → list of articles. Mercedes-AMG folds into
    # Mercedes-Benz (editor groups them as one family).
    brand_buckets = group_articles_by_brand(
        [
            {**a, "row": a["sheet_row"]}  # llm_editor expects 'row'
            for a in articles
        ],
        bucket_aliases={"Mercedes-AMG": "Mercedes-Benz"},
    )
    # Build sheet_row -> original article dict for fast lookup
    by_row: dict[int, dict] = {a["sheet_row"]: a for a in articles}

    # Article id() -> group index (for union-find)
    article_to_group: dict[int, int] = {}
    for g_idx, grp in enumerate(groups):
        for art in grp:
            article_to_group[id(art)] = g_idx

    # Process multi-article brand groups
    for brand, brand_articles in brand_buckets.items():
        stats["brand_groups_total"] += 1
        if brand == "_unknown" or len(brand_articles) < 2:
            continue
        stats["brand_groups_called"] += 1

        # Hand brand_articles back as the llm_editor-shaped dicts
        ed_articles = [
            {
                "row": a["sheet_row"],
                "title": a["title"],
                "lede": a["lede"],
                "section": a["section"],
                "url": a["url"],
            }
            for a in brand_articles
        ]
        result = cluster_group(brand=brand, articles=ed_articles)
        stats["cost_usd"] += result.cost_usd
        stats["elapsed_s"] += result.elapsed_s
        if result.error:
            stats["llm_errors"] += 1
            print(f"  [llm-editor] {brand}: ERROR {result.error}")
            continue
        stats["events_returned"] += len(result.events)

        for ev in result.events:
            # LOWCONF events stay split — same as lexical.
            if ev.event_id.startswith("LOWCONF__"):
                continue
            if ev.is_cross_run_dup:
                stats["cross_run_dups"] += 1
            if len(ev.member_rows) < 2:
                continue
            # Map event rows back to article dicts via sheet_row
            target_articles = [by_row[r] for r in ev.member_rows
                                if r in by_row]
            if len(target_articles) < 2:
                continue
            # Union their lexical groups
            idxs = sorted({article_to_group[id(a)]
                           for a in target_articles})
            if len(idxs) <= 1:
                continue  # already in same lexical cluster
            target = idxs[0]
            for src in idxs[1:]:
                for art_id, g in list(article_to_group.items()):
                    if g == src:
                        article_to_group[art_id] = target
            stats["merges_applied"] += 1

            # Stash LLM verdict on member articles so the output stage
            # can prefer LLM-chosen primary_row + section.
            for a in target_articles:
                a["_llm_event_summary"] = ev.summary
                a["_llm_event_section"] = ev.section
                a["_llm_event_primary_row"] = ev.primary_row

    # Rebuild groups from union-find result
    by_new_group: dict[int, list[dict]] = {}
    for art in articles:
        g = article_to_group[id(art)]
        by_new_group.setdefault(g, []).append(art)
    new_groups = list(by_new_group.values())
    return new_groups, stats


def main() -> int:
    use_llm_editor = "--use-llm-editor" in sys.argv
    if use_llm_editor:
        sys.argv = [a for a in sys.argv if a != "--use-llm-editor"]
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
    # Extended to AE to read:
    #   AC = launch stage, AD = brand+model (Phase 1 lifecycle)
    #   AE = LLM reason (may-2026 editorial review)
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A2:AH"
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
        launch_stage = _get(r, COL_LAUNCH_STAGE)
        launch_brand_model = _get(r, COL_LAUNCH_BRAND_MODEL)
        # LLM reason: prefer the dedicated col AE; fall back to extracting
        # "reason: ..." substring from llm_note (col M) for legacy v30 rows
        # that pre-date the AE column.
        llm_reason = _get(r, COL_LLM_REASON)
        if not llm_reason:
            note = _get(r, COL_NOTE)
            if "reason:" in note:
                # Extract "reason: <text>" up to next " | " divider or end.
                start = note.find("reason:") + len("reason:")
                end = note.find(" | ", start)
                llm_reason = note[start:end if end > 0 else None].strip()
        # Hybrid dedup Stage 1: semantic event-signature (cols AF-AH).
        ev_brand = _get(r, COL_EVENT_BRAND).strip().lower()
        ev_model = _get(r, COL_EVENT_MODEL).strip().lower()
        ev_type = _get(r, COL_EVENT_TYPE).strip().lower()
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
            "launch_stage": launch_stage,
            "launch_brand_model": launch_brand_model,
            "llm_reason": llm_reason,
            "primary_dom": primary_dom,
            "primary_url": primary_url,
            "primary_conf": primary_conf,
            "event_brand": ev_brand,
            "event_model": ev_model,
            "event_type": ev_type,
        })
    print(f"'Точно новость' rows: {len(articles)}")

    groups = cluster_articles(articles)
    print(f"Lexical clusters: {len(groups)}")

    if use_llm_editor:
        print("LLM-as-editor pass: ON (--use-llm-editor)")
        groups, ed_stats = _apply_llm_editor_pass(groups, articles)
        print(f"  brand groups called: {ed_stats['brand_groups_called']}"
              f"/{ed_stats['brand_groups_total']}")
        print(f"  events returned:     {ed_stats['events_returned']}")
        print(f"  merges applied:      {ed_stats['merges_applied']}")
        print(f"  cross-run dups:      {ed_stats['cross_run_dups']}")
        print(f"  llm errors:          {ed_stats['llm_errors']}")
        print(f"  cost: ${ed_stats['cost_usd']:.4f}  "
              f"time: {ed_stats['elapsed_s']:.0f}s")
        print(f"Clusters after LLM-editor: {len(groups)}")

    # Pack output
    out_clusters: list[dict] = []
    singletons = 0
    for grp in groups:
        # Sort by priority — first is canonical. If LLM-editor assigned
        # an explicit primary_row in this group, honour it.
        llm_primary = next(
            (a["_llm_event_primary_row"] for a in grp
             if a.get("_llm_event_primary_row")),
            None,
        )
        if llm_primary is not None:
            grp_sorted = sorted(
                grp,
                key=lambda a: (0 if a["sheet_row"] == llm_primary else 1,
                                _cluster_priority(
                                    a,
                                    press_release_hosts=press_release_hosts,
                                    whitelist=whitelist,
                                )),
            )
        else:
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
        # Phase-1 launch stage carry-through: pick the first non-empty
        # stage / brand+model from any cluster member (canonical first,
        # then the rest). Some sources extract stage/model better than
        # others; one populated value across the cluster wins.
        launch_stage = ""
        launch_brand_model = ""
        llm_reason = ""
        for a in grp_sorted:
            if not launch_stage and a.get("launch_stage"):
                launch_stage = a["launch_stage"]
            if not launch_brand_model and a.get("launch_brand_model"):
                launch_brand_model = a["launch_brand_model"]
            if not llm_reason and a.get("llm_reason"):
                llm_reason = a["llm_reason"]
            if launch_stage and launch_brand_model and llm_reason:
                break

        # If LLM-as-editor assigned a section to this event, prefer it.
        # Validated: editor's manual section corrections (96 in v2 sync)
        # were 93% concordant with LLM-as-editor PoC on v41.
        llm_section = next(
            (a.get("_llm_event_section") for a in grp_sorted
             if a.get("_llm_event_section")),
            None,
        )
        section_final = llm_section or canonical["section"]

        cluster = {
            "size": len(grp),
            "canonical_title": canonical["title"],
            "canonical_url": canonical["url"],
            "canonical_domain": canonical["domain"],
            "canonical_lede": canonical["lede"],
            "section": section_final,
            "region": canonical["region"],
            "country": canonical["country"],
            "published": canonical["published"],
            "image_url": canonical["image_url"],
            "primary_domain": canonical["primary_dom"],
            "primary_url": canonical["primary_url"],
            "primary_conf": canonical["primary_conf"],
            "launch_stage": launch_stage,
            "launch_brand_model": launch_brand_model,
            "llm_reason": llm_reason,
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
