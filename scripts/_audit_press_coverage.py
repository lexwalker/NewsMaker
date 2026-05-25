"""Press-coverage audit — structured registry gap-fill.

Treats the editor-cited URLs in data/eval_set_v2.jsonl as the
ground-truth "this is what the canonical primary SHOULD be" set, then
checks our current crawl + brand_domains registry against it.

Designed as Layer 1+2 (registry + discovery) of the 6-layer
autonomous-editor architecture. Output is NOT a flat domain dump but
two structured patches:

  (1) brand_domains.yaml additions
        — for new OEM press domains the editor named.
        Strengthens the Layer 1 registry that Layer 3 (search) will
        later query.

  (2) crawl-source additions (printed as Sheet rows to copy in)
        — for domains we should fetch periodically so the local index
        (Layer 2) has them when LLM-as-editor needs them.

Categorisation per editor-cited domain:
  A. ALREADY-CRAWLED          → no patch; tier-fix from yesterday handles it
  B. PARTIAL                  → main domain crawled, specific path/subdomain not
  C. MISSING (feed-able)      → add to crawl
  D. SPECIAL-FETCHER          → needs custom infra (NHTSA, patent offices)
  E. MIRROR/EDITOR-SNIPPET    → t.me, telegra.ph — not a primary source anyway

Dry-run by default. --apply writes patches to brand_domains.yaml only
(crawl-source rows are shown so editor can add them by hand, since
the sheet's audit semantics live with the editor).
"""

from __future__ import annotations

import io
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

import yaml  # noqa: E402
from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

from news_agent.core.brand_canonical import canonicalize_brand  # noqa: E402
from news_agent.core.source_priority import (  # noqa: E402
    GENERIC_PRESS_HOSTS,
    INDUSTRY_EN_PRIMARY,
    INDUSTRY_RU_PRIMARY,
    MIRROR_HOSTS,
    REGULATOR_DOMAINS,
    RU_AGGREGATORS,
)

DATA = ROOT / "data"
EVAL_V2 = DATA / "eval_set_v2.jsonl"
BRAND_DOMAINS_YAML = ROOT / "config" / "brand_domains.yaml"

# Domains that need special fetchers (no clean RSS / structured feed).
# Listed here so they're surfaced separately, not lumped into "missing".
SPECIAL_FETCHER_HOSTS = frozenset({
    "nhtsa.gov", "static.nhtsa.gov",
    "dpma.de", "register.dpma.de",
    "uspto.gov", "ppubs.uspto.gov",
    "globalncap.org",
    "europa.eu", "ec.europa.eu",
})


def _norm(dom: str) -> str:
    return (dom or "").lower().lstrip(".")


def _strip_www(dom: str) -> str:
    d = _norm(dom)
    return d[4:] if d.startswith("www.") else d


def _registered_domains() -> dict[str, str]:
    """All domains in brand_domains.yaml → canonical brand."""
    data = yaml.safe_load(BRAND_DOMAINS_YAML.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for entry in data.get("brands", []):
        brand = entry.get("brand", "").strip()
        for d in entry.get("domains", []) or []:
            out[_norm(d)] = brand
    return out


def _crawled_domains() -> set[str]:
    """Domains currently in our active crawl source list."""
    SHEET_ID = os.environ["SPREADSHEET_ID"]
    SOURCES_TAB = os.environ["SOURCES_TAB_RU"]
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    svc = build("sheets", "v4", credentials=creds)
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{SOURCES_TAB}'",
    ).execute()
    out: set[str] = set()
    for row in resp.get("values", [])[1:]:
        if not row:
            continue
        flag = (row[0] if len(row) > 0 else "").strip()
        url = (row[1] if len(row) > 1 else "").strip()
        if flag == "0" or not url:
            continue
        try:
            out.add(_strip_www(urlparse(url).netloc))
        except Exception:
            pass
    return out


def _editor_cited_urls() -> list[tuple[str, dict]]:
    """Return [(url, row_meta), ...] for all editor referenced URLs."""
    out = []
    for ln in EVAL_V2.read_text(encoding="utf-8").splitlines():
        if not ln.strip():
            continue
        try:
            r = json.loads(ln)
        except Exception:
            continue
        for url in r.get("referenced_urls", []) or []:
            out.append((url, {
                "row": r.get("row"),
                "title": r.get("title", "")[:80],
                "wrong_primary": r.get("label_wrong_primary"),
                "needs_translation": r.get("label_needs_translation"),
                "dup_cross_run": r.get("label_dup_cross_run"),
            }))
    return out


def _classify_domain(
    domain: str,
    *,
    full_url: str,
    crawled: set[str],
    registered: dict[str, str],
) -> str:
    """Return category code (A/B/C/D/E) for this editor-cited domain."""
    d = _strip_www(domain)
    if not d:
        return "E"

    # Mirror / social: never a primary anyway
    if any(d == h or d.endswith("." + h) for h in MIRROR_HOSTS):
        return "E"

    # Special fetcher needed
    if any(d == h or d.endswith("." + h) for h in SPECIAL_FETCHER_HOSTS):
        return "D"

    # Already crawled exactly
    if d in crawled:
        return "A"

    # Sub-host of a crawled root (e.g. global.honda when honda.com crawled)
    parts = d.split(".")
    for i in range(1, len(parts)):
        root = ".".join(parts[i:])
        if root in crawled:
            return "B"  # partial — main domain crawled, specific sub not

    # Registered as OEM domain but not in crawl
    if d in registered:
        return "C"
    # OR a sub of a registered domain
    for i in range(1, len(parts)):
        root = ".".join(parts[i:])
        if root in registered:
            return "C"

    # Not crawled, not registered
    return "C"


def _suggest_brand(url: str, title: str, registered: dict[str, str]) -> str:
    """Best guess at canonical brand the URL is press-for."""
    d = _strip_www(urlparse(url).netloc)
    # Direct domain match in registry
    if d in registered:
        return registered[d]
    # Sub-host?
    parts = d.split(".")
    for i in range(1, len(parts)):
        root = ".".join(parts[i:])
        if root in registered:
            return registered[root]
    # Fallback: extract from title
    return canonicalize_brand(title)


def _suggest_tier(domain: str) -> str:
    d = _strip_www(domain)
    if any(d == h or d.endswith("." + h) for h in REGULATOR_DOMAINS):
        return "regulator"
    if any(d == h or d.endswith("." + h) for h in GENERIC_PRESS_HOSTS):
        return "oem-generic"
    if any(d == h or d.endswith("." + h) for h in INDUSTRY_EN_PRIMARY):
        return "industry-en"
    if any(d == h or d.endswith("." + h) for h in INDUSTRY_RU_PRIMARY):
        return "industry-ru"
    if any(d == h or d.endswith("." + h) for h in RU_AGGREGATORS):
        return "aggregator"
    # heuristic: starts with media. / press. / newsroom. / global. → OEM
    if any(d.startswith(p + ".") for p in ("media", "press", "newsroom",
                                            "global")):
        return "oem-brand"
    return "unknown"


# Domains that look like sources but aren't editorial primaries.
# Filtered out of the Patch 2 (crawl-add) recommendations so we don't
# spam the sheet with junk.
_SKIP_DOMAINS_FROM_PATCH2 = frozenset({
    # URL shorteners
    "skr.sh", "bit.ly", "t.co", "tinyurl.com",
    # Ford consumer content (press lives at media.ford.com, already crawled)
    "fromtheroad.ford.com",
})


def _add_sources_to_sheet(
    urls: list[tuple[str, str]],
    *,
    active_flag: str = "0",  # default "0" = inactive — editor opts in
) -> int:
    """Append URLs to the SOURCES_TAB sheet. Returns count added."""
    SHEET_ID = os.environ["SPREADSHEET_ID"]
    SOURCES_TAB = os.environ["SOURCES_TAB_RU"]
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    svc = build("sheets", "v4", credentials=creds)

    # Read existing rows to know where to append + avoid double-adds
    existing = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{SOURCES_TAB}'",
    ).execute().get("values", [])
    existing_urls = {
        (r[1].strip().lower() if len(r) > 1 else "")
        for r in existing[1:]
    }
    new_rows = [
        [active_flag, url, f"audit-added: {note}"]
        for url, note in urls
        if url.lower() not in existing_urls
    ]
    if not new_rows:
        return 0
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"'{SOURCES_TAB}'!A:C",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": new_rows},
    ).execute()
    return len(new_rows)


def main() -> int:
    args = sys.argv[1:]
    apply_yaml = "--apply-yaml" in args
    apply_sheet = "--apply-sheet" in args

    crawled = _crawled_domains()
    registered = _registered_domains()
    cited = _editor_cited_urls()
    print(f"Crawl list domains: {len(crawled)}")
    print(f"brand_domains.yaml registered domains: {len(registered)}")
    print(f"Editor-cited URLs in eval_set_v2: {len(cited)}")
    print()

    # Aggregate: domain → list[(url, row_meta)]
    by_dom: dict[str, list] = defaultdict(list)
    for url, meta in cited:
        try:
            d = _strip_www(urlparse(url).netloc)
        except Exception:
            continue
        if d:
            by_dom[d].append((url, meta))

    print(f"Unique editor-cited domains: {len(by_dom)}\n")

    # Classify
    by_cat: dict[str, dict[str, list]] = {
        "A": {}, "B": {}, "C": {}, "D": {}, "E": {},
    }
    for d, items in by_dom.items():
        cat = _classify_domain(d, full_url=items[0][0],
                                crawled=crawled, registered=registered)
        by_cat[cat][d] = items

    cat_titles = {
        "A": "ALREADY CRAWLED — tier-fix from Task B handles it (no patch)",
        "B": "PARTIAL — main domain crawled, specific path/subdomain NOT",
        "C": "MISSING — domain entirely absent from crawl + registry",
        "D": "SPECIAL FETCHER NEEDED — regulator/patent (no clean feed)",
        "E": "MIRROR / SOCIAL — never a primary source anyway",
    }

    for cat in "ABCDE":
        items = by_cat[cat]
        cited_count = sum(len(v) for v in items.values())
        print(f"━━ Category {cat}: {cat_titles[cat]}")
        print(f"   {len(items)} domains, cited {cited_count} times by editor\n")
        for d, urls in sorted(items.items(),
                               key=lambda x: -len(x[1]))[:10]:
            brand = _suggest_brand(urls[0][0], urls[0][1]["title"], registered)
            tier = _suggest_tier(d)
            print(f"   {d:38} {len(urls):>2}x  "
                  f"brand={brand or '—':18} tier={tier}")
            for u, m in urls[:2]:
                flag = ("wp" if m["wrong_primary"] else
                         "dup" if m["dup_cross_run"] else
                         "tr" if m["needs_translation"] else "?")
                print(f"     [{flag}] r{m['row']:>3}  {u[:75]}")
        if len(items) > 10:
            print(f"   ... +{len(items)-10} more")
        print()

    # ── PROPOSED PATCHES ──────────────────────────────────────────
    print("=" * 72)
    print("PROPOSED PATCHES")
    print("=" * 72)

    # Patch 1: brand_domains.yaml additions
    # For Category C (missing) domains that we can attribute to a brand
    # AND that look like OEM press hosts → register them.
    print("\n── PATCH 1: brand_domains.yaml — add OEM press domains "
          "to existing brand entries ──\n")
    yaml_additions: dict[str, list[str]] = defaultdict(list)
    for d in by_cat["C"]:
        tier = _suggest_tier(d)
        if tier not in ("oem-brand", "oem-generic"):
            continue
        brand = _suggest_brand(
            by_cat["C"][d][0][0],
            by_cat["C"][d][0][1]["title"],
            registered,
        )
        if not brand:
            continue
        # Skip if already registered under this brand
        if d in registered and registered[d] == brand:
            continue
        yaml_additions[brand].append(d)

    if not yaml_additions:
        print("  (no OEM-brand additions detected)")
    else:
        for brand, doms in yaml_additions.items():
            print(f"  - brand: {brand}")
            print(f"      add domains: {sorted(doms)}")

    # Patch 2: crawl-source additions (Category C non-OEM + Category B)
    print("\n── PATCH 2: crawl-source rows to add to Sheet "
          "(SOURCES_TAB_RU) ──\n")
    crawl_adds: list[tuple[str, str, int, str]] = []  # (url, kind, cites, ex_title)
    for cat in ("B", "C"):
        for d, urls in by_cat[cat].items():
            if cat == "C":
                tier = _suggest_tier(d)
                if tier == "oem-brand":
                    # Add a /news/ or root path
                    suggest_url = f"https://{d}/"
                else:
                    suggest_url = f"https://{d}/"
            else:  # Category B — use the deeper path the editor cited
                # Pick the most-common path prefix from cited URLs
                paths = [urlparse(u).path for u, _ in urls if u]
                if paths:
                    # Trim to second segment as feed root
                    p = paths[0].rstrip("/")
                    parts = p.split("/")
                    suggest_path = "/" + "/".join(parts[1:2]) + "/" if len(parts) > 1 else "/"
                else:
                    suggest_path = "/"
                suggest_url = f"https://{d}{suggest_path}"
            crawl_adds.append((suggest_url, _suggest_tier(d), len(urls),
                               urls[0][1]["title"]))

    if not crawl_adds:
        print("  (nothing to add)")
    else:
        print(f"  {'URL':70} {'tier':12} cites  example_title")
        for url, kind, cites, t in sorted(crawl_adds, key=lambda x: -x[2]):
            print(f"  {url:70} {kind:12} {cites:>4}x  {t[:50]}")

    # Patch 3: Category D — note only, no action
    if by_cat["D"]:
        print("\n── PATCH 3 (deferred): SPECIAL FETCHERS ──\n")
        print("  These need search-API integration, not feed crawling.")
        print("  Recommended for Layer 3 (active retrieval) phase:")
        for d, urls in by_cat["D"].items():
            print(f"    {d:30} {len(urls):>2}x cited")

    # Apply patch 1 if asked
    if apply_yaml and yaml_additions:
        print("\n── APPLYING brand_domains.yaml additions ──")
        data = yaml.safe_load(BRAND_DOMAINS_YAML.read_text(encoding="utf-8"))
        modified = 0
        for entry in data.get("brands", []):
            brand = entry.get("brand", "").strip()
            if brand in yaml_additions:
                existing = set(_norm(d) for d in (entry.get("domains") or []))
                additions = [d for d in yaml_additions[brand]
                              if _norm(d) not in existing]
                if additions:
                    entry.setdefault("domains", []).extend(additions)
                    print(f"  + {brand}: added {additions}")
                    modified += 1
        if modified:
            BRAND_DOMAINS_YAML.write_text(
                yaml.dump(data, allow_unicode=True, sort_keys=False),
                encoding="utf-8",
            )
            print(f"\nWrote {modified} updates to "
                  f"{BRAND_DOMAINS_YAML.name}")
        else:
            print("  (no actual additions — all already registered)")
    elif yaml_additions:
        print("\n  (run with --apply-yaml to write brand_domains.yaml "
              "additions)")

    # Patch 2 application — curated to skip junk
    print("\n── PATCH 2 application status ──\n")
    curated = [
        (url, f"{kind}, {cites}x cited")
        for url, kind, cites, _t in crawl_adds
        if _strip_www(urlparse(url).netloc) not in _SKIP_DOMAINS_FROM_PATCH2
        and kind != "unknown"  # skip ambiguous classifications
    ]
    skipped = [
        url for url, _kind, _cites, _t in crawl_adds
        if _strip_www(urlparse(url).netloc) in _SKIP_DOMAINS_FROM_PATCH2
        or (_strip_www(urlparse(url).netloc) not in _SKIP_DOMAINS_FROM_PATCH2
            and _suggest_tier(_strip_www(urlparse(url).netloc)) == "unknown")
    ]
    print(f"  Curated for crawl-sheet: {len(curated)}")
    for url, note in curated:
        print(f"    + {url:60} ({note})")
    if skipped:
        print(f"  Skipped (junk or ambiguous): {len(skipped)}")
        for url in skipped:
            print(f"    - {url}")

    if apply_sheet and curated:
        print("\n  Appending to SOURCES_TAB_RU with active='0' "
              "(inactive — editor enables after review)…")
        n = _add_sources_to_sheet(curated, active_flag="0")
        print(f"  Wrote {n} new source rows.")
    elif curated:
        print("\n  (run with --apply-sheet to append rows to "
              "SOURCES_TAB_RU as inactive — editor enables after review)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
