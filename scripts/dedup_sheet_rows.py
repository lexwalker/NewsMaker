"""Find duplicate rows already pushed to the editor's «Новости (новые)»
and (with --apply) merge + hide them.

Grouping = the same high-confidence signals the live clusterer uses,
deliberately CONSERVATIVE so we never collapse two distinct stories:

  • identical primary URL (col J), OR
  • identical _url_model_key() derived from the article URL (col D),
    e.g. both ".../jaguar-type-01-..." → "jaguar type 01"
  AND published within TIME_WINDOW (36h) of each other.

Loose title-fuzz alone is NOT used here — too risky for an in-place
edit of the editor's working sheet.

For each group of 2+ rows:
  • canonical = press-release/whitelist domain first, else earliest row
  • section: if ANY member's title carries a spy-shot/rumor signal and
    none has brand-voice, the canonical is forced to "Rumors"
    (editor: spy shots are never Confirmed)
  • duplicates' source URLs are merged into the canonical col M
  • duplicate rows are HIDDEN (reversible: hiddenByUser) and annotated
    in col P "🔁 дубль строки <canonical> (<reason>)"
  • a duplicate row that already holds an editor comment is annotated
    but NOT hidden (respect their review)

Dry-run by default. --apply writes. --start/--end limit the row range.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from googleapiclient.discovery import build  # noqa: E402
from google.oauth2 import service_account  # noqa: E402

from news_agent.core.config_loader import load_brand_domains  # noqa: E402
from news_agent.core.heuristic_relevance import (  # noqa: E402
    has_brand_voice,
    has_rumor_signal,
)

import build_news_clusters as bnc  # noqa: E402

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs"
)
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
creds = service_account.Credentials.from_service_account_file(
    str(SA_PATH), scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
svc = build("sheets", "v4", credentials=creds)
TAB = "Новости (новые)"

# Column indexes (0-based) in «Новости (новые)».
C_RUN, C_TITLE, C_LEDE, C_SECTION = 0, 1, 2, 3
C_URL, C_PUB = 9, 6           # primary URL col J(9), pub date col G(6)
C_ALLURLS, C_COMMENT = 12, 15  # col M(12), col P(15)


from urllib.parse import urlparse  # noqa: E402

# Primary-source extraction sometimes assigns the SAME junk/placeholder
# URL to many unrelated articles (uaz.ru/owner, .../subscribe,
# google.com/preferences/...). Treating those as "same primary" would
# catastrophically merge distinct stories. Only trust same_primary when
# BOTH URLs look like a real per-article permalink.
_JUNK_PATH_TAILS = {
    "owner", "subscribe", "preferences", "login", "signin", "sign-in",
    "register", "signup", "sign-up", "news", "feed", "rss", "index",
    "home", "about", "contact", "contacts", "search", "press", "newsroom",
    "media", "category", "tag", "tags", "topic", "topics",
}
_JUNK_HOSTS = (
    "google.", "facebook.", "twitter.", "x.com", "t.me", "vk.com",
    "youtube.", "linkedin.", "instagram.", "substack.com",
)


def _is_real_article_url(url: str) -> bool:
    """True only for a substantive per-article permalink.

    Rejects bare section/index pages, junk tails, and aggregator/redirect
    hosts. A real slug is long and hyphenated
    ("ao-avtovaz-i-ooo-promteh-podpisali-so...") — that's the signal we
    keep; "owner" / "subscribe" / a Google redirect are dropped.
    """
    if not url:
        return False
    try:
        p = urlparse(url)
    except Exception:  # noqa: BLE001
        return False
    host = (p.netloc or "").lower()
    if any(j in host for j in _JUNK_HOSTS):
        return False
    segs = [s for s in (p.path or "").split("/") if s]
    if not segs:
        return False
    tail = segs[-1].lower()
    if tail in _JUNK_PATH_TAILS:
        return False
    # Real article slug: long OR clearly multi-token hyphenated.
    return len(tail) >= 20 or tail.count("-") >= 3


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Merge+hide dup rows in editor sheet")
    p.add_argument("--start", type=int, default=3)
    p.add_argument("--end", type=int, default=200)
    p.add_argument("--apply", action="store_true")
    return p.parse_args()


def _sheet_id_for_tab() -> int:
    meta = svc.spreadsheets().get(
        spreadsheetId=EDITOR, fields="sheets(properties(sheetId,title))"
    ).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == TAB:
            return s["properties"]["sheetId"]
    raise RuntimeError(f"tab {TAB!r} not found")


def main() -> int:
    args = _parse_args()
    # Populate the brand list the clusterer's _url_model_key relies on.
    brands = load_brand_domains()
    bnc._BRANDS_LOWER = []
    for b in brands:
        bnc._BRANDS_LOWER.append(b.brand.lower())
        for a in getattr(b, "aliases", []) or []:
            bnc._BRANDS_LOWER.append(a.lower())
    bnc._BRANDS_LOWER = [b for b in bnc._BRANDS_LOWER if len(b) >= 4]

    rows = (
        svc.spreadsheets().values().get(
            spreadsheetId=EDITOR,
            range=f"'{TAB}'!A{args.start}:S{args.end}",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute().get("values", [])
    )

    recs = []
    for off, row in enumerate(rows):
        sr = args.start + off
        if not row or "Прогон от" in str(row[0]):
            continue
        title = str(row[C_TITLE]) if len(row) > C_TITLE else ""
        if not title.strip():
            continue
        url = str(row[C_URL]) if len(row) > C_URL else ""
        recs.append({
            "row": sr,
            "title": title,
            "lede": str(row[C_LEDE]) if len(row) > C_LEDE else "",
            "section": str(row[C_SECTION]) if len(row) > C_SECTION else "",
            "url": url,
            "pub": bnc._parse_dt(str(row[C_PUB]) if len(row) > C_PUB else ""),
            "allurls": str(row[C_ALLURLS]) if len(row) > C_ALLURLS else "",
            "comment": (str(row[C_COMMENT]) if len(row) > C_COMMENT else "").strip(),
            "umk": bnc._url_model_key(url),
        })

    # Union-find on conservative signals.
    parent = list(range(len(recs)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i, j):
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[max(ri, rj)] = min(ri, rj)

    TW = bnc.TIME_WINDOW.total_seconds()
    for i in range(len(recs)):
        for j in range(i + 1, len(recs)):
            a, b = recs[i], recs[j]
            pa = a["url"].strip().lower()
            pb = b["url"].strip().lower()
            # same_primary only when the shared URL is a REAL article
            # permalink — never a junk/placeholder both rows happen to
            # carry from broken primary-source extraction.
            same_primary = bool(
                pa and pa == pb and _is_real_article_url(pa)
            )
            same_umk = bool(a["umk"] and a["umk"] == b["umk"])
            if not (same_primary or same_umk):
                continue
            if a["pub"] and b["pub"]:
                if abs((a["pub"] - b["pub"]).total_seconds()) > TW and not same_primary:
                    continue
            union(i, j)

    groups: dict[int, list[int]] = {}
    for idx in range(len(recs)):
        groups.setdefault(find(idx), []).append(idx)
    dup_groups = [g for g in groups.values() if len(g) > 1]

    print(f"=== dedup {TAB!r} rows {args.start}-{args.end} ===")
    print(f"Records: {len(recs)} | duplicate groups: {len(dup_groups)} "
          f"| rows to hide: {sum(len(g) - 1 for g in dup_groups)}")
    print()

    batch_values: list[dict] = []
    hide_requests: list[dict] = []
    sheet_id = _sheet_id_for_tab() if args.apply else -1

    for g in dup_groups:
        members = sorted(g, key=lambda k: recs[k]["row"])
        # canonical = earliest sheet row (already nearest the top = newest run)
        canon = members[0]
        crow = recs[canon]
        # spy-shot rule: any rumor-signal member + no brand-voice → Rumors
        any_rumor = any(
            has_rumor_signal(recs[m]["title"]) for m in members
        )
        any_brand_voice = any(
            has_brand_voice(recs[m]["lede"]) for m in members
        )
        forced_section = (
            "Rumors" if (any_rumor and not any_brand_voice) else None
        )
        merged_urls = []
        for m in members:
            for u in (recs[m]["allurls"] or recs[m]["url"]).split():
                if u and u not in merged_urls:
                    merged_urls.append(u)

        print(f"GROUP → canonical r{crow['row']} [{crow['section']}"
              f"{' → Rumors' if forced_section else ''}]")
        print(f"  {crow['title'][:75]}")
        for m in members[1:]:
            r = recs[m]
            tag = "comment-keep" if r["comment"] and not r["comment"].startswith(
                ("🔁", "⚠ v37")) else "hide"
            print(f"  dup r{r['row']} [{r['section']}] ({tag}): "
                  f"{r['title'][:60]}")

        if not args.apply:
            continue

        # canonical: merge URLs (col M) + fix section (col D) if forced
        batch_values.append({
            "range": f"'{TAB}'!M{crow['row']}",
            "values": [["\n".join(merged_urls)]],
        })
        if forced_section and crow["section"] != "Rumors":
            batch_values.append({
                "range": f"'{TAB}'!D{crow['row']}",
                "values": [["Rumors"]],
            })
        for m in members[1:]:
            r = recs[m]
            note = f"🔁 дубль строки {crow['row']}"
            if r["comment"] and not r["comment"].startswith(("🔁", "⚠ v37")):
                # editor already reviewed — annotate, do NOT hide
                batch_values.append({
                    "range": f"'{TAB}'!P{r['row']}",
                    "values": [[f"{note} | ред.коммент: {r['comment'][:120]}"]],
                })
                continue
            batch_values.append({
                "range": f"'{TAB}'!P{r['row']}",
                "values": [[note]],
            })
            hide_requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": r["row"] - 1,  # 0-based, inclusive
                        "endIndex": r["row"],        # exclusive
                    },
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            })

    if not args.apply:
        print("\n(dry-run — pass --apply to merge + hide)")
        return 0

    if batch_values:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=EDITOR,
            body={"valueInputOption": "USER_ENTERED", "data": batch_values},
        ).execute()
    if hide_requests:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=EDITOR, body={"requests": hide_requests}
        ).execute()
    print(f"\nAPPLIED: {len(dup_groups)} groups, "
          f"{len(hide_requests)} rows hidden, "
          f"{len(batch_values)} cell writes.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
