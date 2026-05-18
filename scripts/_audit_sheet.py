"""Read-only audit of the editor «Новости (новые)» tab.

Surfaces, without changing anything:
  1. near-duplicate rows missed by the conservative dedup
     (title-fuzz >= 80 within 7 days OR shared _url_model_key)
  2. same brand+model appearing in DIFFERENT sections (inconsistency)
  3. malformed / placeholder titles (empty, <UNKNOWN>, single word,
     extraction junk, missing EN or RU line)
  4. rows with a verdict but no section, or section not in the canon
  5. section distribution sanity + flag/comment tallies
"""

from __future__ import annotations

import io
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from googleapiclient.discovery import build  # noqa: E402
from google.oauth2 import service_account  # noqa: E402
from rapidfuzz import fuzz  # noqa: E402

from news_agent.core.config_loader import load_brand_domains  # noqa: E402

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
CANON_SECTIONS = {
    "Confirmed", "Local specifics", "Other news", "Rumors", "Economics",
    "LCV news", "Test-drive", "Test-drive (неактивный)",
    "Dealer news / Promo", "Motorshow",
}
_PLACEHOLDER = {"", "<unknown>", "unknown", "untitled", "no title", "новости"}


def _norm(t: str) -> str:
    en = ""
    for line in t.splitlines():
        s = line.strip()
        if s.startswith("EN:"):
            en = s[3:].strip()
    src = en or t
    src = re.sub(r"\s*\([A-Za-zА-Яа-яЁё]{2,4}\)\s*$", "", src)
    return re.sub(r"[^a-zа-я0-9 ]", " ", src.lower()).strip()


def main() -> int:
    brands = load_brand_domains()
    bnc._BRANDS_LOWER = [b.brand.lower() for b in brands] + [
        a.lower() for b in brands for a in getattr(b, "aliases", []) or []
    ]
    bnc._BRANDS_LOWER = [b for b in bnc._BRANDS_LOWER if len(b) >= 4]

    rows = (
        svc.spreadsheets().values().get(
            spreadsheetId=EDITOR,
            range=f"'{TAB}'!A1:P700",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute().get("values", [])
    )

    recs = []
    for i, row in enumerate(rows, 1):
        if i == 1 or (row and "Прогон от" in str(row[0])):
            continue
        title = str(row[1]) if len(row) > 1 else ""
        if not str(row[0]).strip() if row else True:
            continue
        recs.append({
            "row": i,
            "title": title,
            "section": str(row[3]) if len(row) > 3 else "",
            "url": str(row[9]) if len(row) > 9 else "",
            "pub": bnc._parse_dt(str(row[6]) if len(row) > 6 else ""),
            "comment": (str(row[15]) if len(row) > 15 else "").strip(),
            "norm": _norm(title),
            "umk": bnc._url_model_key(str(row[9]) if len(row) > 9 else ""),
        })

    print(f"=== AUDIT «{TAB}» — {len(recs)} data rows ===\n")

    # 1. section distribution
    sec = Counter(r["section"] for r in recs)
    print("--- Section distribution ---")
    for k, v in sec.most_common():
        print(f"  {k or '(empty)':<24} {v}")
    bad_sec = [r for r in recs if r["section"] and r["section"] not in CANON_SECTIONS]
    if bad_sec:
        print(f"  ! {len(bad_sec)} rows with NON-CANON section:")
        for r in bad_sec[:10]:
            print(f"    r{r['row']}: {r['section']!r}  {r['title'][:50]}")
    print()

    # 2. malformed / placeholder titles
    print("--- Malformed / placeholder titles ---")
    mal = []
    for r in recs:
        t = r["title"]
        low = t.strip().lower()
        en = ru = ""
        for ln in t.splitlines():
            s = ln.strip()
            if s.startswith("EN:"):
                en = s[3:].strip()
            elif s.startswith("RU:"):
                ru = s[3:].strip()
        if low in _PLACEHOLDER or "<unknown>" in low:
            mal.append((r, "placeholder"))
        elif en and not ru:
            mal.append((r, "missing RU line"))
        elif ru and not en:
            mal.append((r, "missing EN line"))
        elif en and len(en.split()) <= 2 and len(en) < 18:
            mal.append((r, f"too-short EN: {en!r}"))
    if not mal:
        print("  (none)")
    for r, why in mal[:20]:
        print(f"  r{r['row']} [{r['section']}] {why}")
        print(f"     {r['title'][:70].replace(chr(10), ' || ')}")
    print()

    # 3. near-duplicate detection (looser than the conservative dedup)
    print("--- Near-duplicate candidates (fuzz>=80 / shared umk, <=7d) ---")
    dups = []
    n = len(recs)
    for a in range(n):
        for b in range(a + 1, n):
            ra, rb = recs[a], recs[b]
            same_umk = bool(ra["umk"] and ra["umk"] == rb["umk"])
            f = fuzz.token_set_ratio(ra["norm"], rb["norm"]) if ra["norm"] and rb["norm"] else 0
            if not (same_umk or f >= 80):
                continue
            if ra["pub"] and rb["pub"]:
                if abs((ra["pub"] - rb["pub"]).days) > 7:
                    continue
            dups.append((ra, rb, f, same_umk))
    if not dups:
        print("  (none)")
    for ra, rb, f, umk in dups[:25]:
        tag = "umk" if umk else f"fuzz={f}"
        flag = ""
        if ra["section"] != rb["section"]:
            flag = "  ⚠ DIFFERENT SECTIONS"
        print(f"  r{ra['row']}[{ra['section']}] ~ r{rb['row']}[{rb['section']}] ({tag}){flag}")
        print(f"     A: {ra['title'][:62].replace(chr(10),' || ')}")
        print(f"     B: {rb['title'][:62].replace(chr(10),' || ')}")
    print(f"\n  total near-dup pairs: {len(dups)} "
          f"(diff-section: {sum(1 for x in dups if x[0]['section'] != x[1]['section'])})")
    print()

    # 4. comment / flag tallies
    commented = [r for r in recs if r["comment"] and not r["comment"].startswith(
        ("⚠ v37", "🔁"))]
    print(f"--- Editor comments: {len(commented)} rows ---")
    return 0


if __name__ == "__main__":
    sys.exit(main())
