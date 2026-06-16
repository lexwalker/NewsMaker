"""Weekly KPI — one reproducible, HONEST run of the 4 agreed metrics for a
week window, saved to data/weekly_kpi_<until>.json so weeks are comparable.

  coverage      — of editor publications this week, how many we collected
  found_right   — of our accepted items, how many the editor published
  section_right — of those, how many sections agree
  reject_right  — of our rejected items, how many editor did NOT publish (PROXY)

Sources (all already available): editor archive "Опубликованные (все)" + our
SQLite cache (data/news_agent.sqlite). Strict matching (url_key + brand-gated
fuzzy title, threshold 85, same as miss_funnel) — NOT the retired token-blob
match that inflated coverage to 83%.

Usage:
  python scripts/weekly_kpi.py                       # last 7 days
  python scripts/weekly_kpi.py --since 2026-06-10 --until 2026-06-16
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

from news_agent.core.published_dedup import url_key  # noqa: E402
from news_agent.core.reject_stage import (  # noqa: E402
    S3_HEURISTIC, S4_LLM, classify_outcome,
)
from news_agent.core.weekly_kpi import (  # noqa: E402
    Item, build_index, coverage, precision_and_section, reject_right,
)

# The published archive + the editor feed live ONLY in the editor's
# original spreadsheet (EDITOR_SPREADSHEET_ID = 1fQic…), NOT in the working
# copy (SPREADSHEET_ID = 14PTb…, which has no "Опубликованные (все)" tab).
SHEET = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
PUB_TAB = "Опубликованные (все)"
SQLITE_PATH = DATA / "news_agent.sqlite"
ACCEPT = {"Точно новость", "Возможно новость"}


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def _parse_date(s):
    """Robust to the THREE date formats the editor's tabs actually use:
    ISO ('2026-06-04 17:50'), US ('05/26/2026 17:06:00'), and Excel serial
    numbers (46177.72 — days since 1899-12-30, used by tabs '4' /
    'Новости опубликованные')."""
    if s is None:
        return None
    # Excel serial (number, or numeric string)
    try:
        n = float(s)
        if 40000 < n < 60000:   # ~2009..2064, sane date-serial range
            return (datetime(1899, 12, 30, tzinfo=timezone.utc)
                    + timedelta(days=n))
    except (ValueError, TypeError):
        pass
    s = str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[: len(fmt) + 4].strip(), fmt).replace(
                tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def load_archive(svc):
    """Return (all_entries: list[Item], section_by_urlkey: dict, dated: list[(dt,Item)])."""
    rows = svc.spreadsheets().values().get(
        spreadsheetId=SHEET, range=f"'{PUB_TAB}'!A1:R6000",
        valueRenderOption="UNFORMATTED_VALUE").execute().get("values", [])

    def c(r, i):
        return str(r[i]).strip() if len(r) > i and r[i] is not None else ""

    entries, sec_by_key, dated = [], {}, []
    for r in rows[1:]:
        en, ru, sec, url, dt = c(r, 3), c(r, 4), c(r, 0), c(r, 11), _parse_date(c(r, 5))
        if not (en or ru):
            continue
        it = Item(title=en or ru, title_alt=ru if en else "", section=sec, url=url)
        entries.append(it)
        if url.startswith("http"):
            sec_by_key[url_key(url)] = sec
        if dt:
            dated.append((dt, it))
    return entries, sec_by_key, dated


def load_cache(since: datetime, until: datetime):
    """Our side: (collection_all, accepted, rejected_content) as Item lists.
    collection_all = wider window (lead/lag); accepted/rejected = the window."""
    con = sqlite3.connect(str(SQLITE_PATH))
    wide = (since - timedelta(days=14)).isoformat()
    rows = con.execute(
        "SELECT canonical_url, title, first_seen_at, cached_row_json "
        "FROM seen_articles WHERE first_seen_at >= ? AND cached_row_json IS NOT NULL",
        (wide,)).fetchall()
    con.close()
    lo, hi = since.isoformat(), until.isoformat()
    collection, accepted, rejected = [], [], []
    for url, title, seen, cj in rows:
        try:
            d = json.loads(cj)
        except Exception:
            continue
        it = Item(title=title or "", url=url or "", section=d.get("llm_section", ""))
        collection.append(it)                       # wider window
        if not (lo <= (seen or "") <= hi):
            continue
        v = d.get("verdict", "")
        if v in ACCEPT:
            accepted.append(it)
        elif classify_outcome(v).stage in (S3_HEURISTIC, S4_LLM):
            rejected.append(it)
    return collection, accepted, rejected


def _pct(r):
    return f"{r*100:.0f}%"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--since", default="")
    ap.add_argument("--until", default="")
    args = ap.parse_args()

    until = (_parse_date(args.until) if args.until
             else datetime.now(timezone.utc))
    since = (_parse_date(args.since) if args.since
             else until - timedelta(days=args.days))

    print(f"=== WEEKLY KPI — {since.date()} .. {until.date()} ===\n"
          "Honest strict matching (url + brand-gated fuzzy title >=85). "
          "NOT the retired token-blob method.\n")

    svc = _svc()
    archive_entries, sec_by_key, dated = load_archive(svc)
    collection, accepted, rejected = load_cache(since, until)

    # editor pubs THIS WEEK (coverage denominator)
    week_pubs = [it for dt, it in dated if since <= dt <= until]
    coll_idx = build_index(collection)
    arch_idx = build_index(archive_entries)

    cov = coverage(week_pubs, coll_idx)
    ps = precision_and_section(accepted, arch_idx, sec_by_key)
    rj = reject_right(rejected, arch_idx)

    print(f"inputs: editor pubs this week={len(week_pubs)}, our collection "
          f"(wide)={len(collection)}, accepted={len(accepted)}, "
          f"rejected(content)={len(rejected)}, archive={len(archive_entries)}\n")

    def line(name, m, extra=""):
        print(f"  {name:16} {_pct(m['rate']):>5}  ({m['hit']}/{m['total']}) {extra}")

    print("METRICS (honest):")
    line("1 coverage", cov,
         f"[url {cov['by_url']} + fuzzy {cov['by_fuzzy']}]")
    line("2 found_right", ps["found_right"],
         f"[url {ps['found_right']['by_url']} + fuzzy {ps['found_right']['by_fuzzy']}]")
    line("3 section_right", ps["section_right"])
    line("4 reject_right", rj,
         f"PROXY; false-rejects caught: {rj['false_rejects']}")

    print("\n— caveats —")
    print("  · coverage/found rest partly on cross-language fuzzy title match")
    print("    (imperfect) — the url part is exact. Sample sizes are small;")
    print("    treat single-week moves cautiously.")
    print("  · reject_right is a PROXY ('editor didn't publish it' ≠ 'editor")
    print("    confirmed junk'). The precise version needs the editor's да/нет")
    print("    labelling (рейтинг лист 'Разметка отклонённого').")
    if rj["false_reject_examples"]:
        print("  · bot wrongly rejected (editor DID publish) — recall holes:")
        for t in rj["false_reject_examples"][:5]:
            print(f"      - {t}")

    out = {
        "since": since.date().isoformat(), "until": until.date().isoformat(),
        "coverage": cov, "found_right": ps["found_right"],
        "section_right": ps["section_right"], "reject_right": rj,
    }
    path = DATA / f"weekly_kpi_{until.date()}.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nsaved → {path.name} (compare week-over-week with the prior file).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
