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

from news_agent.core.editor_feedback import precision_from_feedback  # noqa: E402
from news_agent.core.published_dedup import url_key  # noqa: E402
from news_agent.core.sheet_dates import parse_sheet_date as _parse_date  # noqa: E402
from news_agent.core.reject_stage import (  # noqa: E402
    S3_HEURISTIC, S4_LLM, classify_outcome,
)
from news_agent.core.weekly_kpi import (  # noqa: E402
    Item, active_collection_days, build_index, coverage_by_day,
    coverage_day_aligned, precision_and_section, reject_right,
)

# The published archive + the editor feed live ONLY in the editor's
# original spreadsheet (EDITOR_SPREADSHEET_ID = 1fQic…), NOT in the working
# copy (SPREADSHEET_ID = 14PTb…, which has no "Опубликованные (все)" tab).
SHEET = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
PUB_TAB = "Опубликованные (все)"
SQLITE_PATH = DATA / "news_agent.sqlite"
EVAL_V2 = DATA / "eval_set_v2.jsonl"
ACCEPT = {"Точно новость", "Возможно новость"}


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def load_archive(svc):
    """Return (all_entries: list[Item], section_by_urlkey: dict, dated: list[(dt,Item)])."""
    rows = svc.spreadsheets().values().get(
        spreadsheetId=SHEET, range=f"'{PUB_TAB}'!A:R",
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
    """Our side: (collection_all, accepted, rejected_content, coll_dates).
    collection_all = wider window (lead/lag) used for matching;
    accepted/rejected = the window; coll_dates = first_seen DATES inside the
    window (one per article, for detecting which days a prog actually ran)."""
    con = sqlite3.connect(str(SQLITE_PATH))
    wide = (since - timedelta(days=14)).isoformat()
    rows = con.execute(
        "SELECT canonical_url, title, first_seen_at, cached_row_json "
        "FROM seen_articles WHERE first_seen_at >= ? AND cached_row_json IS NOT NULL",
        (wide,)).fetchall()
    con.close()
    lo, hi = since.isoformat(), until.isoformat()
    collection, accepted, rejected, coll_dates = [], [], [], []
    for url, title, seen, cj in rows:
        try:
            d = json.loads(cj)
        except Exception:
            continue
        it = Item(title=title or "", url=url or "", section=d.get("llm_section", ""))
        collection.append(it)                       # wider window
        if not (lo <= (seen or "") <= hi):
            continue
        dt = _parse_date(seen)
        if dt:
            coll_dates.append(dt.date())             # which days a prog ran
        v = d.get("verdict", "")
        if v in ACCEPT:
            accepted.append(it)
        elif classify_outcome(v).stage in (S3_HEURISTIC, S4_LLM):
            rejected.append(it)
    return collection, accepted, rejected, coll_dates


def load_editor_precision(since: datetime, until: datetime) -> dict:
    """Editor-comment precision over feedback synced in the window. Reads
    data/eval_set_v2.jsonl (provenance == 'editor_sheet'). Falls back to
    all-time if no feedback landed in the window (editor reviews in bursts)."""
    if not EVAL_V2.exists():
        return {"total": 0, "rate": 0.0, "by_type": {}, "is_biased": True,
                "scope": "no-data"}
    rows = []
    for ln in EVAL_V2.read_text(encoding="utf-8").splitlines():
        if not ln.strip():
            continue
        try:
            rows.append(json.loads(ln))
        except (json.JSONDecodeError, ValueError):
            continue
    fb = [r for r in rows if r.get("provenance") == "editor_sheet"]
    lo, hi = since.isoformat(), until.isoformat()
    windowed = [r for r in fb if lo <= r.get("synced_at", "") <= hi]
    scope = "window"
    if not windowed:                 # editor hadn't reviewed inside the window
        windowed, scope = fb, "all-time"
    p = precision_from_feedback(windowed)
    p["scope"] = scope
    return p


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
    # '--until 2026-06-16' parses to midnight, which would EXCLUDE that day's
    # publications. The intent is "through the 16th", so extend to end-of-day.
    if args.until and until and until.time() == datetime.min.time():
        until = until.replace(hour=23, minute=59, second=59)

    print(f"=== WEEKLY KPI — {since.date()} .. {until.date()} ===\n"
          "Honest strict matching (url + brand-gated fuzzy title >=85). "
          "NOT the retired token-blob method.\n")

    svc = _svc()
    archive_entries, sec_by_key, dated = load_archive(svc)
    collection, accepted, rejected, coll_dates = load_cache(since, until)

    # editor pubs THIS WEEK, kept WITH their date for day-alignment
    dated_pubs = [(dt.date(), it) for dt, it in dated if since <= dt <= until]
    coll_idx = build_index(collection)
    arch_idx = build_index(archive_entries)

    # Coverage is counted only over days a prog actually ran (active days);
    # no-prog days are an uptime gap, reported separately — NOT a coverage
    # failure. This is the day-aligned method the editor asked for.
    active = active_collection_days(coll_dates)
    cov = coverage_day_aligned(dated_pubs, active, coll_idx)
    by_day = coverage_by_day(dated_pubs, active, coll_idx)
    total_days = (until.date() - since.date()).days + 1

    prec = load_editor_precision(since, until)        # headline precision
    ps = precision_and_section(accepted, arch_idx, sec_by_key)  # lower bound
    rj = reject_right(rejected, arch_idx)

    print(f"inputs: editor pubs in window={len(dated_pubs)}, our collection "
          f"(wide)={len(collection)}, accepted={len(accepted)}, "
          f"rejected(content)={len(rejected)}, archive={len(archive_entries)}\n")

    def line(name, m, extra=""):
        print(f"  {name:18} {_pct(m['rate']):>5}  ({m['hit']}/{m['total']}) {extra}")

    print("METRICS (honest):")
    line("1 coverage*", cov,
         f"[url {cov['by_url']} + fuzzy {cov['by_fuzzy']}]")
    print(f"     *day-aligned: prog ran {cov['active_days']}/{total_days} days; "
          f"{cov['excluded_no_prog']} pubs on no-prog days excluded (uptime gap)")
    line(f"2 precision ({prec['scope']})", prec,
         "← editor comments (headline)")
    if prec["by_type"]:
        bt = ", ".join(f"{k} {v}" for k, v in prec["by_type"].items())
        print(f"       complaints: {bt}")
    line("  found_right", ps["found_right"],
         "← archive-match LOWER BOUND (not the headline)")
    line("3 section_right", ps["section_right"])
    line("4 reject_right", rj,
         f"PROXY; false-rejects: {rj['false_rejects']}")

    # ── per-day coverage (shows exactly where the prog gaps are) ──
    print("\nCOVERAGE BY DAY (only prog days count toward the headline):")
    print(f"  {'date':12} {'pubs':>5} {'found':>6} {'rate':>6}  prog?")
    for r in by_day:
        if r["prog"]:
            print(f"  {r['date']:12} {r['pubs']:>5} {r['found']:>6} "
                  f"{_pct(r['rate']):>6}  yes")
        else:
            print(f"  {r['date']:12} {r['pubs']:>5} {'—':>6} {'—':>6}  NO PROG")

    print("\n— caveats —")
    print("  · coverage rests partly on cross-language fuzzy title match")
    print("    (imperfect) — the url part is exact. Small samples; treat")
    print("    single-week moves cautiously.")
    print("  · precision = editor comments; editor flags PROBLEM rows and")
    print("    passes clean ones silently → this is a pessimistic FLOOR.")
    print(f"    (scope={prec['scope']}: 'all-time' means no feedback landed")
    print("    inside the window yet — editor reviews in bursts.)")
    print("  · found_right (archive match) is a strict LOWER bound, not the")
    print("    'правильность' headline — it needs the editor to have published")
    print("    the exact story, title-matchable. Use precision above instead.")
    print("  · reject_right is a PROXY ('editor didn't publish' ≠ 'confirmed")
    print("    junk'). Precise version needs the да/нет labelling ritual.")
    if rj["false_reject_examples"]:
        print("  · bot wrongly rejected (editor DID publish) — recall holes:")
        for t in rj["false_reject_examples"][:5]:
            print(f"      - {t}")

    out = {
        "since": since.date().isoformat(), "until": until.date().isoformat(),
        "coverage": cov, "coverage_by_day": by_day,
        "precision_editor": prec, "found_right": ps["found_right"],
        "section_right": ps["section_right"], "reject_right": rj,
    }
    path = DATA / f"weekly_kpi_{until.date()}.json"
    path.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"\nsaved → {path.name} (compare week-over-week with the prior file).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
