"""Miss-funnel — trace each editor publication through our pipeline to
find WHERE it died (peer-review §3, Week1-C).

For every story the editor published in a window, decide its death stage:
  S1     source not in our 338-source list
  S2     source present, we never collected it
  S3     collected, a heuristic killed it   (with sub-cause + phrase)
  S4     collected, the LLM rejected it
  S0/S5  we accepted it (reached table, or lost downstream in clustering)

Inputs (read-only):
  * editor publications  — 'Опубликованные (все)' tab (title, date, Outer Link)
  * our collected set    — data/news_agent.sqlite (every scored article)
  * the 338 sources      — 'Источники (для РУ)' tab

Usage:
  python scripts/miss_funnel.py                 # last 14 days
  python scripts/miss_funnel.py --days 30
  python scripts/miss_funnel.py --days 14 --dump # also write JSONL of rows

This is a DIAGNOSIS tool — it changes nothing. It prints an honest map of
where recall leaks, plus the caveats that bound each number.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

import sqlite3  # noqa: E402

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

from news_agent.core.miss_funnel import (  # noqa: E402
    ACCEPTED,
    S1,
    S2,
    S3,
    S4,
    Collected,
    EditorPub,
    build_funnel,
    summarise,
)
from news_agent.core.urls import domain_of  # noqa: E402

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs"
)
PUB_TAB = "Опубликованные (все)"
SRC_TAB = "Источники (для РУ)"
SQLITE_PATH = DATA / "news_agent.sqlite"

# Order + human labels for the report.
STAGE_LABELS = [
    (S1, "source not in our 338 list"),
    (S2, "source present, never collected"),
    (S3, "collected, killed by a heuristic"),
    (S4, "collected, rejected by the LLM"),
    (ACCEPTED, "accepted by us (reached table / lost downstream)"),
]


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def _parse_date(s: str) -> datetime | None:
    """Parse the editor's 'Начало активности' (e.g. '2026-06-04 17:50')."""
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[: len(fmt) + 2].strip(), fmt).replace(
                tzinfo=timezone.utc)
        except ValueError:
            continue
    # ISO with T
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_source_domains(svc) -> set[str]:
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{SRC_TAB}'!A1:F2000",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])
    domains: set[str] = set()
    for r in rows[1:]:
        if len(r) < 2:
            continue
        url = str(r[1]).strip()
        if not url.startswith("http"):
            continue
        d = domain_of(url)
        if d:
            domains.add(d)
    return domains


def load_editor_pubs(svc, start: datetime, end: datetime) -> tuple[list[EditorPub], int]:
    """Return (pubs in window, total-rows-scanned). Columns:
    A Раздел | D Название | E Заголовок локализованный | F Начало активности
    | L Outer Link."""
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{PUB_TAB}'!A1:R6000",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])
    pubs: list[EditorPub] = []
    scanned = 0
    for r in rows[1:]:  # skip header
        def cell(i: int) -> str:
            return str(r[i]).strip() if len(r) > i and r[i] is not None else ""
        section = cell(0)
        title_en = cell(3)
        title_ru = cell(4)
        date_s = cell(5)
        url = cell(11)
        if not (title_en or title_ru):
            continue
        scanned += 1
        dt = _parse_date(date_s)
        if dt is None or not (start <= dt <= end):
            continue
        pubs.append(EditorPub(title_en, title_ru, url, date_s, section))
    return pubs, scanned


def load_collected(start: datetime, end: datetime) -> list[Collected]:
    """Every article we scored whose first_seen falls in the (widened)
    window. Verdict comes from cached_row_json."""
    if not SQLITE_PATH.exists():
        return []
    con = sqlite3.connect(str(SQLITE_PATH))
    cur = con.cursor()
    cur.execute(
        "SELECT canonical_url, source_domain, title, first_seen_at, "
        "cached_row_json FROM seen_articles WHERE first_seen_at IS NOT NULL"
    )
    out: list[Collected] = []
    lo, hi = start.isoformat(), end.isoformat()
    for canon, dom, title, seen, cj in cur.fetchall():
        if seen is None or not (lo <= seen <= hi):
            continue
        verdict = ""
        if cj:
            try:
                verdict = json.loads(cj).get("verdict", "")
            except Exception:
                verdict = ""
        out.append(Collected(canon or "", dom or "", title or "",
                             seen or "", verdict))
    con.close()
    return out


def _pct(n: int, total: int) -> str:
    return f"{100*n/total:4.1f}%" if total else "  0.0%"


def main() -> int:
    ap = argparse.ArgumentParser(description="Recall miss-funnel (diagnosis)")
    ap.add_argument("--days", type=int, default=14,
                    help="window: editor pubs in the last N days (default 14)")
    ap.add_argument("--threshold", type=float, default=85.0,
                    help="fuzzy title-match cutoff 0-100 (default 85)")
    ap.add_argument("--dump", action="store_true",
                    help="write the classified rows to data/miss_funnel_<end>.jsonl")
    ap.add_argument("--examples", type=int, default=4,
                    help="examples to print per stage (default 4)")
    args = ap.parse_args()

    # Window is anchored to the latest collected date, not wall-clock, so the
    # tool is reproducible regardless of when it's run.
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    # collected is widened: we usually collect BEFORE the editor publishes
    col_start = start - timedelta(days=14)
    col_end = end + timedelta(days=2)

    print(f"=== Miss-funnel: editor pubs {start.date()} .. {end.date()} "
          f"({args.days}d) ===\n")

    svc = _svc()
    print("Loading source list, editor publications, collected set …")
    source_domains = load_source_domains(svc)
    pubs, scanned = load_editor_pubs(svc, start, end)
    collected = load_collected(col_start, col_end)
    print(f"  sources in list:   {len(source_domains)} domains")
    print(f"  editor pubs:       {len(pubs)} in window "
          f"({scanned} total rows had a title)")
    print(f"  collected (scored): {len(collected)} in "
          f"[{col_start.date()} .. {col_end.date()}]\n")

    if not pubs:
        print("No editor publications in window — widen --days or check the "
              "'Начало активности' date column.")
        return 0
    if not collected:
        print("No collected rows in window — is data/news_agent.sqlite the "
              "current cache?")
        return 0

    rows = build_funnel(pubs, collected, source_domains,
                        threshold=args.threshold)
    s = summarise(rows)
    total = s["total"]

    # ── Funnel table ────────────────────────────────────────────────
    print("STAGE   COUNT   SHARE   WHAT IT MEANS")
    for code, label in STAGE_LABELS:
        n = s["stages"].get(code, 0)
        print(f"  {code:6} {n:4}   {_pct(n, total)}   {label}")
    coverage_miss = s["stages"].get(S1, 0) + s["stages"].get(S2, 0)
    print(f"\n  COVERAGE MISS (S1+S2): {coverage_miss}/{total} "
          f"({_pct(coverage_miss, total)}) — never collected")
    classif_miss = s["stages"].get(S3, 0) + s["stages"].get(S4, 0)
    print(f"  CLASSIFIER MISS (S3+S4): {classif_miss}/{total} "
          f"({_pct(classif_miss, total)}) — collected but killed")

    # ── S3 breakdown (which heuristic / phrase) ─────────────────────
    if s["s3_causes"]:
        print("\nS3 — collected but killed by a heuristic, by cause:")
        for cause, n in sorted(s["s3_causes"].items(), key=lambda x: -x[1]):
            print(f"    {n:3}  {cause}")

    # ── S2 breakdown (which sources we fail to crawl) ───────────────
    if s["s2_domains"]:
        print("\nS2 — source in our list but article not collected, top domains:")
        for dom, n in sorted(s["s2_domains"].items(),
                             key=lambda x: -x[1])[:12]:
            print(f"    {n:3}  {dom}")

    # ── S1 breakdown (sources to consider adding) ───────────────────
    if s["s1_domains"]:
        print("\nS1 — editor's source domain NOT in our list, top domains:")
        for dom, n in sorted(s["s1_domains"].items(),
                             key=lambda x: -x[1])[:12]:
            print(f"    {n:3}  {dom}")

    # ── match-method honesty ────────────────────────────────────────
    print(f"\nMatch method: url={s['methods'].get('url', 0)}  "
          f"fuzzy={s['methods'].get('fuzzy', 0)}  "
          f"none={s['methods'].get('none', 0)}")

    # Self-validation: for UNMATCHED pubs, how close was the best (sub-
    # threshold) candidate? Many near-misses (70-84) would mean S2 is
    # inflated by match failures; mostly <70 means S2 is real coverage gaps.
    none_scores = [r.score for r in rows if r.match_method == "none"]
    if none_scores:
        b = {"0-49": 0, "50-69": 0, "70-84": 0}
        for sc in none_scores:
            if sc < 50:
                b["0-49"] += 1
            elif sc < 70:
                b["50-69"] += 1
            else:
                b["70-84"] += 1
        nn = len(none_scores)
        near = b["70-84"]
        print(f"  unmatched best-score: <50={b['0-49']}  50-69={b['50-69']}  "
              f"70-84={near}  (of {nn})")
        print(f"  → {_pct(b['0-49'] + b['50-69'], nn)} of unmatched have NO "
              f"close candidate (best <70) = real coverage gaps; "
              f"{near} are near-misses (S2 may be slightly over-counted).")

    # ── examples per stage ──────────────────────────────────────────
    by_stage: dict[str, list] = {}
    for r in rows:
        by_stage.setdefault(r.stage, []).append(r)
    for code, label in STAGE_LABELS:
        ex = by_stage.get(code, [])[: args.examples]
        if not ex:
            continue
        print(f"\n  e.g. {code} ({label}):")
        for r in ex:
            extra = (f" ~{r.score:.0f} «{r.matched_title[:40]}»"
                     if r.match_method == "fuzzy" else
                     (f" [{r.cause}]" if r.cause else ""))
            print(f"    · {r.pub.display_title[:60]}{extra}")

    # ── honest caveats ──────────────────────────────────────────────
    print("\n— Caveats (read before trusting the numbers) —")
    print("  · S1 uses the editor's Outer Link domain; they often link a")
    print("    PRIMARY (moex.com etc.) we'd reach via an aggregator, so S1")
    print("    OVER-states source gaps. Treat as an upper bound.")
    print("  · Cross-language title match is imperfect; a missed match")
    print("    inflates S2. Check 'none' count vs fuzzy above — if 'none'")
    print("    is large, some S2 are really mis-matches, not true misses.")
    print("  · S0 vs S5 is not split — ACCEPTED means our classifier said")
    print("    publish (reached table OR lost in clustering). Needs push")
    print("    history to split (deferred).")

    if args.dump:
        out_path = DATA / f"miss_funnel_{end.date()}.jsonl"
        with out_path.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps({
                    "stage": r.stage, "cause": r.cause,
                    "method": r.match_method, "score": round(r.score, 1),
                    "title": r.pub.display_title[:200],
                    "url": r.pub.url, "domain": r.pub.domain,
                    "date": r.pub.date, "section": r.pub.section,
                    "matched_title": r.matched_title[:200],
                }, ensure_ascii=False) + "\n")
        print(f"\nRows written → {out_path.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
