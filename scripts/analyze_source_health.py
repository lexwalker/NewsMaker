"""Read 'ТЕСТ прогон vN' and summarise source availability.

Breaks failures down into actionable buckets:
  • HTTP 403 / 401 — blocked by bot protection (Cloudflare, Akamai)
  • HTTP 404 / 410 — URL rot, needs replacement
  • HTTP 5xx       — server problems (transient)
  • timeout / connect error — network / DNS / site down
  • OK but 0 articles — we reach the site but extract nothing
  • OK with articles — healthy

Run:  python scripts/analyze_source_health.py [tab_name]
"""

from __future__ import annotations

import io
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# HEADER from batch_fetch_test.py:
# A run_ts | B idx | C url | D detected_type | E feed_url | F http_status
# G articles_attempted | H articles_with_title | I articles_with_body |
# J articles_with_date | K articles_with_image | L news_like |
# M passed_is_article | N passed_auto_topic | O elapsed_ms | P error |
# Q sample_titles | R sample_passed
COL_URL = 2
COL_TYPE = 3
COL_FEED = 4
COL_HTTP = 5
COL_ATTEMPTED = 6
COL_WITH_BODY = 8
COL_NEWS_LIKE = 11
COL_PASSED_AUTO = 13
COL_ELAPSED = 14
COL_ERROR = 15


def _cell(row: list[str], i: int) -> str:
    return row[i] if i < len(row) else ""


def _bucket(row: list[str]) -> str:
    status = _cell(row, COL_HTTP).strip()
    err = _cell(row, COL_ERROR).lower()
    attempted = _cell(row, COL_ATTEMPTED).strip()
    passed_auto = _cell(row, COL_PASSED_AUTO).strip()

    if status in {"403", "401"}:
        return "🚫 blocked (403/401 — Cloudflare/WAF)"
    if status in {"404", "410"}:
        return "💀 URL rot (404/410)"
    if status and status.isdigit() and 500 <= int(status) < 600:
        return "🔥 server 5xx"
    if "timeout" in err or "timed out" in err:
        return "⏱ timeout"
    if "connecterror" in err or "connect error" in err or "nameresolution" in err or "dns" in err:
        return "📡 connect/DNS error"
    if err and not status:
        return f"❓ other error ({err[:40]})"

    # Reached the site.
    if status == "200" or (not status and not err):
        if attempted in {"", "0"}:
            return "🕳 reachable, 0 articles found"
        if passed_auto in {"", "0"}:
            return "🔍 reachable, 0 auto-topic hits"
        return "✅ healthy"

    return f"? status={status!r} err={err[:30]!r}"


def main() -> int:
    tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ прогон v11"
    svc = _svc()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A2:R"
    ).execute()
    rows = resp.get("values", []) or []
    total = len(rows)
    print(f"Sources in '{tab}': {total}")
    print()

    buckets: Counter[str] = Counter()
    bucket_urls: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for r in rows:
        b = _bucket(r)
        buckets[b] += 1
        url = _cell(r, COL_URL)
        status = _cell(r, COL_HTTP)
        err = _cell(r, COL_ERROR)
        bucket_urls[b].append((url, f"{status} | {err[:60]}"))

    print("=== Breakdown ===")
    for b, n in buckets.most_common():
        pct = f"{n / total * 100:.1f}%"
        print(f"  {b:55}  {n:>3}  ({pct})")

    # Domain-level view of problem buckets
    print("\n=== Problem domains ===")
    problem_buckets = [b for b in buckets if not b.startswith("✅")]
    for b in problem_buckets:
        if buckets[b] == 0:
            continue
        print(f"\n--- {b}  (n={buckets[b]}) ---")
        domains: Counter[str] = Counter()
        for url, _info in bucket_urls[b]:
            try:
                dom = urlparse(url).netloc.lower()
            except Exception:  # noqa: BLE001
                dom = "?"
            domains[dom] += 1
        for url, info in bucket_urls[b][:30]:
            print(f"    {info:30}  {url[:80]}")
        if buckets[b] > 30:
            print(f"    ... and {buckets[b] - 30} more")

    return 0


if __name__ == "__main__":
    sys.exit(main())
