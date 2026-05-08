"""Backfill 'Обоснование LLM' (col S) directly on the persistent Новости
sheet by calling editorial_review on each row's title + lede.

Bypasses the cluster-builder roundtrip — useful when historical rows
predate the editorial_review architecture and we want to add reasons
without re-running the full pipeline.

Usage:
    python scripts/backfill_news_reasons_direct.py [--limit N] [--sheet SHEET_ID] [--tab TAB]

Defaults:
    --limit 100         (top-N rows; sheet has newest at top)
    --sheet from .env   SPREADSHEET_ID
    --tab "Новости"     (or whatever NEWS_TAB is)

Cost: ~$0.002 per row (Anthropic Haiku with prompt caching).
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.core.budget import BudgetTracker  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Сolumn indices in Новости (новые) header
COL_TITLE = 1   # B
COL_LEDE = 2    # C
COL_REASON = 18  # S


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=100,
                   help="Number of top rows to scan (newest first). Default 100.")
    p.add_argument("--sheet", default=os.environ.get("SPREADSHEET_ID"),
                   help="Spreadsheet ID. Default: $SPREADSHEET_ID")
    p.add_argument("--tab", default="Новости",
                   help="Tab name. Default: 'Новости'")
    args = p.parse_args()

    settings = get_settings()
    sections = load_sections()
    budget = BudgetTracker(getattr(settings, "max_cost_usd", 5.0))
    client = make_llm_client(settings)
    print(f"  provider: {client.provider_name}  model: {client.model}")
    print(f"  target: spreadsheet={args.sheet}, tab='{args.tab}'")

    sa_path = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = Credentials.from_service_account_file(str(sa_path), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # Read top N+separator rows (separator rows interleave with data)
    end_row = args.limit + 50  # extra buffer for separators
    resp = svc.spreadsheets().values().get(
        spreadsheetId=args.sheet, range=f"'{args.tab}'!A2:S{end_row}",
    ).execute()
    rows = resp.get("values", []) or []

    targets: list[tuple[int, str, str]] = []
    data_seen = 0
    for ri, r in enumerate(rows, start=2):
        if not r or len(r) < 3:
            continue
        title = r[COL_TITLE] if len(r) > COL_TITLE else ""
        if title.startswith("━━"):
            continue  # run separator
        if not title.strip():
            continue
        data_seen += 1
        if data_seen > args.limit:
            break
        existing = r[COL_REASON] if len(r) > COL_REASON else ""
        if existing.strip():
            continue
        body = r[COL_LEDE] if len(r) > COL_LEDE else ""
        if not body.strip():
            continue
        targets.append((ri, title, body))

    print(f"Scanned {data_seen} data rows, found {len(targets)} missing reason.")
    if not targets:
        return 0

    # Strip "EN: ... \n RU: ..." prefix to feed LLM the source title.
    def _clean_title(t: str) -> str:
        if "EN:" in t:
            for line in t.splitlines():
                ll = line.strip()
                if ll.lower().startswith("en:"):
                    return ll[3:].strip()
        return t

    updates: list[dict] = []
    for idx, (ri, title, body) in enumerate(targets, start=1):
        try:
            review, u = client.editorial_review(
                title=_clean_title(title),
                body=body,
                sections=sections,
                portal_country="Russia",
            )
            budget.record(u)
        except Exception as e:  # noqa: BLE001
            print(f"  [{idx}/{len(targets)}] FAILED row {ri}: {e!s:80}")
            continue
        reason = (review.reason or "").strip()[:300]
        if reason:
            updates.append({
                "range": f"'{args.tab}'!S{ri}",
                "values": [[reason]],
            })
            if idx % 10 == 0 or idx == len(targets):
                print(f"  [{idx}/{len(targets)}] row {ri}: {reason[:60]}  (run total ${budget.spent_usd:.4f})")

    if not updates:
        print("Nothing to write.")
        return 0

    CHUNK = 200
    for i in range(0, len(updates), CHUNK):
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=args.sheet,
            body={"valueInputOption": "USER_ENTERED", "data": updates[i:i + CHUNK]},
        ).execute()
    print(f"\nApplied {len(updates)} reason updates. Total cost: ${budget.spent_usd:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
