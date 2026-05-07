"""Backfill 'Стадия запуска' (Q) and 'Бренд + модель' (R) columns in the
persistent 'Новости' sheet by matching rows to a 'ТЕСТ статьи vN' tab via URL.

Useful after the Phase-1 lifecycle columns are added to Новости — old
v22-v28 rows pushed before the columns existed get retroactively tagged.

Usage:
    python scripts/backfill_news_stages.py "ТЕСТ статьи v29"
"""

from __future__ import annotations

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

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

NEWS_TAB = "Новости"

# Indices in 'ТЕСТ статьи vN' (matches batch_fetch_test ARTICLES_HEADER)
COL_TS_URL = 3            # D — article URL
COL_TS_LAUNCH_STAGE = 28  # AC
COL_TS_LAUNCH_BM = 29     # AD

# Indices in 'Новости'
COL_N_PRIMARY_URL = 9   # J — primary URL = our dedup key
COL_N_MEMBER_URLS = 12  # M — newline-separated all member URLs
COL_N_STAGE = 16        # Q — phase 1 stage
COL_N_BRAND_MODEL = 17  # R — phase 1 brand+model


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: backfill_news_stages.py 'ТЕСТ статьи vN'")
        return 2
    tab = sys.argv[1]

    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # Read source: URL → (stage, brand_model)
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A2:AD",
    ).execute()
    src_rows = resp.get("values", []) or []
    url_to_meta: dict[str, tuple[str, str]] = {}
    for r in src_rows:
        url = (r[COL_TS_URL] if len(r) > COL_TS_URL else "").strip()
        stage = (r[COL_TS_LAUNCH_STAGE] if len(r) > COL_TS_LAUNCH_STAGE else "").strip()
        bm = (r[COL_TS_LAUNCH_BM] if len(r) > COL_TS_LAUNCH_BM else "").strip()
        if url and (stage or bm):
            url_to_meta[url] = (stage, bm)
    print(f"Loaded {len(url_to_meta)} URLs with stage/brand-model data from {tab!r}")

    # Read Новости
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A2:R",
    ).execute()
    news_rows = resp.get("values", []) or []
    print(f"Loaded {len(news_rows)} rows from {NEWS_TAB!r}")

    updates: list[dict] = []
    matched = 0
    for ri, r in enumerate(news_rows, start=2):
        primary = (r[COL_N_PRIMARY_URL] if len(r) > COL_N_PRIMARY_URL else "").strip()
        members_raw = (r[COL_N_MEMBER_URLS] if len(r) > COL_N_MEMBER_URLS else "").strip()
        existing_stage = (r[COL_N_STAGE] if len(r) > COL_N_STAGE else "").strip()
        existing_bm = (r[COL_N_BRAND_MODEL] if len(r) > COL_N_BRAND_MODEL else "").strip()

        if existing_stage or existing_bm:
            continue  # already populated, skip

        urls_to_check = [primary] if primary else []
        urls_to_check += [u.strip() for u in members_raw.splitlines() if u.strip()]
        for u in urls_to_check:
            if u in url_to_meta:
                stage, bm = url_to_meta[u]
                updates.append({
                    "range": f"'{NEWS_TAB}'!Q{ri}:R{ri}",
                    "values": [[stage, bm]],
                })
                matched += 1
                break

    print(f"Backfill candidates: {matched}")
    if not updates:
        return 0

    # Apply in chunks (batch can hit limits with thousands of updates)
    CHUNK = 200
    for i in range(0, len(updates), CHUNK):
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": updates[i:i + CHUNK]},
        ).execute()
    print(f"Wrote {len(updates)} cells to {NEWS_TAB!r} (Q + R columns).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
