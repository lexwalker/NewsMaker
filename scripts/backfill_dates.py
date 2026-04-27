"""Fill empty 'Дата публикации' cells in 'Новости' using the new date
extractors (URL pattern + lede text scan).

No re-fetch: we use only what's already in the sheet (canonical URL +
lede column), so the script runs in seconds and costs nothing.

Run:  python scripts/backfill_dates.py
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

from news_agent.adapters.fetchers.html import (  # noqa: E402
    _pick_published_from_text,
    _pick_published_from_url,
)

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

NEWS_TAB = "Новости"
COL_TITLE = 1   # B
COL_LEDE = 2    # C
COL_DATE = 6    # G
COL_PRIMARY_URL = 9  # J
COL_MEMBER_URLS = 12  # M


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(r: list[str], i: int) -> str:
    return r[i] if i < len(r) else ""


def main() -> int:
    svc = _svc()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A2:P"
    ).execute()
    rows = resp.get("values", []) or []
    print(f"Loaded {len(rows)} rows from '{NEWS_TAB}'.")

    updates: list[dict] = []
    filled_url = 0
    filled_text = 0
    for i, r in enumerate(rows, start=2):  # 1-based
        if _get(r, COL_DATE):
            continue  # already has a date
        title = _get(r, COL_TITLE)
        if not title or "EN:" not in title and "Прогон от" in title:
            continue  # separator row
        # Try every URL we know — primary + members — for URL-pattern dates
        candidate_urls: list[str] = []
        primary = _get(r, COL_PRIMARY_URL)
        if primary:
            candidate_urls.append(primary)
        for u in _get(r, COL_MEMBER_URLS).splitlines():
            u = u.strip()
            if u and u not in candidate_urls:
                candidate_urls.append(u)
        dt = None
        for u in candidate_urls:
            dt = _pick_published_from_url(u)
            if dt:
                filled_url += 1
                break
        if dt is None:
            lede = _get(r, COL_LEDE)
            dt = _pick_published_from_text(title, lede)
            if dt:
                filled_text += 1
        if dt is None:
            continue
        # Write ISO 8601 string to keep the sheet's existing format
        iso = dt.isoformat()
        updates.append({
            "range": f"'{NEWS_TAB}'!G{i}",
            "values": [[iso]],
        })

    print(f"Filled from URL pattern: {filled_url}")
    print(f"Filled from lede text:   {filled_text}")
    print(f"Total to update: {len(updates)}")

    if not updates:
        return 0

    CHUNK = 200
    for i in range(0, len(updates), CHUNK):
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": updates[i:i + CHUNK]},
        ).execute()
    print(f"Wrote {len(updates)} date cells.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
