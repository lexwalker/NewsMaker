"""Standalone smoke test: can we read & write the ТЕСТ tab?

Does NOT depend on the rest of the package (so it runs before we install
the full stack). Run:  python scripts/smoke_sheets_access.py
"""

from __future__ import annotations

import io
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
TAB = "ТЕСТ"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def main() -> int:
    print(f"Spreadsheet ID: {SHEET_ID}")
    print(f"Service account file: {SA_PATH}  (exists: {SA_PATH.exists()})")
    if not SA_PATH.exists():
        print("  ✗ service_account.json not found", file=sys.stderr)
        return 2

    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    print(f"Service account email: {creds.service_account_email}")

    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # 1. Metadata — does the sheet exist? do we have access?
    try:
        meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    except HttpError as e:
        print(f"  ✗ metadata read failed: HTTP {e.resp.status} — {e.reason}", file=sys.stderr)
        print("    → share the sheet with the service account as Editor", file=sys.stderr)
        return 3

    print(f"\n✓ access OK — '{meta['properties']['title']}'")
    tabs = [s["properties"]["title"] for s in meta["sheets"]]
    print("  existing tabs:")
    for t in tabs:
        marker = " ← target" if t == TAB else ""
        print(f"   • {t}{marker}")

    if TAB not in tabs:
        print(f"\n  ℹ '{TAB}' tab not found. Creating it…")
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TAB}}}]},
        ).execute()
        print(f"  ✓ tab '{TAB}' created")

    # 2. Read current contents
    vals = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"'{TAB}'")
        .execute()
        .get("values", [])
    )
    print(f"\n✓ read OK — '{TAB}' has {len(vals)} existing rows")

    # 3. Append a test row
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    row = [now_iso, "smoke test", "read+write OK", "news_agent"]
    try:
        resp = (
            svc.spreadsheets()
            .values()
            .append(
                spreadsheetId=SHEET_ID,
                range=f"'{TAB}'!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            )
            .execute()
        )
    except HttpError as e:
        print(f"  ✗ write failed: HTTP {e.resp.status} — {e.reason}", file=sys.stderr)
        return 4

    updated_range = resp.get("updates", {}).get("updatedRange", "?")
    print(f"✓ write OK — appended to {updated_range}")
    print(f"  row contents: {row}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
