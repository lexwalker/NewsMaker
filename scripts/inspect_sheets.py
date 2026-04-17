"""Inspect key tabs in the spreadsheet: show headers + first few rows."""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

TABS_TO_INSPECT = [
    os.environ["SOURCES_TAB_RU"],
    os.environ["NEWS_TAB"],
    os.environ["PUBLISHED_NEWS_TAB"],
    os.environ["SECTIONS_TAB"],
]


def main() -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    for tab in TABS_TO_INSPECT:
        print(f"\n========== [{tab}] ==========")
        try:
            vals = (
                svc.spreadsheets()
                .values()
                .get(spreadsheetId=SHEET_ID, range=f"'{tab}'")
                .execute()
                .get("values", [])
            )
        except Exception as e:  # noqa: BLE001
            print(f"  ✗ read failed: {e}")
            continue

        if not vals:
            print("  (empty)")
            continue

        print(f"  rows: {len(vals)} (including header)")
        header = vals[0]
        print(f"  header ({len(header)} cols):")
        for i, h in enumerate(header):
            print(f"    [{i}] {h!r}")

        preview = vals[1:4]
        if preview:
            print(f"  first {len(preview)} data row(s):")
            for j, row in enumerate(preview, start=1):
                cells = [f"{header[i] if i < len(header) else f'col{i}'}={row[i]!r}" for i in range(len(row))]
                print(f"    row{j}: " + " | ".join(cells))
    return 0


if __name__ == "__main__":
    sys.exit(main())
