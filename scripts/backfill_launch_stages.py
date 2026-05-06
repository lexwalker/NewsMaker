"""Apply launch-stage + brand+model detection to an existing 'ТЕСТ статьи vN'
tab and write results to columns AC/AD.

Useful for:
  - Running the latest heuristic on a prior run without redoing fetch+LLM
  - Validating new stage phrases against historical data

Usage:
    python scripts/backfill_launch_stages.py "ТЕСТ статьи v28"

By default detects on the combined sheet title (EN + RU LLM translations);
this gives broader coverage than the original raw title since the LLM
often surfaces stage signals in either language.
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

from news_agent.core.config_loader import load_brand_domains  # noqa: E402
from news_agent.core.launch_stages import (  # noqa: E402
    detect_launch_stages,
    extract_brand_model,
)

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: backfill_launch_stages.py 'ТЕСТ статьи vN'")
        return 2
    tab = sys.argv[1]
    brands = load_brand_domains()

    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A2:AD",
    ).execute()
    rows = resp.get("values", []) or []
    print(f"Loaded {len(rows)} data rows from {tab!r}")

    updates: list[dict] = []
    hits = 0
    for ri, r in enumerate(rows, start=2):
        title = r[1] if len(r) > 1 else ""
        if not title.strip():
            continue
        stages = detect_launch_stages(title)
        bm = extract_brand_model(title, brands)
        if stages and bm:
            stage_str = ", ".join(stages)
            bm_str = f"{bm[0]} {bm[1]}"
            updates.append({
                "range": f"'{tab}'!AC{ri}:AD{ri}",
                "values": [[stage_str, bm_str]],
            })
            hits += 1
            if hits <= 30:
                preview = title.split("\n")[0][:90]
                print(f"  Row {ri}: [{stage_str}] [{bm_str}]")
                print(f"     {preview}")
    print(f"\nTotal launch detections: {hits}")
    if not updates:
        print("Nothing to backfill.")
        return 0

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "USER_ENTERED", "data": updates},
    ).execute()
    print(f"Wrote {len(updates)} rows to AC/AD of {tab!r}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
