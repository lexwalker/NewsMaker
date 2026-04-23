"""Apply out-of-band (Claude session) classifications back to 'ТЕСТ статьи vN'.

Reads all JSON files in ``data/classifications/batch_*.json``, merges them
by ``sheet_row``, and writes:
  • Col B  — "EN: {title_en}\\nRU: {title_ru}"  (combined title cell)
  • Col D  — section
  • Col E  — region
  • Col K  — note
  • Col M  — final verdict ("Точно новость" / "Отклонено LLM")
  • Col X  — LLM relevance ("Да" / "Нет")

Usage:
    python scripts/apply_classifications.py "ТЕСТ статьи v14"
"""

from __future__ import annotations

import glob
import io
import json
import os
import sys
from pathlib import Path

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


def _load_classifications() -> dict[int, dict]:
    """Merge all batch_*.json files into a dict keyed by sheet_row."""
    out: dict[int, dict] = {}
    paths = sorted(glob.glob(str(ROOT / "data" / "classifications" / "batch_*.json")))
    for p in paths:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        for entry in data:
            row = entry.get("sheet_row")
            if row:
                out[row] = entry
    return out


def _combined_title(entry: dict) -> str:
    en = (entry.get("title_en") or "").strip()
    ru = (entry.get("title_ru") or "").strip()
    if en and ru:
        return f"EN: {en[:220]}\nRU: {ru[:220]}"
    return (en or ru)[:400]


def main() -> int:
    tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ статьи v14"
    classifications = _load_classifications()
    print(f"Loaded {len(classifications)} classifications from data/classifications/.")

    svc = _svc()

    # Build per-cell value updates — Sheets batchUpdate values.
    # Column letters reflect the post-"Лид" + "Country" layout:
    # B=Title, C=Лид, D=URL, E=Section, F=Region, G=Country, H=Date,
    # I=Image URL, M=Note, O=Verdict, Z=LLM relevance.
    data_updates: list[dict] = []
    for row, c in classifications.items():
        combined = _combined_title(c)
        section = c.get("section", "") or ""
        region = c.get("region", "") or ""
        note = c.get("note", "") or ""
        verdict = c.get("final_verdict", "") or ""
        relevance = c.get("relevance", "") or ""

        # Col B — combined title
        data_updates.append(
            {"range": f"'{tab}'!B{row}", "values": [[combined]]}
        )
        # Col E — section
        data_updates.append(
            {"range": f"'{tab}'!E{row}", "values": [[section]]}
        )
        # Col F — region
        data_updates.append(
            {"range": f"'{tab}'!F{row}", "values": [[region]]}
        )
        # Col M — note
        data_updates.append(
            {"range": f"'{tab}'!M{row}", "values": [[note]]}
        )
        # Col O — verdict
        data_updates.append(
            {"range": f"'{tab}'!O{row}", "values": [[verdict]]}
        )
        # Col Z — relevance
        data_updates.append(
            {"range": f"'{tab}'!Z{row}", "values": [[relevance]]}
        )

    # Write in chunks (Sheets API limit: 10MB body, 1000 ranges per request is comfortable).
    CHUNK = 500
    total = len(data_updates)
    print(f"Sending {total} cell updates in chunks of {CHUNK}...")
    for i in range(0, total, CHUNK):
        chunk = data_updates[i : i + CHUNK]
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": chunk},
        ).execute()
        print(f"  updated {min(i + CHUNK, total)}/{total}")

    print(f"\nDone. Applied {len(classifications)} classifications to '{tab}'.")
    print(f"Next: run `python scripts/write_stats_panel.py \"{tab}\"` to refresh stats.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
