"""Read editor comments from a sheet column. Writes findings to stdout
as UTF-8 (Windows cp1251 terminal would corrupt Cyrillic)."""

from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path

# Force stdout to UTF-8 on Windows (default cp1251 mangles Cyrillic).
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from googleapiclient.discovery import build  # noqa: E402
from google.oauth2 import service_account  # noqa: E402

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
TAB = os.environ.get("FEEDBACK_TAB", "Новости (новые)")

creds = service_account.Credentials.from_service_account_file(
    str(SA_PATH), scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
svc = build("sheets", "v4", credentials=creds)


def col_letter(n: int) -> str:
    out = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def main() -> int:
    # Read full width + all rows
    r = (
        svc.spreadsheets()
        .values()
        .get(
            spreadsheetId=SHEET_ID,
            range=f"'{TAB}'!A1:AZ500",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    vals = r.get("values", [])
    if not vals:
        print(f"Tab {TAB!r} is empty.")
        return 1

    header = vals[0]
    print(f"=== {TAB} ===")
    print(f"Header ({len(header)} cols):")
    for i, h in enumerate(header):
        nonempty = sum(1 for row in vals[1:] if i < len(row) and row[i])
        marker = "← comment col?" if "ом" in str(h).lower() or "review" in str(h).lower() or nonempty < 100 and nonempty > 0 else ""
        print(f"  {col_letter(i+1):>2} ({i:>2}): {h!r:<35}  [{nonempty} nonempty] {marker}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
