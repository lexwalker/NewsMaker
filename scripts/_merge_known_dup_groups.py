"""One-shot: merge the 3 high-confidence fuzz-duplicate groups the
may-2026 sheet audit found (editor approved Variant A).

Groups (canonical = first / topmost row):
  r482  ← r489, r495   AvtoVAZ–Promtech agreement  (all Dealer)
  r292  ← r310          GAC S9 in Russia            (all Confirmed)
  r351  ← r376          Kia XCeed facelift          (all Confirmed)

Editor-comment alignment confirmed before running:
  r310 "это дубль, нет"  / r495 "уже несколько раз было"  → editor
  themselves flagged these as dups → hiding matches their intent.
  r482 "это LCV ..." is about the SECTION, not a dup → left as
  canonical, section UNCHANGED (separate feedback, out of scope).

Action per group: merge every member's source URLs into the canonical
col M (deduped, newline-joined), hide the non-canonical rows
(hiddenByUser — reversible), and annotate their col P
"🔁 дубль строки <canon>" while preserving any existing editor text
inline. Sections are never changed. Dry-run unless --apply.
"""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from googleapiclient.discovery import build  # noqa: E402
from google.oauth2 import service_account  # noqa: E402

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs"
)
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
creds = service_account.Credentials.from_service_account_file(
    str(SA_PATH), scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
svc = build("sheets", "v4", credentials=creds)
TAB = "Новости (новые)"
APPLY = "--apply" in sys.argv

# (canonical_row, [duplicate_rows])
GROUPS = [
    (482, [489, 495]),   # r482 earliest; r495 "уже было", r489 already 🔁
    (292, [310]),         # editor: r292 "да" (keep), r310 "это дубль, нет"
    (376, [351]),         # editor: r376 "да" + thekoreancarblog primary;
                          # r351 (ixbt aggregator, no comment) → hide
]


def _sheet_id() -> int:
    meta = svc.spreadsheets().get(
        spreadsheetId=EDITOR, fields="sheets(properties(sheetId,title))"
    ).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == TAB:
            return s["properties"]["sheetId"]
    raise RuntimeError("tab not found")


def main() -> int:
    rows = (
        svc.spreadsheets().values().get(
            spreadsheetId=EDITOR,
            range=f"'{TAB}'!A1:P700",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute().get("values", [])
    )

    def cell(sr: int, c: int) -> str:
        r = rows[sr - 1] if sr - 1 < len(rows) else []
        return str(r[c]) if len(r) > c else ""

    values_batch: list[dict] = []
    hide_reqs: list[dict] = []
    sid = _sheet_id() if APPLY else -1

    for canon, dups in GROUPS:
        members = [canon, *dups]
        # union of all source URLs (col M, fallback col J)
        merged: list[str] = []
        for sr in members:
            blob = cell(sr, 12) or cell(sr, 9)
            for u in blob.replace("\n", " ").split():
                if u and u not in merged:
                    merged.append(u)
        print(f"GROUP canonical r{canon} [{cell(canon, 3)}] "
              f"← {', '.join('r'+str(d) for d in dups)}")
        print(f"  canon title: {cell(canon,1)[:65].replace(chr(10),' || ')}")
        print(f"  merged {len(merged)} source URLs")
        for d in dups:
            ex = cell(d, 15).strip()
            note = f"🔁 дубль строки {canon}"
            if ex and not ex.startswith("🔁"):
                note = f"{note} | ред.коммент: {ex[:140]}"
            print(f"  hide r{d}: P={note[:80]}")
            if APPLY:
                values_batch.append({
                    "range": f"'{TAB}'!P{d}", "values": [[note]],
                })
                hide_reqs.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sid, "dimension": "ROWS",
                            "startIndex": d - 1, "endIndex": d,
                        },
                        "properties": {"hiddenByUser": True},
                        "fields": "hiddenByUser",
                    }
                })
        if APPLY:
            values_batch.append({
                "range": f"'{TAB}'!M{canon}",
                "values": [["\n".join(merged)]],
            })
        print()

    if not APPLY:
        print("(dry-run — pass --apply)")
        return 0

    if values_batch:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=EDITOR,
            body={"valueInputOption": "USER_ENTERED", "data": values_batch},
        ).execute()
    if hide_reqs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=EDITOR, body={"requests": hide_reqs}
        ).execute()
    print(f"APPLIED: {len(GROUPS)} groups, "
          f"{sum(len(d) for _, d in GROUPS)} rows hidden + annotated, "
          f"{len(GROUPS)} canonical URL-merges.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
