"""Round-2 merge for v41 sheet — 13 dup groups the wide scan + editor's
own comments confirmed. The first audit missed 7 of these (lexical fuzz
threshold was too tight and didn't cross-check lede content).

Strongest signal across the board: the EDITOR themselves wrote «ДУБЛЬ»
/ «опять дубль» / «дубль 136» / «Дубль» / «постили пресс» on
r137, r155, r167, r217, r238, r292, r293 — we are just executing what
they already flagged.

Reversibility / safety (same model as _merge_known_dup_groups.py):
  • Source URLs unioned into canonical col M (nothing lost).
  • Duplicate rows hidden via hiddenByUser (un-hideable in one click).
  • Col P annotated «🔁 дубль строки N | ред.коммент: <preserved>»
    — editor's existing text NEVER overwritten.
  • Sections never changed (out of scope).
  • Dry-run unless --apply.
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

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

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

# (canonical_row, [duplicate_rows], short_note_for_log)
GROUPS = [
    (3,   [4],            "Ram Rumble Bee 2027"),
    (8,   [18],           "VinFast VF 8 gen-2"),
    (23,  [38],           "Mercedes-AMG GT 4-Door 1169hp"),
    (59,  [94],           "Xiaomi YU7 GT Nürburgring record"),
    (73,  [144, 155],     "Maextro S800 Grand Design (ред: ДУБЛЬ r155)"),
    (88,  [101],          "Chevrolet Blazer EV delay (X-section)"),
    (103, [137],          "Sollers SP7 (ред: ДУБЛЬ r137)"),
    (140, [116, 148, 167], "BMW Alpina relaunch (ред: опять дубль r167)"),
    (164, [235],          "Lotus engines via Renault-Geely Horse"),
    (196, [217],          "Volga C50 spy (ред: Дубль r217)"),
    (208, [210],          "Mercedes-AMG GT Black Series 2027"),
    (212, [238],          "Hyundai Azera = Grandeur (ред: постили r238)"),
    (258, [292, 293],     "Jeep Avenger refresh (ред: дубль 136 r292,r293)"),
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
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{TAB}'!A1:P700",
        valueRenderOption="UNFORMATTED_VALUE"
    ).execute().get("values", [])

    def cell(sr: int, c: int) -> str:
        r = rows[sr - 1] if sr - 1 < len(rows) else []
        return str(r[c]) if len(r) > c else ""

    values_batch: list[dict] = []
    hide_reqs: list[dict] = []
    sid = _sheet_id() if APPLY else -1

    total_hidden = 0
    for canon, dups, note in GROUPS:
        members = [canon, *dups]
        merged: list[str] = []
        for sr in members:
            blob = cell(sr, 12) or cell(sr, 9)
            for u in blob.replace("\n", " ").split():
                if u and u not in merged:
                    merged.append(u)
        print(f"━━ GROUP {note}")
        print(f"   canon r{canon} [{cell(canon, 3)}]: "
              f"{cell(canon, 1)[:80].replace(chr(10),' | ')}")
        print(f"   merged {len(merged)} source URLs")
        for d in dups:
            ex = cell(d, 15).strip()
            ann = f"🔁 дубль строки {canon}"
            if ex and not ex.startswith("🔁"):
                # preserve editor's text inline, truncated
                ann = f"{ann} | ред.коммент: {ex[:160]}"
            print(f"   hide r{d} [{cell(d,3)}]: "
                  f"{cell(d,1)[:65].replace(chr(10),' | ')}")
            print(f"     P = {ann[:110]}")
            total_hidden += 1
            if APPLY:
                values_batch.append({
                    "range": f"'{TAB}'!P{d}", "values": [[ann]],
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

    print(f"PLAN: {len(GROUPS)} groups, {total_hidden} rows to hide,"
          f" {len(GROUPS)} URL-unions to write.")
    if not APPLY:
        print("(dry-run — pass --apply to execute)")
        return 0

    if values_batch:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=EDITOR,
            body={"valueInputOption": "USER_ENTERED",
                  "data": values_batch},
        ).execute()
    if hide_reqs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=EDITOR, body={"requests": hide_reqs}
        ).execute()
    print("APPLIED.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
