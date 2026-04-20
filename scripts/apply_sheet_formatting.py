"""Paint a ТЕСТ статьи[ vN] tab by the 'Итог бота' verdict.

Green  — "Отправить в LLM"
Red    — fetch / extraction errors
Yellow — heuristic rejections (not an article / not automotive)

Callable as a library (from batch_fetch_test.py) or from the CLI:

  python scripts/apply_sheet_formatting.py                  # latest version
  python scripts/apply_sheet_formatting.py "ТЕСТ статьи"    # specific tab
"""

from __future__ import annotations

import io
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

TAB_BASE = "ТЕСТ статьи"
VERDICT_COLUMN_LETTER = "O"  # 15th column — «Итог бота»
START_COL = 0  # A
END_COL = 17  # up to Q inclusive
MAX_ROWS = 2000

GREEN = {"red": 0.70, "green": 0.92, "blue": 0.72}   # точно новость
YELLOW = {"red": 1.00, "green": 0.93, "blue": 0.72}  # возможно новость
GREY = {"red": 0.93, "green": 0.93, "blue": 0.93}    # точно не новость (чистый отказ)
RED = {"red": 0.98, "green": 0.80, "blue": 0.80}     # ошибка сети / извлечения
BLUE = {"red": 0.83, "green": 0.89, "blue": 0.98}    # дубль финального URL

RULES: list[tuple[str, dict]] = [
    # Three-tier grading (new in v3):
    (f'=$%s2="Точно новость"' % VERDICT_COLUMN_LETTER, GREEN),
    (f'=$%s2="Возможно новость"' % VERDICT_COLUMN_LETTER, YELLOW),
    (f'=$%s2="Точно не новость (не статья)"' % VERDICT_COLUMN_LETTER, GREY),
    (f'=$%s2="Точно не новость (не авто)"' % VERDICT_COLUMN_LETTER, GREY),
    (f'=$%s2="Точно не новость (старая)"' % VERDICT_COLUMN_LETTER, GREY),
    (f'=$%s2="Точно не новость (чёрный список)"' % VERDICT_COLUMN_LETTER, GREY),
    # Errors and dedup:
    (f'=$%s2="Отклонить (ошибка загрузки)"' % VERDICT_COLUMN_LETTER, RED),
    (f'=$%s2="Отклонить (не удалось извлечь)"' % VERDICT_COLUMN_LETTER, RED),
    (f'=$%s2="Отклонить (дубль финального URL)"' % VERDICT_COLUMN_LETTER, BLUE),
    # Back-compat for older v1/v2 tabs:
    (f'=$%s2="Отправить в LLM"' % VERDICT_COLUMN_LETTER, GREEN),
    (f'=$%s2="Отклонить (не статья)"' % VERDICT_COLUMN_LETTER, GREY),
    (f'=$%s2="Отклонить (не авто/эконом)"' % VERDICT_COLUMN_LETTER, GREY),
]


def _latest_tab(tab_names: list[str]) -> str | None:
    """Pick 'ТЕСТ статьи vN' with the highest N, or the bare base tab."""
    pat = re.compile(rf"^{re.escape(TAB_BASE)}\s+v(\d+)$")
    versioned = [(int(m.group(1)), t) for t in tab_names if (m := pat.match(t))]
    if versioned:
        versioned.sort()
        return versioned[-1][1]
    return TAB_BASE if TAB_BASE in tab_names else None


def apply_formatting(svc, tab_name: str) -> None:  # type: ignore[no-untyped-def]
    """Clear old rules on <tab_name> and re-apply our 5 colour rules + header style."""
    meta = (
        svc.spreadsheets()
        .get(
            spreadsheetId=SHEET_ID,
            fields="sheets(properties(sheetId,title),conditionalFormats)",
        )
        .execute()
    )
    sheet_id: int | None = None
    existing_rule_count = 0
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab_name:
            sheet_id = s["properties"]["sheetId"]
            existing_rule_count = len(s.get("conditionalFormats", []))
            break
    if sheet_id is None:
        raise RuntimeError(f"Tab {tab_name!r} not found")

    requests = []
    for i in reversed(range(existing_rule_count)):
        requests.append(
            {"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": i}}
        )
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        }
    )
    requests.append(
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": START_COL,
                    "endColumnIndex": END_COL,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
                        "textFormat": {"bold": True},
                        "wrapStrategy": "WRAP",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,wrapStrategy)",
            }
        }
    )
    for formula, colour in RULES:
        requests.append(
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [
                            {
                                "sheetId": sheet_id,
                                "startRowIndex": 1,
                                "endRowIndex": MAX_ROWS,
                                "startColumnIndex": START_COL,
                                "endColumnIndex": END_COL,
                            }
                        ],
                        "booleanRule": {
                            "condition": {
                                "type": "CUSTOM_FORMULA",
                                "values": [{"userEnteredValue": formula}],
                            },
                            "format": {"backgroundColor": colour},
                        },
                    },
                    "index": 0,
                }
            }
        )
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body={"requests": requests}
    ).execute()


def main(argv: list[str]) -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    if len(argv) >= 2:
        tab = argv[1]
    else:
        meta = (
            svc.spreadsheets()
            .get(spreadsheetId=SHEET_ID, fields="sheets(properties(title))")
            .execute()
        )
        tab = _latest_tab([s["properties"]["title"] for s in meta["sheets"]]) or ""
        if not tab:
            print("No suitable tab found.", file=sys.stderr)
            return 2

    print(f"Formatting tab: {tab!r}")
    apply_formatting(svc, tab)
    print(f"Applied {len(RULES)} rules + frozen header + header style.")
    print("Colours: GREEN=LLM, RED=error, YELLOW=rejected.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
