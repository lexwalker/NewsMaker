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
VERDICT_COLUMN_LETTER = "O"  # 15th column — «Итог бота» (shifted after Country insert)
START_COL = 0   # A
END_COL = 28    # A..AB inclusive = 28 columns (after Лид + Country inserts)
MAX_ROWS = 2000

# --- Block bands (0-based half-open ranges) ------------------------------
BLOCK_BANDS: list[tuple[int, int, dict[str, float], str]] = [
    # (start_col, end_col, bg colour, label)
    (0,  9,  {"red": 0.80, "green": 0.93, "blue": 0.80}, "«Что за новость»"),
    (9, 12,  {"red": 0.82, "green": 0.89, "blue": 0.98}, "«Первоисточник»"),
    (12, 17, {"red": 1.00, "green": 0.95, "blue": 0.80}, "«Для редактора»"),
    (17, 28, {"red": 0.88, "green": 0.88, "blue": 0.88}, "«Отладка» (скрыто по умолчанию)"),
]
# Columns to hide by default (index 17..27 — the whole «Отладка» block).
HIDDEN_COLUMNS: tuple[int, ...] = tuple(range(17, 28))
# Columns whose rows should wrap (title, lede, image URL, primary URL, note, reasons).
WRAP_COLUMNS: tuple[int, ...] = (1, 2, 8, 10, 12, 16, 23)
# Pixel widths (sheet feels a lot less cramped when title is wide enough).
COL_WIDTHS: dict[int, int] = {
    0: 145,   # Прогон
    1: 380,   # Заголовок (EN/RU)
    2: 420,   # Лид
    3: 300,   # URL статьи
    4: 140,   # Раздел
    5: 70,    # Регион
    6: 100,   # Страна
    7: 165,   # Дата
    8: 260,   # Картинка URL
    9: 200,   # Первоисточник домен
    10: 320,  # Первоисточник URL
    11: 110,  # Уверенность
    12: 260,  # Пометка
    13: 110,  # Confidence
    14: 210,  # Итог бота
    15: 260,  # Ручная проверка
    16: 260,  # Комментарий
}

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
    # LLM relevance-check rejection — stays yellow so the editor can still
    # see what the LLM filtered out, but clearly marked as rejected.
    (f'=$%s2="Отклонено LLM"' % VERDICT_COLUMN_LETTER, YELLOW),
    # Errors and dedup:
    (f'=$%s2="Отклонить (ошибка загрузки)"' % VERDICT_COLUMN_LETTER, RED),
    (f'=$%s2="Отклонить (не удалось извлечь)"' % VERDICT_COLUMN_LETTER, RED),
    (f'=$%s2="Отклонить (дубль финального URL)"' % VERDICT_COLUMN_LETTER, BLUE),
    (f'=$%s2="Отклонить (обработан ранее)"' % VERDICT_COLUMN_LETTER, BLUE),
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
    # Freeze the header row AND the first 3 columns (прогон + заголовок + URL)
    requests.append(
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": 1,
                        "frozenColumnCount": 3,
                    },
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }
        }
    )
    # Base header styling: bold + wrap (each block adds its own colour below).
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
                        "textFormat": {"bold": True},
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(textFormat,wrapStrategy,verticalAlignment)",
            }
        }
    )
    # Colour-band each logical block in the header row.
    for start, end, bg, _label in BLOCK_BANDS:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0, "endRowIndex": 1,
                        "startColumnIndex": start, "endColumnIndex": end,
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": bg}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        )
    # Hide the debug columns by default (editors can unhide via right-click).
    for col in HIDDEN_COLUMNS:
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id, "dimension": "COLUMNS",
                        "startIndex": col, "endIndex": col + 1,
                    },
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            }
        )
    # Per-column pixel widths (visible columns only).
    for col, px in COL_WIDTHS.items():
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id, "dimension": "COLUMNS",
                        "startIndex": col, "endIndex": col + 1,
                    },
                    "properties": {"pixelSize": px},
                    "fields": "pixelSize",
                }
            }
        )
    # Enable text wrap + top-align for body-content columns (title, URL, reasons).
    for col in WRAP_COLUMNS:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1, "endRowIndex": MAX_ROWS,
                        "startColumnIndex": col, "endColumnIndex": col + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP",
                            "verticalAlignment": "TOP",
                        }
                    },
                    "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)",
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
