"""Read a 'ТЕСТ статьи vN' tab and write a stats panel to the right of it.

Columns of the panel live at AD:AE (after the "Лид" + "Country" inserts the
data block occupies A..AB, AC is a visual gap, AD..AE is stats).

The panel has a visual style matching the editor's mock-up:
  • section-header rows have a dusty-pink background ('red' colour in the mock)
  • body rows have a pale-yellow background
  • one blank row separates sections

Run:  python scripts/write_stats_panel.py [tab_name]
"""

from __future__ import annotations

import io
import os
import sys
from collections import Counter
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

# Column indices (0-based) — must match write_articles() in batch_fetch_test.py.
# Layout after "Лид" (C) + "Страна" (G) inserts:
COL_SECTION = 4          # E — Раздел
COL_REGION = 5           # F — Регион
COL_COUNTRY = 6          # G — Страна
COL_PRIMARY_DOM = 9      # J — Первоисточник (домен)
COL_PRIMARY_CONF = 11    # L — Уверенность источника
COL_VERDICT = 14         # O — Итог бота
COL_LLM_REL = 25         # Z — LLM relevance
COL_COST = 26            # AA — Стоимость LLM, $
COL_METHOD = 27          # AB — Способ поиска источника

STATS_START_COL_LETTER = "AD"  # column 30 (one visual gap past the grid)
STATS_RANGE_CLEAR = "AD1:AE300"
STATS_COL_INDEX_LABEL = 29   # 0-based index of column AD
STATS_COL_INDEX_VALUE = 30   # AE

# Palette — picked to match the screenshot the editor shared.
HEADER_BG = {"red": 0.88, "green": 0.60, "blue": 0.58}  # dusty pink
BODY_BG = {"red": 1.00, "green": 0.95, "blue": 0.80}    # pale yellow
HEADER_FG = {"red": 0.20, "green": 0.05, "blue": 0.08}  # near-black for contrast


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get_sheet_id(svc, tab: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab:
            return int(s["properties"]["sheetId"])
    raise SystemExit(f"Tab not found: {tab!r}")


def _fetch_rows(svc, tab: str) -> list[list[str]]:
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{tab}'!A2:AB",
    ).execute()
    return resp.get("values", []) or []


def _pct(n: int, total: int) -> str:
    if total <= 0:
        return ""
    return f"{n / total * 100:.1f}%"


def _cell(row: list[str], idx: int) -> str:
    return row[idx] if idx < len(row) else ""


# ---------------- panel building -----------------------------------------
# Each returned tuple: (kind, label, value)
#   kind = "H" — section header (pink background, bold)
#        = "B" — body row (yellow background)
#        = "S" — spacer (no colour, no content)
def _h(label: str, value: str = "") -> tuple[str, str, str]:
    return ("H", label, value)


def _b(label: str, value: str = "") -> tuple[str, str, str]:
    return ("B", label, value)


def _s() -> tuple[str, str, str]:
    return ("S", "", "")


def build_stats(rows: list[list[str]]) -> list[tuple[str, str, str]]:
    total = len(rows)

    # Аккумуляторы по всем строкам.
    verdicts = Counter(_cell(r, COL_VERDICT) or "(пусто)" for r in rows)
    regions = Counter(_cell(r, COL_REGION) or "(без региона)" for r in rows)
    countries = Counter(_cell(r, COL_COUNTRY) or "(без страны)" for r in rows)
    primary_conf = Counter(_cell(r, COL_PRIMARY_CONF) or "(нет)" for r in rows)
    primary_methods = Counter(_cell(r, COL_METHOD) or "(нет)" for r in rows)

    # Только «Точно новость» — для секций и топа первоисточников.
    news_rows = [r for r in rows if _cell(r, COL_VERDICT) == "Точно новость"]
    accepted_n = len(news_rows)
    sections = Counter(
        (_cell(r, COL_SECTION) or "(без раздела)").replace(" (неактивный)", "")
        for r in news_rows
    )

    # Стоимость (колонка AA — строка, может быть пустой / числом).
    total_cost = 0.0
    for r in rows:
        raw = _cell(r, COL_COST).replace(",", ".").strip()
        try:
            total_cost += float(raw)
        except ValueError:
            pass

    out: list[tuple[str, str, str]] = []

    # 1) СТАТИСТИКА
    out.append(_h("СТАТИСТИКА"))
    out.append(_b("Всего строк", str(total)))
    out.append(_b("Стоимость LLM, $", f"{total_cost:.4f}"))
    out.append(_s())

    # 2) Итог LLM — три обобщённые группы.
    accepted = verdicts.get("Точно новость", 0)
    rejected_llm = verdicts.get("Отклонено LLM", 0)
    blacklist = verdicts.get("Точно не новость (чёрный список)", 0)
    out.append(_h("Итог LLM", "Кол-во / %"))
    out.append(_b("✅ Принято (Точно новость)", f"{accepted}  ({_pct(accepted, total)})"))
    out.append(_b("❌ Отклонено LLM", f"{rejected_llm}  ({_pct(rejected_llm, total)})"))
    out.append(_b("⛔ Чёрный список", f"{blacklist}  ({_pct(blacklist, total)})"))
    out.append(_s())

    # 3) Разделы — только среди принятых, % тоже от принятых (как в mock-up).
    out.append(_h(f"Раздел (только принятые, n={accepted_n})", "Кол-во"))
    for sec, n in sorted(sections.items(), key=lambda kv: -kv[1]):
        out.append(_b(sec, f"{n}  ({_pct(n, accepted_n)})"))
    out.append(_s())

    # 4) Регионы — % от total (как в mock-up).
    out.append(_h("Регион (все строки)", "Кол-во"))
    for reg, n in sorted(regions.items(), key=lambda kv: -kv[1]):
        out.append(_b(reg, f"{n}  ({_pct(n, total)})"))
    out.append(_s())

    # 5) Страны
    out.append(_h("Страна (все строки)", "Кол-во"))
    for c, n in sorted(countries.items(), key=lambda kv: -kv[1]):
        out.append(_b(c, f"{n}  ({_pct(n, total)})"))
    out.append(_s())

    # 6) Уверенность первоисточника
    out.append(_h("Уверенность первоисточника", "Кол-во"))
    for conf, n in sorted(primary_conf.items(), key=lambda kv: -kv[1]):
        out.append(_b(conf, f"{n}  ({_pct(n, total)})"))
    out.append(_s())

    # 7) Способ поиска первоисточника
    out.append(_h("Способ поиска первоисточника", "Кол-во"))
    for m, n in sorted(primary_methods.items(), key=lambda kv: -kv[1]):
        out.append(_b(m, f"{n}  ({_pct(n, total)})"))

    return out


def _ensure_columns(svc, sheet_id: int, min_cols: int = 32) -> None:
    """Expand the sheet so it has at least ``min_cols`` columns.

    A freshly-created tab has 28 columns (after the Лид+Страна inserts). The
    stats panel lives at AD..AE (30..31), so we append if needed.
    """
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    current = None
    for s in meta["sheets"]:
        if int(s["properties"]["sheetId"]) == sheet_id:
            current = int(s["properties"]["gridProperties"]["columnCount"])
            break
    if current is None:
        return
    if current >= min_cols:
        return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={
            "requests": [
                {
                    "appendDimension": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "length": min_cols - current,
                    }
                }
            ]
        },
    ).execute()


def _format_panel(svc, sheet_id: int, stats: list[tuple[str, str, str]]) -> None:
    """Apply per-row colour formatting matching the editor's mock-up.

    Pink-with-bold on section headers, pale yellow on body rows, whitespace
    on spacers. Also widens the two stats columns so the labels fit.
    """
    col_label = STATS_COL_INDEX_LABEL  # AD
    col_value = STATS_COL_INDEX_VALUE  # AE

    requests: list[dict] = [
        # Widen AD / AE
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id, "dimension": "COLUMNS",
                    "startIndex": col_label, "endIndex": col_label + 1,
                },
                "properties": {"pixelSize": 360},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id, "dimension": "COLUMNS",
                    "startIndex": col_value, "endIndex": col_value + 1,
                },
                "properties": {"pixelSize": 180},
                "fields": "pixelSize",
            }
        },
    ]

    # Per-row formatting: walk through stats and group consecutive rows of the
    # same kind to minimise API calls.
    def _row_range(start: int, end_exclusive: int) -> dict:
        return {
            "sheetId": sheet_id,
            "startRowIndex": start,
            "endRowIndex": end_exclusive,
            "startColumnIndex": col_label,
            "endColumnIndex": col_value + 1,
        }

    i = 0
    n = len(stats)
    while i < n:
        kind = stats[i][0]
        j = i
        while j < n and stats[j][0] == kind:
            j += 1
        if kind == "H":
            requests.append({
                "repeatCell": {
                    "range": _row_range(i, j),
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": HEADER_BG,
                            "textFormat": {
                                "bold": True,
                                "fontSize": 11,
                                "foregroundColor": HEADER_FG,
                            },
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment,wrapStrategy)",
                }
            })
        elif kind == "B":
            requests.append({
                "repeatCell": {
                    "range": _row_range(i, j),
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": BODY_BG,
                            "textFormat": {"bold": False, "fontSize": 10},
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,verticalAlignment,wrapStrategy)",
                }
            })
        else:  # "S" — spacer, clear background
            requests.append({
                "repeatCell": {
                    "range": _row_range(i, j),
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor",
                }
            })
        i = j

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body={"requests": requests}
    ).execute()


def main() -> int:
    tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ статьи v16"
    svc = _svc()
    sheet_id = _get_sheet_id(svc, tab)
    rows = _fetch_rows(svc, tab)
    print(f"Loaded {len(rows)} article rows from '{tab}'.")
    stats = build_stats(rows)

    # Grid must be at least 32 cols wide for AD..AE.
    _ensure_columns(svc, sheet_id, min_cols=32)

    # Clear the target block first (covers the possible previous run's stats).
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!{STATS_RANGE_CLEAR}"
    ).execute()

    # Write values: [label, value] per row.
    values = [[s[1], s[2]] for s in stats]
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{tab}'!{STATS_START_COL_LETTER}1",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()

    _format_panel(svc, sheet_id, stats)
    print(
        f"Stats panel written to '{tab}' at columns "
        f"{STATS_START_COL_LETTER}:AE  ({len(stats)} rows, coloured)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
