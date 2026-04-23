"""Read a 'ТЕСТ статьи vN' tab and write a stats panel to the right of it.

The panel is laid out in columns AB-AC so it sits past the data block (the
articles tab has 26 columns A..Z).

Run:  python scripts/write_stats_panel.py [tab_name]
Default tab: 'ТЕСТ статьи v11'.
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

# Column indices (0-based) — must match write_articles() in batch_fetch_test.py
# Layout after adding the "Лид" column (post-v16):
COL_SECTION = 4          # E  — Раздел
COL_REGION = 5           # F  — Регион
COL_PRIMARY_DOM = 8      # I
COL_PRIMARY_CONF = 10    # K
COL_VERDICT = 13         # N  — Итог бота
COL_LLM_REL = 24         # Y  — LLM relevance
COL_COST = 25            # Z  — Стоимость LLM, $
COL_METHOD = 26          # AA — Способ поиска источника

STATS_START_COL_LETTER = "AC"  # column 29 (one past the shifted debug block)
STATS_RANGE_CLEAR = "AC1:AE200"


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
        range=f"'{tab}'!A2:AA",
    ).execute()
    return resp.get("values", []) or []


def _pct(n: int, total: int) -> str:
    if total <= 0:
        return ""
    return f"{n / total * 100:.1f}%"


def _cell(row: list[str], idx: int) -> str:
    return row[idx] if idx < len(row) else ""


def build_stats(rows: list[list[str]]) -> list[list[str]]:
    total = len(rows)
    verdicts = Counter(_cell(r, COL_VERDICT) or "(пусто)" for r in rows)
    sections = Counter(_cell(r, COL_SECTION) or "(без раздела)" for r in rows)
    regions = Counter(_cell(r, COL_REGION) or "(без региона)" for r in rows)
    primary_conf = Counter(_cell(r, COL_PRIMARY_CONF) or "(нет)" for r in rows)
    primary_methods = Counter(_cell(r, COL_METHOD) or "(нет)" for r in rows)
    llm_rel = Counter(_cell(r, COL_LLM_REL) or "(не вызывался)" for r in rows)

    # Breakdown: среди "Точно новость" — какое распределение по разделам
    news_rows = [r for r in rows if _cell(r, COL_VERDICT) == "Точно новость"]
    news_sections = Counter(_cell(r, COL_SECTION) or "(без раздела)" for r in news_rows)
    news_regions = Counter(_cell(r, COL_REGION) or "(без региона)" for r in news_rows)

    # Стоимость (колонка Y — строка, может быть пустой / числом)
    total_cost = 0.0
    for r in rows:
        raw = _cell(r, COL_COST).replace(",", ".").strip()
        try:
            total_cost += float(raw)
        except ValueError:
            pass

    # Top-10 первоисточников (непустые)
    primary_domains = Counter(
        _cell(r, COL_PRIMARY_DOM) for r in rows if _cell(r, COL_PRIMARY_DOM)
    )

    out: list[list[str]] = []
    out.append(["СТАТИСТИКА", ""])
    out.append(["Всего строк", str(total)])
    out.append(["Стоимость LLM, $", f"{total_cost:.4f}"])
    out.append(["", ""])

    out.append(["Итог бота", "Кол-во / %"])
    order = [
        "Точно новость",
        "Возможно новость",
        "Отклонено LLM",
        "Точно не новость (чёрный список)",
        "Не новость",
        "Отклонить (дубль)",
        "Отклонить (дубль финального URL)",
        "Отклонить (не авто/эконом)",
        "Отклонить (не статья)",
        "Отклонить (старая)",
        "Отклонить (ошибка загрузки)",
        "Отклонить (не удалось извлечь)",
    ]
    seen = set()
    for v in order:
        if v in verdicts:
            out.append([v, f"{verdicts[v]}  ({_pct(verdicts[v], total)})"])
            seen.add(v)
    for v, n in sorted(verdicts.items(), key=lambda kv: -kv[1]):
        if v not in seen:
            out.append([v, f"{n}  ({_pct(n, total)})"])
    out.append(["", ""])

    certain = verdicts.get("Точно новость", 0)
    rejected_llm = verdicts.get("Отклонено LLM", 0)
    blacklist_rej = verdicts.get("Точно не новость (чёрный список)", 0)
    out.append(["✅ Принято (Точно новость)", f"{certain}  ({_pct(certain, total)})"])
    out.append(["❌ Отклонено LLM", f"{rejected_llm}  ({_pct(rejected_llm, total)})"])
    out.append(["⛔ Чёрный список", f"{blacklist_rej}  ({_pct(blacklist_rej, total)})"])
    out.append(["", ""])

    # Разделы — все строки
    out.append(["Раздел (все строки)", "Кол-во"])
    for sec, n in sorted(sections.items(), key=lambda kv: -kv[1]):
        out.append([sec, f"{n}  ({_pct(n, total)})"])
    out.append(["", ""])

    # Разделы — только среди "Точно новость"
    out.append([f"Раздел (только 'Точно новость', n={len(news_rows)})", "Кол-во"])
    for sec, n in sorted(news_sections.items(), key=lambda kv: -kv[1]):
        out.append([sec, f"{n}  ({_pct(n, len(news_rows))})"])
    out.append(["", ""])

    # Регионы — все + news
    out.append(["Регион (все строки)", "Кол-во"])
    for reg, n in sorted(regions.items(), key=lambda kv: -kv[1]):
        out.append([reg, f"{n}  ({_pct(n, total)})"])
    out.append(["", ""])

    out.append([f"Регион (только 'Точно новость', n={len(news_rows)})", "Кол-во"])
    for reg, n in sorted(news_regions.items(), key=lambda kv: -kv[1]):
        out.append([reg, f"{n}  ({_pct(n, len(news_rows))})"])
    out.append(["", ""])

    # Primary source
    out.append(["Уверенность первоисточника", "Кол-во"])
    for conf, n in sorted(primary_conf.items(), key=lambda kv: -kv[1]):
        out.append([conf, f"{n}  ({_pct(n, total)})"])
    out.append(["", ""])

    out.append(["Способ поиска первоисточника", "Кол-во"])
    for m, n in sorted(primary_methods.items(), key=lambda kv: -kv[1]):
        out.append([m, f"{n}  ({_pct(n, total)})"])
    out.append(["", ""])

    # LLM relevance
    out.append(["LLM relevance", "Кол-во"])
    for rel, n in sorted(llm_rel.items(), key=lambda kv: -kv[1]):
        out.append([rel, f"{n}  ({_pct(n, total)})"])
    out.append(["", ""])

    # Top-10 первоисточников
    out.append(["Top-10 доменов первоисточников", "Кол-во"])
    for dom, n in primary_domains.most_common(10):
        out.append([dom, str(n)])

    return out


def _ensure_columns(svc, sheet_id: int, min_cols: int = 31) -> None:
    """Expand the sheet so it has at least ``min_cols`` columns.

    A freshly-created tab has 26 columns (A..Z). The stats panel lives at
    AB..AC (27..28), so we append columns before writing.
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


def _format_panel(svc, tab: str, sheet_id: int, n_rows: int) -> None:
    """Bold headers + widen columns + grey fill on title cells."""
    # Columns AC=28, AD=29 (0-based: 28, 29) — shifted after the "Лид" insert.
    col_ab = 28  # kept variable name for brevity — now actually column AC
    col_ac = 29
    requests = [
        # Widen AC to 380, AD to 220
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": col_ab,
                    "endIndex": col_ab + 1,
                },
                "properties": {"pixelSize": 380},
                "fields": "pixelSize",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": col_ac,
                    "endIndex": col_ac + 1,
                },
                "properties": {"pixelSize": 220},
                "fields": "pixelSize",
            }
        },
        # Bold + light-grey background on title row A (row 0)
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": col_ab,
                    "endColumnIndex": col_ac + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True, "fontSize": 12},
                        "backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
                    }
                },
                "fields": "userEnteredFormat(textFormat,backgroundColor)",
            }
        },
    ]
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body={"requests": requests}
    ).execute()


def main() -> int:
    tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ статьи v11"
    svc = _svc()
    sheet_id = _get_sheet_id(svc, tab)
    rows = _fetch_rows(svc, tab)
    print(f"Loaded {len(rows)} article rows from '{tab}'.")
    stats = build_stats(rows)

    # Grid has only 27 columns after the "Лид" insert — expand to fit AC:AD.
    _ensure_columns(svc, sheet_id, min_cols=31)

    # Clear the target block first (covers the possible previous run's stats)
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!{STATS_RANGE_CLEAR}"
    ).execute()

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{tab}'!{STATS_START_COL_LETTER}1",
        valueInputOption="USER_ENTERED",
        body={"values": stats},
    ).execute()
    _format_panel(svc, tab, sheet_id, len(stats))
    print(f"Stats panel written to '{tab}' at columns AB:AC  ({len(stats)} rows).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
