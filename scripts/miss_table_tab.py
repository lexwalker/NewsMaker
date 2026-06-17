"""'Непокрытые (анализ)' tab — a snapshot table of editor publications we did
NOT surface in the last window, each tagged with its death stage (S1-S4) and a
recommendation. Written by miss_funnel.py --to-sheet.

SNAPSHOT semantics (not append): each run OVERWRITES the data area with the
current window's misses, so the tab always reflects "what slipped through
most recently" without unbounded growth. The window + timestamp sit in row 1.
Import-safe: no I/O at module load.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.core.miss_analysis import HEADER  # noqa: E402

TAB = "Непокрытые (анализ)"
# Last column letter for the data range (9 columns → I).
_LAST_COL = chr(ord("A") + len(HEADER) - 1)
_WIDTHS = [90, 110, 360, 150, 150, 140, 90, 220, 320]


def ensure_tab(svc, spreadsheet_id: str) -> int:
    """Return the tab's sheetId, creating it if missing."""
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == TAB:
            return s["properties"]["sheetId"]
    resp = svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": TAB}}}]},
    ).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


def write_snapshot(svc, spreadsheet_id: str, data_rows: list, window_label: str) -> int:
    """Overwrite the data area with this window's misses. Returns row count."""
    sheet_id = ensure_tab(svc, spreadsheet_id)
    # Clear generously so a shorter snapshot leaves no stale rows behind.
    svc.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{TAB}'!A1:{_LAST_COL}20000").execute()
    instr = (f"Непокрытые публикации редактора · {window_label}. Снимок за "
             "прогон (перезаписывается). Стадия: S1 источника нет · S2 источник "
             "есть, не собрали · S3 убила эвристика · S4 отклонил ИИ. "
             "Колонка «ИИ-рекомендация» — по источнику, как закрыть пропуск.")
    pad = [""] * (len(HEADER) - 1)
    body = [[instr, *pad], HEADER] + data_rows
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"'{TAB}'!A1",
        valueInputOption="USER_ENTERED", body={"values": body}).execute()
    apply_formatting(svc, spreadsheet_id, sheet_id)
    return len(data_rows)


def apply_formatting(svc, spreadsheet_id: str, sheet_id: int) -> None:
    """Freeze instr+header, green header band, widths, wrap, and colour the
    Стадия column by stage. Idempotent — drops prior conditional rules first."""
    green = {"red": 0.27, "green": 0.45, "blue": 0.30}
    reqs: list[dict] = []
    try:
        meta = svc.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties.sheetId,conditionalFormats)").execute()
        for s in meta.get("sheets", []):
            if s.get("properties", {}).get("sheetId") == sheet_id:
                n = len(s.get("conditionalFormats", []))
                reqs += [{"deleteConditionalFormatRule":
                          {"sheetId": sheet_id, "index": 0}} for _ in range(n)]
                break
    except Exception:  # noqa: BLE001
        pass
    reqs += [
        {"updateSheetProperties": {
            "properties": {"sheetId": sheet_id,
                           "gridProperties": {"frozenRowCount": 2}},
            "fields": "gridProperties.frozenRowCount"}},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
            "cell": {"userEnteredFormat": {
                "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.88},
                "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE",
                "textFormat": {"italic": True, "fontSize": 9}}},
            "fields": "userEnteredFormat(backgroundColor,wrapStrategy,"
                      "verticalAlignment,textFormat)"}},
        {"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "ROWS",
                      "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 46}, "fields": "pixelSize"}},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2,
                      "startColumnIndex": 0, "endColumnIndex": len(HEADER)},
            "cell": {"userEnteredFormat": {
                "backgroundColor": green,
                "textFormat": {"bold": True, "fontSize": 11,
                               "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                "verticalAlignment": "MIDDLE", "horizontalAlignment": "CENTER",
                "wrapStrategy": "WRAP"}},
            "fields": "userEnteredFormat(backgroundColor,textFormat,"
                      "verticalAlignment,horizontalAlignment,wrapStrategy)"}},
        {"repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 2,
                      "startColumnIndex": 0, "endColumnIndex": len(HEADER)},
            "cell": {"userEnteredFormat": {
                "wrapStrategy": "WRAP", "verticalAlignment": "TOP",
                "textFormat": {"fontSize": 10}}},
            "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat)"}},
    ]
    for i, px in enumerate(_WIDTHS):
        reqs.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                      "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": px}, "fields": "pixelSize"}})
    # Colour the Стадия column (E, index 3) by stage code.
    stage_range = {"sheetId": sheet_id, "startRowIndex": 2,
                   "startColumnIndex": 3, "endColumnIndex": 4}
    for code, colour in (
        ("S1", {"red": 0.96, "green": 0.80, "blue": 0.80}),   # red — add source
        ("S2", {"red": 0.99, "green": 0.90, "blue": 0.78}),   # orange — fix crawl
        ("S3", {"red": 1.00, "green": 0.97, "blue": 0.80}),   # yellow — heuristic
        ("S4", {"red": 0.82, "green": 0.88, "blue": 0.98}),   # blue — classifier
    ):
        reqs.append({"addConditionalFormatRule": {"rule": {
            "ranges": [stage_range],
            "booleanRule": {
                "condition": {"type": "TEXT_STARTS_WITH",
                              "values": [{"userEnteredValue": code}]},
                "format": {"backgroundColor": colour}}},
            "index": 0}})
    try:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
    except Exception:  # noqa: BLE001 — formatting is cosmetic, never fatal
        pass
