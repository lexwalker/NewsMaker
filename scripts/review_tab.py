"""Shared review surface — ONE tab the editor uses for every borderline AI
decision, fed by two sources:

  * sample_rejected.py     — auto-borderline rows the bot REJECTED
                             ("agree it's not needed?")
  * build_news_sheet.py    — rows the LLM self-flagged (possible-dup /
                             not-news) that would otherwise hit the feed
                             ("confirm keep/reject")

Both APPEND (deduped by url_hash, newest batch on top, history preserved),
so neither clobbers the other. ingest_rejected_labels.py reads the whole
tab and routes by the Тип column. Import-safe: no I/O / stdout rebind at
module load.
"""

from __future__ import annotations

REVIEW_TAB = "Разметка отклонённого (ИИ)"
# A..G — D/E are the editor's to fill. F (url_hash) is the stable key for
# dedup + idempotent ingest.
HEADER = ["Заголовок", "Контекст ИИ", "Тип",
          "Нужно? (да/нет)", "Раздел (если да)", "url_hash", "URL"]
INSTRUCTIONS = (
    "ИИ просит проверить. Колонка D: «да» = новость нужна, «нет» = не нужна. "
    "Если «да» — по возможности впишите раздел в E. Тип (C): «отклонил» — ИИ "
    "это отклонил (нужна ли?); «дубль»/«не новость» — ИИ бы опубликовал, но "
    "засомневался (оставить?). Это учит ИИ на ваших решениях."
)

# Тип values (col C) — drive ingest routing.
TYPE_REJECTED = "отклонил"        # bot rejected it (+ ":cause")
TYPE_DUP = "дубль"                # bot would publish, suspected duplicate
TYPE_NOT_NEWS = "не новость"      # bot would publish, looks like a guide/opinion


def ensure_tab(svc, spreadsheet_id: str) -> int:
    """Return the review tab's sheetId, creating it if missing."""
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == REVIEW_TAB:
            return s["properties"]["sheetId"]
    resp = svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": REVIEW_TAB}}}]},
    ).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


# Column widths (px) for A..G — title/context wide, verdict/section narrow.
_WIDTHS = [380, 300, 140, 95, 140, 110, 230]


def apply_formatting(svc, spreadsheet_id: str, sheet_id: int) -> None:
    """Style like 'Новости (новые)': freeze instructions+header, green
    header band, column widths, body wrap, and a да/нет colour rule on the
    'Нужно?' column. Idempotent — safe to re-run on every append."""
    green = {"red": 0.27, "green": 0.45, "blue": 0.30}
    # Idempotent: drop any conditional rules we added before so re-runs
    # don't accumulate duplicates.
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
        # freeze row1 (instructions) + row2 (header)
        {"updateSheetProperties": {
            "properties": {"sheetId": sheet_id,
                           "gridProperties": {"frozenRowCount": 2}},
            "fields": "gridProperties.frozenRowCount"}},
        # instructions row — light grey, wrapped
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
        # header row (row 2) — green band, white bold
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
        # body — wrap, top-aligned, fontSize 10
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
    # да / нет colour rule on the "Нужно?" column (D, index 3)
    d_range = {"sheetId": sheet_id, "startRowIndex": 2,
               "startColumnIndex": 3, "endColumnIndex": 4}
    for text, colour in (("да", {"red": 0.80, "green": 0.94, "blue": 0.80}),
                         ("нет", {"red": 0.96, "green": 0.80, "blue": 0.80})):
        reqs.append({"addConditionalFormatRule": {"rule": {
            "ranges": [d_range],
            "booleanRule": {
                "condition": {"type": "TEXT_EQ",
                              "values": [{"userEnteredValue": text}]},
                "format": {"backgroundColor": colour}}},
            "index": 0}})
    try:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": reqs}).execute()
    except Exception:  # noqa: BLE001 — formatting is cosmetic, never fatal
        pass


def _existing(svc, spreadsheet_id: str) -> list[list]:
    """Data rows below the header (row 1 = instructions, row 2 = header)."""
    return svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"'{REVIEW_TAB}'!A3:G5000",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])


def append_batch(svc, spreadsheet_id: str, rows: list[dict],
                 run_label: str) -> int:
    """Prepend a new batch (separator + rows) above existing data, skipping
    any url_hash already present. rows: dicts with keys title, context,
    type, url_hash, url. Returns the number of NEW rows written."""
    sheet_id = ensure_tab(svc, spreadsheet_id)
    existing = _existing(svc, spreadsheet_id)
    seen = {str(r[5]).strip() for r in existing if len(r) > 5 and r[5]}
    fresh = [r for r in rows if r.get("url_hash") and r["url_hash"] not in seen]
    if not fresh:
        return 0
    sep = [f"━━  {run_label}  ━━", "", "", "", "", "", ""]
    new_data = [[
        (r.get("title") or "")[:300], (r.get("context") or "")[:300],
        r.get("type", ""), "", "", r.get("url_hash", ""), r.get("url", ""),
    ] for r in fresh]
    body = [[INSTRUCTIONS, "", "", "", "", "", ""], HEADER, sep] \
        + new_data + existing
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=f"'{REVIEW_TAB}'!A1",
        valueInputOption="USER_ENTERED", body={"values": body},
    ).execute()
    apply_formatting(svc, spreadsheet_id, sheet_id)
    return len(fresh)
