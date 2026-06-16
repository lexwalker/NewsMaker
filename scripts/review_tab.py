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


def ensure_tab(svc, spreadsheet_id: str) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    if any(s["properties"]["title"] == REVIEW_TAB for s in meta["sheets"]):
        return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": REVIEW_TAB}}}]},
    ).execute()


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
    ensure_tab(svc, spreadsheet_id)
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
    return len(fresh)
