"""Build / refresh the persistent 'Новости' tab — the editor's working list.

Behaviour:
  - Reads data/clusters_<input_tab>.json
  - Filters out junk clusters (aggregator / nav / "{Brand}" stub titles)
  - Flags year-mismatch (future verb + past year, or year present in only
    one of the two language lines) for editor review
  - Creates the 'Новости' tab on first call; on subsequent calls just
    prepends new clusters at the top via insertDimension
  - Dedups against the canonical_url AND primary_url already present in
    column J of the sheet — already-seen stories aren't re-added, so
    editor's manual edits in cols O/P keep their attachment
  - Applies visual formatting on first creation: green header band, frozen
    rows/cols, per-section background tint in column D, multi-source
    highlight on column L, flag highlight on column N, alternating row
    bands, sensible column widths

Run:  python scripts/build_news_sheet.py "ТЕСТ статьи v18"
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
from datetime import datetime, timezone
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

NEWS_TAB = "Новости"

HEADER = [
    "Прогон (UTC)",          # A
    "Заголовок (EN / RU)",   # B
    "Лид",                   # C
    "Раздел",                # D — colour-tinted per section
    "Регион",                # E
    "Страна",                # F
    "Дата публикации",       # G
    "Картинка (URL)",        # H
    "Первоисточник (домен)", # I
    "Первоисточник URL",     # J — also the dedup key
    "Уверенность",           # K
    "Источников",            # L — numeric, blue when >1
    "Все URL источников",    # M
    "Флаг проверки",         # N — orange when filled
    "Ручная проверка",       # O — editor input, never overwritten
    "Комментарий",           # P — editor input, never overwritten
]


# ----- Junk filter --------------------------------------------------------
def _is_junk_cluster(c: dict) -> bool:
    title = c["canonical_title"].strip()
    text = re.sub(r"\s*EN:\s*", "", title)
    text = re.sub(r"\s*RU:\s*", " ", text)
    text = re.sub(r"\s*\([A-Za-zА-Яа-яЁё]{2,4}\)\s*", "", text).strip()
    if len(text.split()) <= 2:
        return True
    junk_patterns = (
        "PressClub", "PRESSCLUB", "Press Club",
        "Newsroom", "NEWSROOM",
        "News Release", "Press releases",
        "/* page */",
        "Image Gallery",
        "Disclosure Policy",
        "IR Calendar",
        "ESG data book",
        "Sustainability Report",
        "Корпоративный профиль",
        "Главная страница",
    )
    if any(p.lower() in title.lower() for p in junk_patterns):
        return True
    lede = c.get("canonical_lede", "").strip()
    if len(lede) < 50 and len(text.split()) < 6:
        return True
    return False


# ----- Year-drift detector ------------------------------------------------
_PAST_YEARS = {y for y in range(2020, 2026)}
_FUTURE_VERBS = (
    "will ", "to arrive", "to launch", "to open", "to expand",
    "to start", "to debut", "to introduce", "to build", "плани",
    "появится", "запустит", "откроет", "выйдет", "начнёт",
    "стартует", "дебютирует",
)
_YEAR_RE = re.compile(r"\b(20\d{2})\b")


def _split_lines(combined: str) -> tuple[str, str]:
    en, ru = "", ""
    for line in combined.splitlines():
        s = line.strip()
        if s.startswith("EN:"):
            en = s[3:].strip()
        elif s.startswith("RU:"):
            ru = s[3:].strip()
    if not en and not ru:
        en = combined.strip()
    return en, ru


def _strip_lang_tag(s: str) -> str:
    return re.sub(r"\s*\([A-Za-zА-Яа-яЁё]{2,4}\)\s*$", "", s).strip()


def _flag_review(title: str) -> str:
    en, ru = _split_lines(title)
    for line, lang in ((en, "EN"), (ru, "RU")):
        text_lower = line.lower()
        if any(v in text_lower for v in _FUTURE_VERBS):
            for y in _PAST_YEARS:
                if str(y) in line:
                    return f"⚠ возможно неверный год {y} (в {lang}-строке)"
    if en and ru:
        en_years = set(_YEAR_RE.findall(_strip_lang_tag(en)))
        ru_years = set(_YEAR_RE.findall(_strip_lang_tag(ru)))
        only_en = en_years - ru_years
        only_ru = ru_years - en_years
        if only_en or only_ru:
            both = sorted(only_en | only_ru)
            return f"⚠ EN/RU расходятся по годам: {', '.join(both)}"
    return ""


# ----- Section colour palette --------------------------------------------
SECTION_TINT: dict[str, dict[str, float]] = {
    "Confirmed":          {"red": 0.78, "green": 0.95, "blue": 0.78},  # mint
    "Local specifics":    {"red": 1.00, "green": 0.95, "blue": 0.74},  # cream
    "Other news":         {"red": 0.85, "green": 0.92, "blue": 0.99},  # ice blue
    "Rumors":             {"red": 0.99, "green": 0.85, "blue": 0.92},  # blush
    "Economics":          {"red": 1.00, "green": 0.88, "blue": 0.78},  # peach
    "LCV news":           {"red": 0.92, "green": 0.86, "blue": 0.99},  # lavender
    "Test-drive":         {"red": 0.92, "green": 0.92, "blue": 0.92},  # grey
    "Dealer news / Promo":{"red": 0.99, "green": 0.93, "blue": 0.85},  # latte
    "Motorshow":          {"red": 0.84, "green": 0.96, "blue": 0.94},  # mint-teal
}


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _ensure_tab(svc, tab: str) -> tuple[int, bool]:
    """Create tab if missing. Return (sheetId, was_created)."""
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab:
            return int(s["properties"]["sheetId"]), False
    resp = svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
    ).execute()
    new = resp["replies"][0]["addSheet"]["properties"]
    return int(new["sheetId"]), True


def _existing_keys(svc, tab: str) -> set[str]:
    """Pull existing primary URLs (column J) from the news tab for dedup.

    Treats both the canonical_url and primary_url as identifying keys —
    if either is already in the sheet, the cluster is skipped.
    """
    keys: set[str] = set()
    try:
        # Column J = Первоисточник URL
        resp = svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"'{tab}'!J2:J"
        ).execute()
        for row in resp.get("values", []) or []:
            if row and row[0]:
                keys.add(row[0])
    except Exception:  # noqa: BLE001
        pass
    try:
        # Column M holds the newline-separated URL list — pull all of those too
        resp = svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"'{tab}'!M2:M"
        ).execute()
        for row in resp.get("values", []) or []:
            if not row or not row[0]:
                continue
            for u in row[0].splitlines():
                u = u.strip()
                if u:
                    keys.add(u)
    except Exception:  # noqa: BLE001
        pass
    return keys


def _row_for_cluster(c: dict, run_ts: str) -> list[str]:
    members_urls = "\n".join(m["url"] for m in c["members"])
    flag = _flag_review(c["canonical_title"])
    return [
        run_ts,
        c["canonical_title"],
        c.get("canonical_lede", "")[:600],
        c.get("section", "") or "",
        c.get("region", "") or "",
        c.get("country", "") or "",
        c.get("published", "") or "",
        c.get("image_url", "") or "",
        c.get("primary_domain", "") or "",
        c.get("primary_url", "") or "",
        c.get("primary_conf", "") or "",
        str(c["size"]),
        members_urls,
        flag,
        "",
        "",
    ]


def _apply_full_formatting(svc, sheet_id: int) -> None:
    """One-shot styling applied right after the tab is created.

    Header band, freeze, widths, body wrap, alternating bands, and
    conditional rules for multi-source / flag highlights. Idempotent —
    callers may re-run after deleting old conditional rules.
    """
    requests: list[dict] = []
    end_row = 2000
    n_cols = len(HEADER)

    requests.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id,
                "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 2},
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
        }
    })
    # Header — solid green band, white bold text
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                "startColumnIndex": 0, "endColumnIndex": n_cols,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "bold": True, "fontSize": 11,
                        "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                    },
                    "backgroundColor": {"red": 0.27, "green": 0.45, "blue": 0.30},
                    "wrapStrategy": "WRAP",
                    "verticalAlignment": "MIDDLE",
                    "horizontalAlignment": "CENTER",
                }
            },
            "fields": (
                "userEnteredFormat(textFormat,backgroundColor,wrapStrategy,"
                "verticalAlignment,horizontalAlignment)"
            ),
        }
    })
    # Header row a bit taller for wrapped multi-line headings
    requests.append({
        "updateDimensionProperties": {
            "range": {
                "sheetId": sheet_id, "dimension": "ROWS",
                "startIndex": 0, "endIndex": 1,
            },
            "properties": {"pixelSize": 44},
            "fields": "pixelSize",
        }
    })

    widths = {
        0: 110,   # Прогон
        1: 420,   # Заголовок
        2: 380,   # Лид
        3: 160,   # Раздел
        4: 80,    # Регион
        5: 95,    # Страна
        6: 145,   # Дата
        7: 220,   # Картинка URL
        8: 180,   # Первоисточник домен
        9: 280,   # Первоисточник URL
        10: 100,  # Уверенность
        11: 90,   # Источников
        12: 260,  # Все URL
        13: 200,  # Флаг проверки
        14: 180,  # Ручная проверка
        15: 200,  # Комментарий
    }
    for col, px in widths.items():
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id, "dimension": "COLUMNS",
                    "startIndex": col, "endIndex": col + 1,
                },
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        })

    # Body wrap + top-align on text-heavy columns (B title, C lede, J URL,
    # M url-list, N flag, P comment)
    for col in (1, 2, 9, 12, 13, 15):
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "TOP",
                        "textFormat": {"fontSize": 10},
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat)",
            }
        })
    # Middle-align + wrap on the rest of body cells (compact)
    for col in (0, 3, 4, 5, 6, 7, 8, 10, 11, 14):
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "MIDDLE",
                        "textFormat": {"fontSize": 10},
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment,textFormat)",
            }
        })
    # Centre-align numeric columns
    for col in (10, 11):
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment",
            }
        })

    # Alternating row bands — subtle grey on every other row, applied to
    # cols left-of-D and right-of-D (D has its own per-section tint).
    requests.append({
        "addBanding": {
            "bandedRange": {
                "range": {
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": 0, "endColumnIndex": 3,
                },
                "rowProperties": {
                    "firstBandColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}},
                    "secondBandColorStyle": {"rgbColor": {"red": 0.97, "green": 0.97, "blue": 0.97}},
                },
            }
        }
    })
    requests.append({
        "addBanding": {
            "bandedRange": {
                "range": {
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": 4, "endColumnIndex": n_cols,
                },
                "rowProperties": {
                    "firstBandColorStyle": {"rgbColor": {"red": 1, "green": 1, "blue": 1}},
                    "secondBandColorStyle": {"rgbColor": {"red": 0.97, "green": 0.97, "blue": 0.97}},
                },
            }
        }
    })

    # Multi-source highlight on Источников cell (column L = index 11)
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": 11, "endColumnIndex": 12,
                }],
                "booleanRule": {
                    "condition": {
                        "type": "NUMBER_GREATER",
                        "values": [{"userEnteredValue": "1"}],
                    },
                    "format": {
                        "backgroundColor": {"red": 0.45, "green": 0.65, "blue": 0.95},
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                        },
                    },
                },
            },
            "index": 0,
        }
    })

    # Flag highlight on column N = index 13 — orange when non-empty
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": 13, "endColumnIndex": 14,
                }],
                "booleanRule": {
                    "condition": {"type": "NOT_BLANK"},
                    "format": {
                        "backgroundColor": {"red": 1.0, "green": 0.78, "blue": 0.55},
                        "textFormat": {"bold": True},
                    },
                },
            },
            "index": 1,
        }
    })

    # Send in chunks
    CHUNK = 60
    for i in range(0, len(requests), CHUNK):
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID, body={"requests": requests[i:i + CHUNK]}
        ).execute()


def _tint_section_cells(
    svc,
    sheet_id: int,
    *,
    start_data_row: int,
    sections_in_order: list[str],
) -> None:
    """Colour the Раздел cell (column D) by section tint for the freshly
    inserted rows. Group consecutive same-section rows to minimise calls.
    """
    if not sections_in_order:
        return
    requests: list[dict] = []
    i = 0
    while i < len(sections_in_order):
        sec = sections_in_order[i]
        j = i
        while j < len(sections_in_order) and sections_in_order[j] == sec:
            j += 1
        sec_clean = sec.replace(" (неактивный)", "").strip()
        tint = SECTION_TINT.get(sec_clean)
        if tint:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_data_row + i,
                        "endRowIndex": start_data_row + j,
                        "startColumnIndex": 3, "endColumnIndex": 4,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": tint,
                            "textFormat": {"bold": True, "fontSize": 10},
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                        }
                    },
                    "fields": (
                        "userEnteredFormat(backgroundColor,textFormat,"
                        "horizontalAlignment,verticalAlignment)"
                    ),
                }
            })
        i = j
    if not requests:
        return
    CHUNK = 60
    for i in range(0, len(requests), CHUNK):
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID, body={"requests": requests[i:i + CHUNK]}
        ).execute()


def main() -> int:
    src_tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ статьи v18"
    clusters_path = ROOT / "data" / f"clusters_{src_tab.replace(' ', '_')}.json"
    if not clusters_path.exists():
        print(f"Clusters file not found: {clusters_path}", file=sys.stderr)
        print("Run scripts/build_news_clusters.py first.", file=sys.stderr)
        return 2

    clusters = json.loads(clusters_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(clusters)} clusters from {clusters_path.name}")

    clean: list[dict] = []
    junk = 0
    for c in clusters:
        if _is_junk_cluster(c):
            junk += 1
            continue
        clean.append(c)
    print(f"After junk filter: {len(clean)} ({junk} junk dropped)")

    svc = _svc()
    sheet_id, was_created = _ensure_tab(svc, NEWS_TAB)

    # First-time setup — write header + apply formatting
    if was_created:
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [HEADER]},
        ).execute()
        _apply_full_formatting(svc, sheet_id)
        print(f"Created '{NEWS_TAB}' tab with header + formatting.")
    else:
        # Make sure header row is present even if user accidentally cleared it
        cur = svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A1:P1"
        ).execute().get("values", [[]])
        if not cur or not cur[0]:
            svc.spreadsheets().values().update(
                spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A1",
                valueInputOption="USER_ENTERED",
                body={"values": [HEADER]},
            ).execute()
            _apply_full_formatting(svc, sheet_id)
            print(f"Re-applied formatting to existing '{NEWS_TAB}' tab.")

    # Dedup against URLs already in the sheet
    seen = _existing_keys(svc, NEWS_TAB)
    fresh: list[dict] = []
    for c in clean:
        if c["primary_url"] in seen or c["canonical_url"] in seen:
            continue
        # Also skip if any cluster member URL is already in the sheet — avoids
        # adding the same story under a different "canonical" URL.
        if any(m["url"] in seen for m in c["members"]):
            continue
        fresh.append(c)
    print(f"After dedup against existing sheet: {len(fresh)} new clusters")
    if not fresh:
        print("Nothing new to write.")
        return 0

    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_rows = [_row_for_cluster(c, run_ts) for c in fresh]
    sections_in_order = [c.get("section", "") or "" for c in fresh]

    # Prepend new rows just below the header (row index 1, 0-based)
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": [{
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id, "dimension": "ROWS",
                    "startIndex": 1, "endIndex": 1 + len(new_rows),
                },
                "inheritFromBefore": False,
            }
        }]}
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A2",
        valueInputOption="USER_ENTERED",
        body={"values": new_rows},
    ).execute()

    # Tint the section cells for the newly inserted rows (row 2 .. 2+N)
    _tint_section_cells(
        svc, sheet_id,
        start_data_row=1,  # 0-based index of the first new row
        sections_in_order=sections_in_order,
    )

    flagged = sum(1 for r in new_rows if r[13])
    multi = sum(1 for c in fresh if c["size"] > 1)
    print(f"Prepended {len(new_rows)} new clusters to '{NEWS_TAB}'.")
    print(f"  - multi-source clusters: {multi}")
    print(f"  - flagged for review: {flagged}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
