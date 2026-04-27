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
from rapidfuzz import fuzz

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.core.config_loader import load_brand_domains  # noqa: E402

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
# Spec-sheet / static-document URL fragments — these pages have an URL like
# "/technical-specifications-of-the-mini-…-valid-from-11/2024" and they are
# archived reference documents, not news. They have no `published_at` so the
# freshness filter can't reject them upstream.
_STATIC_DOC_URL_HINTS = (
    "/technical-specifications-",
    "valid-from-",
    "/specifications/",
    "/data-sheet",
    "/factsheet",
)


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
    # Static reference documents (spec sheets) detected by URL pattern
    canon_url = (c.get("canonical_url") or "").lower()
    primary_url = (c.get("primary_url") or "").lower()
    member_urls = " ".join(m.get("url", "").lower() for m in c.get("members", []))
    haystack = canon_url + " " + primary_url + " " + member_urls
    if any(h in haystack for h in _STATIC_DOC_URL_HINTS):
        return True
    # LLM-failed clusters: title is just the original scraped string,
    # no "EN:"/"RU:" prefix → LLM didn't translate. Skip them to avoid
    # half-broken rows on the sheet. These will be re-classified after
    # the API cap resets.
    if "EN:" not in title and "RU:" not in title:
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


def _existing_state(svc, tab: str) -> dict:
    """Load everything from the Новости tab needed for dedup + anti-dup.

    Returns a dict with:
      - ``url_to_row``: every URL → 1-based sheet row that already holds it
        (covers both column J primary URL and the newline list in column M)
      - ``rows_meta``: list of {sheet_row, title, normalised, member_urls}
        used by the fuzzy anti-dup matcher
      - ``date_by_row``: 1-based row → current value of column G (Дата
        публикации); empty string if the cell is blank. Lets the dedup
        logic patch missing dates without rewriting other cells.
    """
    try:
        resp = svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"'{tab}'!A2:P"
        ).execute()
    except Exception:  # noqa: BLE001
        return {"url_to_row": {}, "rows_meta": [], "date_by_row": {}}
    rows = resp.get("values", []) or []
    url_to_row: dict[str, int] = {}
    rows_meta: list[dict] = []
    date_by_row: dict[int, str] = {}
    for i, r in enumerate(rows, start=2):  # row index 2 = first data row
        title = r[1] if len(r) > 1 else ""
        primary_url = r[9] if len(r) > 9 else ""
        member_urls_cell = r[12] if len(r) > 12 else ""
        date_value = r[6] if len(r) > 6 else ""
        date_by_row[i] = date_value or ""
        members: list[str] = []
        if primary_url:
            url_to_row[primary_url] = i
            members.append(primary_url)
        for u in (member_urls_cell or "").splitlines():
            u = u.strip()
            if u:
                url_to_row[u] = i
                if u not in members:
                    members.append(u)
        if title:
            rows_meta.append({
                "sheet_row": i,
                "title": title,
                "normalised": _normalise_for_match(title),
                "members": members,
            })
    return {
        "url_to_row": url_to_row,
        "rows_meta": rows_meta,
        "date_by_row": date_by_row,
    }


# ----- Fuzzy anti-dup matcher --------------------------------------------
def _normalise_for_match(t: str) -> str:
    """Strip lang tags / EN: / RU: prefixes / source suffix, keep only the
    semantic skeleton for fuzzy comparison."""
    if not t:
        return ""
    t = t.lower()
    # Drop "EN:" / "RU:" prefixes (sometimes both lines appear)
    t = re.sub(r"^en:\s*", "", t)
    t = re.sub(r"\n\s*ru:\s*", " | ", t)
    t = re.sub(r"^ru:\s*", "", t)
    # Drop language tags
    t = re.sub(r"\([a-zа-яё]{2,4}\)", "", t)
    # Drop "— Source" / "| Source" trailing names
    t = re.sub(r"[—\-|]\s*[a-zа-я0-9 \.&]+$", "", t)
    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _build_brand_lexicon() -> list[str]:
    """List of lowercase brand names (≥4 chars) used as the cluster guard."""
    out: list[str] = []
    try:
        for b in load_brand_domains():
            out.append(b.brand.lower())
            for a in getattr(b, "aliases", []) or []:
                out.append(a.lower())
    except Exception:  # noqa: BLE001
        pass
    return [b for b in out if len(b) >= 4]


def _find_existing_match(
    new_cluster: dict,
    existing_meta: list[dict],
    brand_lex: list[str],
    threshold: int = 72,
) -> int | None:
    """Return the sheet row of an existing cluster that the new cluster
    duplicates (different URL but same story), or None."""
    new_norm = _normalise_for_match(new_cluster["canonical_title"])
    if not new_norm:
        return None
    new_brands = {b for b in brand_lex if b in new_norm}
    best: tuple[int, int] | None = None  # (similarity, sheet_row)
    for ex in existing_meta:
        ex_norm = ex["normalised"]
        if not ex_norm:
            continue
        sim = fuzz.token_set_ratio(new_norm, ex_norm)
        if sim < threshold:
            continue
        ex_brands = {b for b in brand_lex if b in ex_norm}
        # Brand guard — both sides have brand tokens AND they share at
        # least one. If neither side has a brand, fall through (purely
        # title-based — riskier but rare).
        if new_brands and ex_brands and not (new_brands & ex_brands):
            continue
        if best is None or sim > best[0]:
            best = (sim, ex["sheet_row"])
    return best[1] if best else None


# Generic / corporate images that some press sites pin into og:image for
# every article. Filtering them out prevents misleading thumbnails like
# "BMW HQ building 2016" appearing next to a story about a specific car.
_GENERIC_IMAGE_HINTS = (
    "corporate-headquarters",
    "corporate_headquarters",
    "logo.svg",
    "logo.png",
    "default-og",
    "default_og",
    "placeholder",
    "/share-default",
    "social-default",
)


def _clean_image_url(url: str) -> str:
    if not url:
        return ""
    low = url.lower()
    if any(h in low for h in _GENERIC_IMAGE_HINTS):
        return ""
    return url


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
        _clean_image_url(c.get("image_url", "") or ""),
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


def _apply_body_format_block(
    svc,
    sheet_id: int,
    *,
    start_row_zero_based: int,
    end_row_zero_based: int,
) -> None:
    """Re-apply the body-cell formatting (wrap, align, font) to a slice of
    rows. Required after insertDimension because newly-inserted rows are
    created with default cell formatting, ignoring whatever we set on the
    range at sheet-creation time.
    """
    requests: list[dict] = []
    n_cols = len(HEADER)

    # Wrap + top-align on text-heavy columns
    for col in (1, 2, 9, 12, 13, 15):
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row_zero_based,
                    "endRowIndex": end_row_zero_based,
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
    # Middle-align + wrap on metadata columns
    for col in (0, 3, 4, 5, 6, 7, 8, 10, 11, 14):
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row_zero_based,
                    "endRowIndex": end_row_zero_based,
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
                    "sheetId": sheet_id,
                    "startRowIndex": start_row_zero_based,
                    "endRowIndex": end_row_zero_based,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment",
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

    # Pull existing state — URLs already in the sheet and per-row meta
    # for fuzzy anti-dup matching.
    state = _existing_state(svc, NEWS_TAB)
    url_to_row: dict[str, int] = state["url_to_row"]
    existing_meta: list[dict] = state["rows_meta"]
    date_by_row: dict[int, str] = state["date_by_row"]
    brand_lex = _build_brand_lexicon()

    fresh: list[dict] = []                         # truly new → prepend
    merge_into: list[tuple[int, dict]] = []         # fuzzy match → extend row
    date_patches: dict[int, str] = {}               # row → ISO date to write
    skip_exact = 0                                  # exact URL already there

    def _maybe_patch_date(row: int, c: dict) -> None:
        """If the existing row has no date but the new cluster does, copy it."""
        new_date = c.get("published") or ""
        if not new_date:
            return
        if (date_by_row.get(row) or "").strip():
            return  # existing row already has a date — don't overwrite
        # Avoid double-scheduling: keep the first non-empty value we see
        if row not in date_patches:
            date_patches[row] = new_date

    for c in clean:
        # 1) Exact URL hit — story already on the sheet
        urls_in_cluster = {c["primary_url"], c["canonical_url"]}
        urls_in_cluster.update(m["url"] for m in c["members"])
        urls_in_cluster.discard("")
        existing_row_by_url = None
        for u in urls_in_cluster:
            if u in url_to_row:
                existing_row_by_url = url_to_row[u]
                break
        if existing_row_by_url:
            skip_exact += 1
            _maybe_patch_date(existing_row_by_url, c)
            continue

        # 2) Fuzzy anti-dup — same story, different URL
        match_row = _find_existing_match(c, existing_meta, brand_lex)
        if match_row:
            merge_into.append((match_row, c))
            _maybe_patch_date(match_row, c)
            continue

        # 3) Truly new — schedule for prepend
        fresh.append(c)

    print(
        f"Dedup result:\n"
        f"  - already on sheet (exact URL): {skip_exact}\n"
        f"  - merged into existing row (fuzzy match): {len(merge_into)}\n"
        f"  - date patches (existing row was missing date): {len(date_patches)}\n"
        f"  - new (will be prepended): {len(fresh)}"
    )

    # ---- Date patches into existing rows --------------------------------
    if date_patches:
        patch_data = [
            {"range": f"'{NEWS_TAB}'!G{row}", "values": [[iso]]}
            for row, iso in date_patches.items()
        ]
        CHUNK = 200
        for i in range(0, len(patch_data), CHUNK):
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"valueInputOption": "USER_ENTERED", "data": patch_data[i:i + CHUNK]},
            ).execute()

    # ---- Apply the fuzzy-merge: extend the existing row's URL list +1 ---
    if merge_into:
        # Build per-row updates for cols L (Источников) and M (Все URL)
        # Group by sheet_row in case multiple clusters merge into one.
        per_row: dict[int, dict] = {}
        for sheet_row, c in merge_into:
            new_urls = [m["url"] for m in c["members"] if m["url"]]
            entry = per_row.setdefault(sheet_row, {"new_urls": []})
            for u in new_urls:
                if u not in entry["new_urls"]:
                    entry["new_urls"].append(u)
        # Read current Источников counts and URL lists in one call
        read = svc.spreadsheets().values().batchGet(
            spreadsheetId=SHEET_ID,
            ranges=[f"'{NEWS_TAB}'!L{r}:M{r}" for r in per_row],
        ).execute().get("valueRanges", [])
        update_data: list[dict] = []
        for sheet_row, vr in zip(per_row.keys(), read):
            cells = vr.get("values", [[]])[0] if vr.get("values") else []
            current_count = int(cells[0]) if cells and cells[0].isdigit() else 1
            current_urls_cell = cells[1] if len(cells) > 1 else ""
            current_urls = [u.strip() for u in current_urls_cell.splitlines() if u.strip()]
            for u in per_row[sheet_row]["new_urls"]:
                if u not in current_urls:
                    current_urls.append(u)
                    current_count += 1
            update_data.append({
                "range": f"'{NEWS_TAB}'!L{sheet_row}",
                "values": [[str(current_count)]],
            })
            update_data.append({
                "range": f"'{NEWS_TAB}'!M{sheet_row}",
                "values": [["\n".join(current_urls)]],
            })
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": update_data},
        ).execute()

    # ---- Prepend truly new clusters --------------------------------------
    if not fresh:
        print("Nothing new to add at the top.")
        return 0

    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_rows = [_row_for_cluster(c, run_ts) for c in fresh]
    sections_in_order = [c.get("section", "") or "" for c in fresh]

    # Build a separator row that visually divides today's batch from the
    # previously-loaded news. Goes at row 2, everything else shifts down.
    run_human = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    n_clusters = len(new_rows)
    separator_text = (
        f"━━  Прогон от {run_human}: добавлено {n_clusters} новых сюжетов  ━━"
    )
    separator_row = [separator_text] + [""] * (len(HEADER) - 1)

    # 1 separator + N data rows = N+1 inserted rows
    n_insert = 1 + len(new_rows)
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": [{
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id, "dimension": "ROWS",
                    "startIndex": 1, "endIndex": 1 + n_insert,
                },
                "inheritFromBefore": False,
            }
        }]}
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A2",
        valueInputOption="USER_ENTERED",
        body={"values": [separator_row] + new_rows},
    ).execute()

    # Re-apply body formatting to the newly-inserted data rows. Inserted
    # rows arrive with default cell format (no wrap, no fontSize, bottom-
    # aligned) which makes them look broken next to the previously styled
    # block below the separator.
    first_data_zero_based = 2  # row index 2 = separator at idx 1, data starts here
    last_data_zero_based = first_data_zero_based + len(new_rows)
    _apply_body_format_block(
        svc, sheet_id,
        start_row_zero_based=first_data_zero_based,
        end_row_zero_based=last_data_zero_based,
    )

    # Style the separator: merge, dark background, white bold text, taller row
    _style_separator_row(svc, sheet_id, row_index_zero_based=1)

    # Tint section cells for the new data rows (rows 3 .. 2+N+1)
    _tint_section_cells(
        svc, sheet_id,
        start_data_row=2,  # 0-based: separator was 1, first data row is 2
        sections_in_order=sections_in_order,
    )

    flagged = sum(1 for r in new_rows if r[13])
    multi = sum(1 for c in fresh if c["size"] > 1)
    print(f"Prepended {len(new_rows)} new clusters under run-separator '{run_human}'.")
    print(f"  - multi-source clusters within new batch: {multi}")
    print(f"  - flagged for review: {flagged}")
    return 0


def _style_separator_row(svc, sheet_id: int, *, row_index_zero_based: int) -> None:
    """Visually mark a separator row: dark slate band across all columns,
    bold white text in column A, taller row.

    Cells aren't merged because Sheets refuses to merge across frozen /
    non-frozen column boundaries. Painting the whole row + putting text
    only in column A produces the same visual effect.
    """
    n_cols = len(HEADER)
    requests = [
        # Dark band across the whole row
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_index_zero_based,
                    "endRowIndex": row_index_zero_based + 1,
                    "startColumnIndex": 0, "endColumnIndex": n_cols,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.20, "green": 0.25, "blue": 0.30},
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        },
        # Bold white centred text in the first cell — that's where the
        # separator caption lives (column A, value of `separator_row[0]`).
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_index_zero_based,
                    "endRowIndex": row_index_zero_based + 1,
                    "startColumnIndex": 0, "endColumnIndex": 1,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "bold": True, "fontSize": 11,
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                        },
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "OVERFLOW_CELL",
                    }
                },
                "fields": (
                    "userEnteredFormat(textFormat,horizontalAlignment,"
                    "verticalAlignment,wrapStrategy)"
                ),
            }
        },
        # Slightly taller row for the band to read clearly
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id, "dimension": "ROWS",
                    "startIndex": row_index_zero_based,
                    "endIndex": row_index_zero_based + 1,
                },
                "properties": {"pixelSize": 32},
                "fields": "pixelSize",
            }
        },
    ]
    svc.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": requests}).execute()


if __name__ == "__main__":
    sys.exit(main())
