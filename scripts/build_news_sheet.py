"""Build (or refresh) the persistent 'Новости' sheet from a clusters JSON.

Behaviour:
  - Reads data/clusters_<tab>.json
  - Filters out junk clusters (aggregator / nav / "{Brand}" stub titles)
  - Flags year-mismatch (future verb + past year) rows for editor review
  - Creates the 'Новости' tab if missing, sets header / formatting
  - Writes / refreshes rows. New clusters land at the TOP (insertDimension)

Columns of the Новости sheet (16 visible + 5 hidden debug):
   A  Прогон (UTC)
   B  Заголовок (EN / RU)
   C  Лид
   D  Раздел
   E  Регион
   F  Страна
   G  Дата публикации
   H  Картинка (URL)
   I  Первоисточник (домен)
   J  Первоисточник URL
   K  Уверенность источника
   L  Источников
   M  Все URL источников
   N  Флаг проверки
   O  Ручная проверка
   P  Комментарий
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
    "Прогон (UTC)",
    "Заголовок (EN / RU)",
    "Лид",
    "Раздел",
    "Регион",
    "Страна",
    "Дата публикации",
    "Картинка (URL)",
    "Первоисточник (домен)",
    "Первоисточник URL",
    "Уверенность источника",
    "Источников",
    "Все URL источников",
    "Флаг проверки",
    "Ручная проверка",
    "Комментарий",
]


# Detect garbage / aggregator titles that slipped past the heuristic.
def _is_junk_cluster(c: dict) -> bool:
    title = c["canonical_title"].strip()
    # Strip "EN: ... RU: ..." prefixes for length analysis
    text = re.sub(r"\s*EN:\s*", "", title)
    text = re.sub(r"\s*RU:\s*", " ", text)
    text = re.sub(r"\s*\([A-Za-zА-Яа-яЁё]{2,4}\)\s*", "", text).strip()
    # Single-word titles that are essentially the brand only
    if len(text.split()) <= 2:
        return True
    # PressClub / Newsroom landing-page heuristics
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
    # Empty lede + short title = likely a nav page
    lede = c.get("canonical_lede", "").strip()
    if len(lede) < 50 and len(text.split()) < 6:
        return True
    return False


_PAST_YEARS = {y for y in range(2020, 2026)}
_FUTURE_VERBS = (
    "will ", "to arrive", "to launch", "to open", "to expand",
    "to start", "to debut", "to introduce", "to build", "плани",
    "появится", "запустит", "откроет", "выйдет", "начнёт",
    "стартует", "дебютирует",
)
_YEAR_RE = re.compile(r"\b(20\d{2})\b")
# Match every number ≥ 3 digits OR a smaller number with a clear unit.
# We intentionally ignore tiny numbers like "5 моделей" — too noisy.
_BIG_NUMBER_RE = re.compile(r"\b\d{3,}(?:[.,]\d+)?\b")
_UNIT_NUMBER_RE = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*(?:%|млн|млрд|тыс|million|billion|trillion|thousand)\b",
    re.IGNORECASE,
)


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


def _extract_entities(s: str) -> tuple[set[str], set[str]]:
    """Return (years, numeric tokens) found in the headline.

    Numeric tokens are normalised: ``925,7`` and ``925.7`` collapse to
    the same key, units are stripped. We only keep numbers that are
    "big" (≥3 digits) or carry an explicit magnitude unit (млн, billion,
    %) — small standalone digits like "5" in "5 моделей" are too noisy
    to compare across languages.
    """
    s = _strip_lang_tag(s)
    years = set(_YEAR_RE.findall(s))

    def _norm(num: str) -> str:
        return num.replace(",", ".")

    nums: set[str] = set()
    # Big multi-digit numbers
    for m in _BIG_NUMBER_RE.finditer(s):
        token = m.group(0)
        if token in years:  # don't double-count years as generic numbers
            continue
        nums.add(_norm(token))
    # Small numbers with explicit magnitude unit
    for m in _UNIT_NUMBER_RE.finditer(s):
        # Pick out just the digits part
        num_part = re.search(r"\d+(?:[.,]\d+)?", m.group(0)).group(0)
        nums.add(_norm(num_part))
    return years, nums


def _flag_review(title: str) -> str:
    """Return a non-empty marker if the row deserves manual review.

    Two cases are caught:
      - Past year + future tense (will arrive in 2024) — the LLM picked up
        a stale date from the body.
      - Year present in only one of the two language lines — usually the
        LLM dropped a key time qualifier.

    Numeric mismatches (percentages, thousand-separated large numbers)
    used to be flagged too but produced too many false positives because
    of "300,000" vs "300 000" and untranslated unit words. Trust the LLM
    on numbers; rely on editor review for the rest.
    """
    en, ru = _split_lines(title)

    # 1) Past-year-as-future check
    for line, lang in ((en, "EN"), (ru, "RU")):
        text_lower = line.lower()
        if any(v in text_lower for v in _FUTURE_VERBS):
            for y in _PAST_YEARS:
                if str(y) in line:
                    return f"⚠ возможно неверный год {y} (в {lang}-строке)"

    # 2) Year missing in one language but present in the other
    if en and ru:
        en_years = set(_YEAR_RE.findall(_strip_lang_tag(en)))
        ru_years = set(_YEAR_RE.findall(_strip_lang_tag(ru)))
        only_en = en_years - ru_years
        only_ru = ru_years - en_years
        if only_en or only_ru:
            both = sorted(only_en | only_ru)
            return f"⚠ EN/RU расходятся по годам: {', '.join(both)}"

    return ""


# Compatibility shim for older callers that imported _flag_year
def _flag_year(title: str) -> str | None:
    res = _flag_review(title)
    return res or None


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _ensure_tab(svc, tab: str) -> int:
    """Create the tab if missing; return its sheetId."""
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab:
            return int(s["properties"]["sheetId"])
    # Create
    resp = svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
    ).execute()
    new = resp["replies"][0]["addSheet"]["properties"]
    return int(new["sheetId"])


def _existing_urls(svc, tab: str) -> set[str]:
    """Return all primary URLs already present in the news tab."""
    try:
        resp = svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"'{tab}'!J2:J"
        ).execute()
    except Exception:  # noqa: BLE001
        return set()
    vals = resp.get("values", []) or []
    return {row[0] for row in vals if row and row[0]}


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
        "",  # Ручная проверка
        "",  # Комментарий
    ]


def _apply_formatting(svc, sheet_id: int, n_data_rows: int) -> None:
    """One-time formatting: header style, freeze, col widths, wrap."""
    requests = [
        # Frozen header
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1, "frozenColumnCount": 2},
                },
                "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
            }
        },
        # Header bold + green band
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                    "startColumnIndex": 0, "endColumnIndex": len(HEADER),
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True, "fontSize": 11},
                        "backgroundColor": {"red": 0.80, "green": 0.93, "blue": 0.80},
                        "wrapStrategy": "WRAP",
                        "verticalAlignment": "MIDDLE",
                    }
                },
                "fields": "userEnteredFormat(textFormat,backgroundColor,wrapStrategy,verticalAlignment)",
            }
        },
    ]
    # Col widths
    widths = {
        0: 145,   # Прогон
        1: 380,   # Заголовок
        2: 360,   # Лид
        3: 140,   # Раздел
        4: 70,    # Регион
        5: 100,   # Страна
        6: 165,   # Дата
        7: 240,   # Картинка URL
        8: 200,   # Первоисточник домен
        9: 320,   # Первоисточник URL
        10: 110,  # Уверенность
        11: 90,   # Источников
        12: 320,  # Все URL
        13: 220,  # Флаг
        14: 200,  # Ручная проверка
        15: 220,  # Комментарий
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
    # Wrap on text-heavy columns
    end_row = max(n_data_rows + 1, 2000)
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
                    }
                },
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)",
            }
        })
    # Conditional formatting: rows with year flag get a soft red highlight
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": 0, "endColumnIndex": len(HEADER),
                }],
                "booleanRule": {
                    "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [{"userEnteredValue": '=$N2<>""'}],
                    },
                    "format": {"backgroundColor": {"red": 1.0, "green": 0.86, "blue": 0.78}},
                },
            },
            "index": 0,
        }
    })
    # Row with size > 1 gets a soft blue (multi-source story)
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{
                    "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": end_row,
                    "startColumnIndex": 0, "endColumnIndex": len(HEADER),
                }],
                "booleanRule": {
                    "condition": {
                        "type": "NUMBER_GREATER",
                        "values": [{"userEnteredValue": "1"}],
                    },
                    "format": {"backgroundColor": {"red": 0.82, "green": 0.92, "blue": 0.98}},
                },
            },
            "index": 1,
        }
    })
    svc.spreadsheets().batchUpdate(spreadsheetId=SHEET_ID, body={"requests": requests}).execute()


def main() -> int:
    src_tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ статьи v18"
    clusters_path = ROOT / "data" / f"clusters_{src_tab.replace(' ', '_')}.json"
    if not clusters_path.exists():
        print(f"Clusters file not found: {clusters_path}", file=sys.stderr)
        print(f"Run scripts/build_news_clusters.py first.", file=sys.stderr)
        return 2

    clusters = json.loads(clusters_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(clusters)} clusters from {clusters_path.name}")

    # Filter junk
    clean: list[dict] = []
    junk_count = 0
    for c in clusters:
        if _is_junk_cluster(c):
            junk_count += 1
            continue
        clean.append(c)
    print(f"After junk filter: {len(clean)} clusters ({junk_count} junk dropped)")

    svc = _svc()
    sheet_id = _ensure_tab(svc, NEWS_TAB)

    # Make sure the header row is there (idempotent — write only if A1 empty).
    cur_header = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A1:P1"
    ).execute().get("values", [[]])
    if not cur_header or not cur_header[0]:
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [HEADER]},
        ).execute()
        _apply_formatting(svc, sheet_id, n_data_rows=len(clean))
        print(f"Created '{NEWS_TAB}' tab with header + formatting.")

    # Dedup against URLs already in the sheet
    seen = _existing_urls(svc, NEWS_TAB)
    fresh = [c for c in clean if c["primary_url"] not in seen and c["canonical_url"] not in seen]
    print(f"After dedup against existing sheet: {len(fresh)} new clusters")
    if not fresh:
        print("Nothing new to write.")
        return 0

    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    new_rows = [_row_for_cluster(c, run_ts) for c in fresh]

    # Prepend at top: insert N rows after the header (row index 1, 0-based)
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
    flagged = sum(1 for r in new_rows if r[13])
    print(f"Prepended {len(new_rows)} new clusters to '{NEWS_TAB}'.")
    print(f"  - flagged for review (year mismatch): {flagged}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
