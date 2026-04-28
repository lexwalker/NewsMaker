"""Remove rows from the persistent 'Новости' tab that match new junk
filters. Useful when the editor flags issues that the latest filtering
rules can fix retroactively without re-running the whole pipeline.

Reasons a row gets deleted:
  - URL contains a non-article hint (/shop/, /catalog/, /slavery-statement, …)
  - Domain is in the configured blacklist (parking.mos.ru, …)
  - Title contains a blacklisted phrase (clickbait, ESG compliance, traffic
    infrastructure, etc.) AND the title has no auto-signal that overrides

Run:  python scripts/cleanup_news_sheet.py
"""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.core.config_loader import load_blacklist, load_brand_domains  # noqa: E402
from news_agent.core.heuristic_relevance import (  # noqa: E402
    _FORCE_REJECT_PHRASES,
    _NON_ARTICLE_URL_HINTS,
    _NON_ARTICLE_EXTENSIONS,
    _title_has_auto_signal,
)
from news_agent.core.urls import domain_of  # noqa: E402

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

NEWS_TAB = "Новости"
COL_TITLE = 1
COL_URL = 9
COL_MEMBER_URLS = 12


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(r: list[str], i: int) -> str:
    return r[i] if i < len(r) else ""


def _title_only(combined: str) -> str:
    """Extract a single line for blacklist phrase matching."""
    if not combined:
        return ""
    s = combined.lower()
    return s.replace("\n", " ")


def main() -> int:
    bl = load_blacklist()
    brands = load_brand_domains()

    svc = _svc()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A2:P"
    ).execute()
    rows = resp.get("values", []) or []
    print(f"Loaded {len(rows)} rows from '{NEWS_TAB}'.")

    rows_to_delete: list[tuple[int, str, str]] = []  # (sheet_row, reason, preview)
    for i, r in enumerate(rows, start=2):
        title = _get(r, COL_TITLE)
        if not title or "EN:" not in title:
            continue
        url = _get(r, COL_URL)
        member_urls = _get(r, COL_MEMBER_URLS)
        all_urls = [url] + [u.strip() for u in member_urls.splitlines() if u.strip()]

        # 1) Domain blacklist
        for u in all_urls:
            d = domain_of(u)
            for blocked in bl.domains:
                if d == blocked or d.endswith("." + blocked):
                    rows_to_delete.append((i, f"blacklisted domain: {blocked}", title[:80]))
                    break
            else:
                continue
            break
        else:
            # 2) Non-article URL hints
            for u in all_urls:
                low = u.lower()
                if any(h in low for h in _NON_ARTICLE_URL_HINTS):
                    matched = next(h for h in _NON_ARTICLE_URL_HINTS if h in low)
                    rows_to_delete.append((i, f"non-article URL: {matched}", title[:80]))
                    break
                if any(low.endswith(ext) for ext in _NON_ARTICLE_EXTENSIONS):
                    rows_to_delete.append((i, "binary doc URL", title[:80]))
                    break
            else:
                # 3a) Force-reject (clickbait / yellow press, no override)
                title_lower = _title_only(title)
                hit = False
                for phrase in _FORCE_REJECT_PHRASES:
                    if phrase in title_lower:
                        rows_to_delete.append((i, f"clickbait: {phrase!r}", title[:80]))
                        hit = True
                        break
                if hit:
                    continue
                # 3b) Topic blacklist phrase (with brand-override)
                for phrase in bl.all_phrases():
                    if not phrase or phrase not in title_lower:
                        continue
                    if _title_has_auto_signal(title_lower, brands):
                        continue
                    rows_to_delete.append((i, f"blacklist phrase: {phrase!r}", title[:80]))
                    break

    print(f"\nRows to delete: {len(rows_to_delete)}")
    for sheet_row, reason, preview in rows_to_delete[:30]:
        print(f"  Row {sheet_row}: {reason}")
        print(f"    {preview}")
    if len(rows_to_delete) > 30:
        print(f"  ... and {len(rows_to_delete) - 30} more")

    if not rows_to_delete:
        return 0

    # Get sheet ID
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    sheet_id = None
    for s in meta["sheets"]:
        if s["properties"]["title"] == NEWS_TAB:
            sheet_id = s["properties"]["sheetId"]
            break

    # Delete bottom-up so indices don't shift mid-batch
    requests: list[dict] = []
    for sheet_row, _, _ in sorted(rows_to_delete, key=lambda t: -t[0]):
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id, "dimension": "ROWS",
                    "startIndex": sheet_row - 1,  # 0-based
                    "endIndex": sheet_row,
                }
            }
        })
    CHUNK = 100
    for i in range(0, len(requests), CHUNK):
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID, body={"requests": requests[i:i + CHUNK]}
        ).execute()
    print(f"\nDeleted {len(rows_to_delete)} rows.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
