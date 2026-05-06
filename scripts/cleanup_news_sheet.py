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
    _LIFESTYLE_TITLE_PHRASES,
    _NON_ARTICLE_URL_HINTS,
    _NON_ARTICLE_EXTENSIONS,
    _OP_ED_TITLE_PHRASES,
    _title_has_auto_signal,
    is_dzen_listicle,
    is_supplier_abstract_showcase,
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


_PLACEHOLDER_TITLES = {"<unknown>", "unknown", "untitled", "no title", "n/a", "—", "-"}

# Old TRANSLATE_SYSTEM (pre-round-3) preserved Дзен-style adjectives in EN
# translations. The new prompt strips them, but already-translated rows
# stay bad. Delete them retroactively — re-translation is too expensive.
# Each phrase here is something the editor flagged as yellow-press AND
# the new prompt would never produce.
_LEGACY_YELLOW_PRESS_PHRASES = (
    # English (post-translation)
    "with reliable engine",
    "with durable engine",
    "with reliable",
    "more affordable",
    "became more affordable",
    "russians found way",
    "experts assess",
    "experts compared",
    "experts forecast",
    "how dealerships profit",
    "how car dealerships profit",
    "best-selling used car",
    "with best acoustics",
    "best acoustics: rating",
    "cheaper than haval",
    "cheaper than kgm",
    "cheaper than rivals",
    # Russian (post-translation)
    "с надёжным двигателем",
    "с долговечным двигателем",
    "с надёжным мотором",
    "стал доступнее",
    "стала доступнее",
    "россияне нашли способ",
    "россиян предупредили",
    "эксперты оценили",
    "эксперты сравнили",
    "эксперты спрогнозировали",
    "как автосалоны наживаются",
    "лучший по акустике",
)


def _is_placeholder_title(combined: str) -> bool:
    """True if EN portion or full title is empty / UNKNOWN-style placeholder."""
    if not combined:
        return True
    # Sheet titles look like:  "EN: <english>\nRU: <russian>"
    en_part = ""
    for line in combined.splitlines():
        ll = line.strip()
        if ll.lower().startswith("en:"):
            en_part = ll[3:].strip()
            break
    candidate = (en_part or combined.strip()).lower().strip("«»\"' ")
    if not candidate:
        return True
    return candidate in _PLACEHOLDER_TITLES


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

        # 0) Empty / UNKNOWN placeholder titles — heuristic now hard-rejects these
        if _is_placeholder_title(title):
            rows_to_delete.append((i, "placeholder/UNKNOWN title", title[:80] or "<empty>"))
            continue

        url = _get(r, COL_URL)
        member_urls = _get(r, COL_MEMBER_URLS)
        all_urls = [url] + [u.strip() for u in member_urls.splitlines() if u.strip()]

        # 1) Domain blacklist (whole-domain blocks)
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
            # 2) Non-article URL hints — but ONLY check member URLs (col M),
            # NOT the primary URL. Primary URL might have been mis-picked
            # (e.g. /contact.html on a brand site) while the actual article
            # in members is legitimate. Better to fix primary via
            # backfill_primary_source.py than nuke the row.
            article_urls = [u.strip() for u in member_urls.splitlines() if u.strip()]
            url_hit = False
            for u in article_urls:
                low = u.lower()
                if any(h in low for h in _NON_ARTICLE_URL_HINTS):
                    matched = next(h for h in _NON_ARTICLE_URL_HINTS if h in low)
                    rows_to_delete.append((i, f"non-article URL: {matched}", title[:80]))
                    url_hit = True
                    break
                if any(low.endswith(ext) for ext in _NON_ARTICLE_EXTENSIONS):
                    rows_to_delete.append((i, "binary doc URL", title[:80]))
                    url_hit = True
                    break
            if not url_hit:
                # 3a) Force-reject (editor opted-out categories — no brand override)
                title_lower = _title_only(title)
                hit = False
                for phrase in _FORCE_REJECT_PHRASES:
                    if phrase in title_lower:
                        rows_to_delete.append(
                            (i, f"force-reject: {phrase!r}", title[:80])
                        )
                        hit = True
                        break
                if hit:
                    continue
                # 3b) Topic blacklist phrase (with brand-override)
                phrase_hit = False
                for phrase in bl.all_phrases():
                    if not phrase or phrase not in title_lower:
                        continue
                    if _title_has_auto_signal(title_lower, brands):
                        continue
                    rows_to_delete.append((i, f"blacklist phrase: {phrase!r}", title[:80]))
                    phrase_hit = True
                    break
                if phrase_hit:
                    continue
                # 3c) Op-ed / long-form opinion (editor category 6)
                op_ed_hit = False
                for phrase in _OP_ED_TITLE_PHRASES:
                    if phrase in title_lower:
                        rows_to_delete.append((i, f"op-ed title: {phrase!r}", title[:80]))
                        op_ed_hit = True
                        break
                if op_ed_hit:
                    continue
                # 3d) Lifestyle / tourism (editor category 9)
                lifestyle_hit = False
                for phrase in _LIFESTYLE_TITLE_PHRASES:
                    if phrase in title_lower:
                        rows_to_delete.append((i, f"lifestyle title: {phrase!r}", title[:80]))
                        lifestyle_hit = True
                        break
                if lifestyle_hit:
                    continue
                # 3e) Legacy yellow-press translations (pre-round-3 era)
                yp_hit = False
                for phrase in _LEGACY_YELLOW_PRESS_PHRASES:
                    if phrase in title_lower:
                        rows_to_delete.append(
                            (i, f"legacy yellow-press: {phrase!r}", title[:80])
                        )
                        yp_hit = True
                        break
                if yp_hit:
                    continue
                # 3f) Dzen-listicle: "X, Y and N more...", "5 best/worst..."
                # Use the same regex detector as the runtime pipeline.
                # Check both EN and RU lines individually.
                listicle_hit = False
                for line in title.splitlines():
                    ll = line.strip()
                    if ll.lower().startswith("en:"):
                        ll = ll[3:].strip()
                    elif ll.lower().startswith("ru:"):
                        ll = ll[3:].strip()
                    if is_dzen_listicle(ll):
                        rows_to_delete.append(
                            (i, "dzen-listicle pattern", title[:80])
                        )
                        listicle_hit = True
                        break
                if listicle_hit:
                    continue
                # 3g) Supplier-abstract showcase at motorshow
                # (component vendor + abstract noun + motorshow → reject)
                supplier_hit = False
                for line in title.splitlines():
                    ll = line.strip()
                    if ll.lower().startswith("en:"):
                        ll = ll[3:].strip()
                    elif ll.lower().startswith("ru:"):
                        ll = ll[3:].strip()
                    if is_supplier_abstract_showcase(ll):
                        rows_to_delete.append(
                            (i, "supplier-abstract showcase", title[:80])
                        )
                        supplier_hit = True
                        break
                if supplier_hit:
                    continue

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
