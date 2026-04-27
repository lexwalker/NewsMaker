"""Re-run LLM only on rows whose ``Пометка бота`` (col M) carries an error
trace from a previous batch (e.g. ``classify error: 400 ... usage limits``).

Used when the API hit a transient hard-cap mid-run; once the cap is lifted
this script touches only the broken rows and rewrites their LLM fields in
place. Far cheaper than a full v19 re-fetch.

Run:  python scripts/retry_failed_llm.py "ТЕСТ статьи v18"
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

from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.core.budget import BudgetTracker  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.core.models import RawArticle  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column indices (matches write_articles())
COL_TITLE = 1
COL_LEDE = 2
COL_URL = 3
COL_SECTION = 4
COL_REGION = 5
COL_NOTE = 12
COL_VERDICT = 14
COL_LLM_REL = 24
COL_COST = 25


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(r: list[str], i: int) -> str:
    return r[i] if i < len(r) else ""


def main() -> int:
    tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ статьи v18"
    settings = get_settings()
    sections = load_sections()
    budget = BudgetTracker(getattr(settings, "max_cost_usd", 5.0))
    client = make_llm_client(settings)
    print(f"  provider: {client.provider_name}  model: {client.model}")

    svc = _svc()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A1:AB"
    ).execute()
    rows = resp.get("values", [])
    header = rows[0] if rows else []
    rows = rows[1:]
    # Find rows that need a retry: classify error in note OR Точно новость
    # with empty section.
    targets: list[tuple[int, list[str]]] = []
    for i, r in enumerate(rows, start=2):
        verdict = _get(r, COL_VERDICT)
        note = _get(r, COL_NOTE)
        section = _get(r, COL_SECTION)
        title = _get(r, COL_TITLE)
        body = _get(r, COL_LEDE)
        if verdict != "Точно новость":
            continue
        # Two failure modes: explicit "classify error" in note, OR no LLM
        # translation (no "EN:" prefix in title) which implies translate
        # didn't run either.
        looks_failed = (
            "classify error" in note
            or "translate error" in note
            or (not section)
            or ("EN:" not in title)
        )
        if looks_failed and title and body:
            targets.append((i, r))
    print(f"Found {len(targets)} rows needing retry.")
    if not targets:
        return 0

    updates: list[dict] = []
    portal_country = "Russia"  # RU portal hard-coded for now
    for idx, (sheet_row, r) in enumerate(targets, start=1):
        url = _get(r, COL_URL)
        title = _get(r, COL_TITLE)
        # If the title was the original scraped string (no EN: prefix),
        # use it as the source headline. Otherwise drop the prefix
        # because LLM should translate from the original.
        clean_title = title
        if "EN:" in title:
            # Keep the EN line as the source title
            for line in title.splitlines():
                if line.strip().startswith("EN:"):
                    clean_title = line.split(":", 1)[1].strip()
                    break
        body = _get(r, COL_LEDE)
        raw = RawArticle(
            url=url, title=clean_title, body=body, html="",
            source_name="", source_url="",
        )
        try:
            cls, u1 = client.classify_section(
                title=clean_title, body=body,
                sections=sections, few_shots=[], portal_country=portal_country,
            )
            budget.record(u1)
        except Exception as e:  # noqa: BLE001
            print(f"  [{idx}/{len(targets)}] CLASSIFY FAILED row {sheet_row}: {e!s:80}")
            continue
        try:
            tp, u2 = client.translate_title(title=clean_title, source_language_hint=None)
            budget.record(u2)
        except Exception as e:  # noqa: BLE001
            print(f"  [{idx}/{len(targets)}] TRANSLATE FAILED row {sheet_row}: {e!s:80}")
            continue
        # Build new title cell with EN/RU + lang tags
        lang = (tp.source_language or "").lower()[:2]
        lang_map = {
            "en": ("EN", "АНГЛ"), "ru": ("RU", "РУС"), "de": ("DE", "НЕМ"),
            "fr": ("FR", "ФР"),  "it": ("IT", "ИТАЛ"), "es": ("ES", "ИСП"),
            "zh": ("ZH", "КИТ"), "ja": ("JA", "ЯП"),
        }
        en_tag, ru_tag = lang_map.get(lang, (lang.upper(), lang.upper()))
        en_suffix = f" ({en_tag})" if en_tag else ""
        ru_suffix = f" ({ru_tag})" if ru_tag else ""
        new_title = (
            f"EN: {tp.english[:220]}{en_suffix}\n"
            f"RU: {tp.russian[:220]}{ru_suffix}"
        )
        cost = round(u1.cost_usd + u2.cost_usd, 5)
        spent = budget.spent_usd
        print(
            f"  [{idx}/{len(targets)}] OK row {sheet_row}: "
            f"section={cls.section} region={cls.region} cost=${cost} (run total ${spent:.4f})"
        )
        # Schedule cell updates: B (title), E (section), F (region),
        # M (note — clear), Y (relevance — keep "Да" since classify ran)
        updates.append({"range": f"'{tab}'!B{sheet_row}", "values": [[new_title]]})
        updates.append({"range": f"'{tab}'!E{sheet_row}", "values": [[cls.section]]})
        updates.append({"range": f"'{tab}'!F{sheet_row}", "values": [[cls.region]]})
        updates.append({"range": f"'{tab}'!M{sheet_row}", "values": [[""]]})  # clear error
        updates.append({"range": f"'{tab}'!Z{sheet_row}", "values": [[cost]]})

    if updates:
        # Apply in chunks
        CHUNK = 200
        for i in range(0, len(updates), CHUNK):
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"valueInputOption": "USER_ENTERED", "data": updates[i:i + CHUNK]},
            ).execute()
        print(f"\nApplied {len(updates) // 5} row updates.  Total run cost: ${budget.spent_usd:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
