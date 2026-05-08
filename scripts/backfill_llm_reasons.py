"""Backfill 'Обоснование LLM' (col AE) for 'Точно новость' rows that
predate the llm_reason field. Calls editorial_review with the existing
title + lede and writes the reason directly to col AE.

Useful one-shot after rolling out the new LLM-first architecture: cached
rows from earlier runs lack a Russian reason; this script catches them
up at minimal cost (~$0.005-0.008 per row).

Usage:
    python scripts/backfill_llm_reasons.py "ТЕСТ статьи v31"
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
from news_agent.settings import get_settings  # noqa: E402

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

COL_TITLE = 1
COL_LEDE = 2
COL_VERDICT = 14
COL_LLM_REASON = 30  # AE


def _get(r: list[str], i: int) -> str:
    return r[i] if i < len(r) else ""


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: backfill_llm_reasons.py 'ТЕСТ статьи vN'")
        return 2
    tab = sys.argv[1]
    settings = get_settings()
    sections = load_sections()
    budget = BudgetTracker(getattr(settings, "max_cost_usd", 5.0))
    client = make_llm_client(settings)
    print(f"  provider: {client.provider_name}  model: {client.model}")

    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A1:AE",
    ).execute()
    rows = resp.get("values", []) or []
    rows = rows[1:]  # skip header

    targets: list[tuple[int, str, str]] = []
    for ri, r in enumerate(rows, start=2):
        verdict = _get(r, COL_VERDICT)
        if verdict != "Точно новость":
            continue
        if _get(r, COL_LLM_REASON).strip():
            continue  # already has reason
        title = _get(r, COL_TITLE).strip()
        body = _get(r, COL_LEDE).strip()
        if not title or not body:
            continue
        targets.append((ri, title, body))

    print(f"Found {len(targets)} 'Точно новость' rows missing reason.")
    if not targets:
        return 0

    updates: list[dict] = []
    for idx, (ri, title, body) in enumerate(targets, start=1):
        # Strip "EN: ... \n RU: ..." prefix to feed LLM the source title
        clean_title = title
        if "EN:" in title:
            for line in title.splitlines():
                ll = line.strip()
                if ll.lower().startswith("en:"):
                    clean_title = ll[3:].strip()
                    break
        try:
            review, u = client.editorial_review(
                title=clean_title,
                body=body,
                sections=sections,
                portal_country="Russia",
            )
            budget.record(u)
        except Exception as e:  # noqa: BLE001
            print(f"  [{idx}/{len(targets)}] FAILED row {ri}: {e!s:80}")
            continue
        reason = (review.reason or "").strip()[:300]
        if reason:
            updates.append({
                "range": f"'{tab}'!AE{ri}",
                "values": [[reason]],
            })
            print(f"  [{idx}/{len(targets)}] row {ri}: {reason[:80]}  (run total ${budget.spent_usd:.4f})")

    if not updates:
        print("Nothing to write.")
        return 0

    CHUNK = 200
    for i in range(0, len(updates), CHUNK):
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": updates[i:i + CHUNK]},
        ).execute()
    print(f"\nApplied {len(updates)} reason updates. Total cost: ${budget.spent_usd:.4f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
