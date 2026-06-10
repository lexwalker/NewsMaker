"""Advisory editorial judge — runs the precedent-based judge over a
prog's pushed rows in ADVISORY mode. It NEVER changes the bot's
decision; it writes a second opinion into a dedicated column and logs
advisory-vs-(future)-editor for honest measurement.

Usage:
  python scripts/run_advisory_judge.py            # judge latest push
  python scripts/run_advisory_judge.py --rows 3-57

What it does:
  1. Reads the editor sheet rows of the latest push (title + section).
  2. For each, asks EditorialJudge for an advisory verdict by analogy
     to the editor's real decisions.
  3. Writes "Совет ИИ-судьи" into a spare column (R) so the editor
     sees it WHILE reviewing — but the bot's section/verdict stay
     untouched.
  4. Appends each (row, title, bot_section, advisory) to
     data/advisory_log.jsonl. Later, a compare step matches these
     against the editor's actual col-P verdict to measure agreement.

This is the honest "advisory phase": we measure the judge against
reality for weeks before letting it decide anything.
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

from news_agent.core.editorial_judge import EditorialJudge  # noqa: E402

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs"
)
TAB = "Новости (новые)"
ADVISORY_LOG = DATA / "advisory_log.jsonl"
DECISIONS = DATA / "editor_decisions.json"
VECTORS = DATA / "editor_decisions_vectors.npz"
# Spare column for the advisory note (R = index 17, well clear of A-P).
ADVISORY_COL = "R"


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def _find_latest_push(rows: list[list]) -> tuple[int, int]:
    """Return (start_row, end_row) of the most-recent push block."""
    start = end = None
    for i, r in enumerate(rows, 1):
        if r and isinstance(r[0], str) and "━━" in r[0]:
            if start is None:
                start = i + 1
            elif end is None:
                end = i
                break
    if start and not end:
        end = min(start + 60, len(rows) + 1)
    return start or 2, end or 60


def main() -> int:
    args = sys.argv[1:]
    limit = None
    rows_arg = None
    for a in args:
        if a.startswith("--rows="):
            rows_arg = a.split("=", 1)[1]
        if a.startswith("--limit="):
            limit = int(a.split("=", 1)[1])

    if not DECISIONS.exists():
        print("data/editor_decisions.json missing — build it first")
        return 2

    svc = _svc()
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{TAB}'!A1:R200",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])

    if rows_arg:
        a, b = rows_arg.split("-")
        start, end = int(a), int(b) + 1
    else:
        start, end = _find_latest_push(rows)
    print(f"Advisory judge over rows {start}-{end-1}")

    judge = EditorialJudge(DECISIONS, vectors_cache=VECTORS)
    print(f"Decision base: {len(judge.pos)} published, "
          f"{len(judge.neg)} rejected\n")

    updates = []
    log_lines = []
    total_cost = 0.0
    agree_with_bot = disagree = errored = 0
    n = 0
    t0 = time.time()
    for i in range(start, end):
        if i - 1 >= len(rows):
            break
        r = rows[i - 1]
        if not r or (isinstance(r[0], str) and "━━" in r[0]):
            continue
        title = str(r[1]) if len(r) > 1 else ""
        body = str(r[2]) if len(r) > 2 else ""
        bot_section = str(r[3]) if len(r) > 3 else ""
        if not title:
            continue
        if limit and n >= limit:
            break
        n += 1

        v = judge.judge(title, body)
        total_cost += v.cost_usd
        if v.error:
            errored += 1
            note = f"⚖ судья: ошибка ({v.error})"
        else:
            # bot published this row (it's on the sheet) → bot_publish=True
            verdict = ("ОСТАВИТЬ" if v.advisory_publish
                       else "УБРАТЬ" if v.advisory_publish is False
                       else "?")
            sec_note = (f" / раздел: {v.advisory_section}"
                        if v.advisory_publish and v.advisory_section
                        and v.advisory_section != bot_section else "")
            note = (f"⚖ судья: {verdict}{sec_note} "
                    f"(увер. {v.confidence:.0%}) — {v.reason[:90]}")
            # bot decision = published (on sheet). agreement:
            if v.advisory_publish is True:
                agree_with_bot += 1
            elif v.advisory_publish is False:
                disagree += 1

        updates.append({"range": f"'{TAB}'!{ADVISORY_COL}{i}",
                        "values": [[note]]})
        log_lines.append({
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "row": i, "title": title[:120],
            "bot_section": bot_section,
            "bot_publish": True,  # it's on the sheet
            "advisory_publish": v.advisory_publish,
            "advisory_section": v.advisory_section,
            "confidence": v.confidence,
            "reason": v.reason[:200],
            "error": v.error,
        })
        mark = ("=" if v.advisory_publish else
                "≠УБРАТЬ" if v.advisory_publish is False else "err")
        print(f"  r{i:>3} [{mark:7}] {title[:52]}")
        if v.advisory_publish is False:
            print(f"          совет убрать: {v.reason[:75]}")

    # Write advisory notes (does NOT touch the bot's section column E)
    if updates:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=EDITOR,
            body={"valueInputOption": "USER_ENTERED", "data": updates},
        ).execute()
        # header for the advisory column
        svc.spreadsheets().values().update(
            spreadsheetId=EDITOR,
            range=f"'{TAB}'!{ADVISORY_COL}{start-1}",
            valueInputOption="USER_ENTERED",
            body={"values": [["Совет ИИ-судьи (тест, не решение)"]]},
        ).execute()

    with ADVISORY_LOG.open("a", encoding="utf-8") as f:
        for ln in log_lines:
            f.write(json.dumps(ln, ensure_ascii=False) + "\n")

    print(f"\n=== Advisory готов ({time.time()-t0:.0f}s, "
          f"${total_cost:.3f}) ===")
    print(f"  Судил строк: {n}")
    print(f"  Согласен с ботом (оставить): {agree_with_bot}")
    print(f"  Советует УБРАТЬ (расходится): {disagree}")
    print(f"  Ошибок: {errored}")
    print(f"\n  Совет записан в колонку {ADVISORY_COL} — НЕ меняет "
          f"решение бота.")
    print(f"  Лог → {ADVISORY_LOG.name} (сравним с вердиктом редактора).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
