"""PoC step 1 — extract two datasets for the embedding-dedup experiment.

  • v41_rows.jsonl     — every v41 push row that has an editor verdict
                         (col P), the ground-truth signal: is it a dup?
  • history_30d.jsonl  — every article our DedupStore saw in the prior
                         30 days BEFORE the v41 push (the haystack the
                         embedding-matcher will search against).

Editor-verdict classification (from the comment text):
  • dup_yes  : explicit «дубль», «постили», «уже было», «опять дубль»,
               «дубль NNN», «постили пресс»
  • dup_no   : explicit «ок», «постим», «да», positive section assign,
               or no comment but row was pushed (assume non-dup)
  • dup_amb  : everything else (hedged / off-topic) — excluded from
               precision/recall

This gives us a labeled set to score the embedding matcher against.
"""

from __future__ import annotations

import io
import json
import os
import re
import sqlite3
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs"
)
SA = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
creds = service_account.Credentials.from_service_account_file(
    str(SA), scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
svc = build("sheets", "v4", credentials=creds)
TAB = "Новости (новые)"

DUP_POS = (
    "дубль", "дубли", "опять дубль", "уже было", "уже постил",
    "повтор", "постили", "постили пресс", "постили глобал",
    "выше новость о том же", "та же новость", "была новость",
    "несколько раз было",
)
DUP_NEG = (  # explicit non-dup approval
    " ок", "ок,", "ок.", "ок ", "норм", "согласен", "годится",
    " да", "да,", "да.",  "постим", "это факт", "это в факт",
    "это слух", "это рум", "это в мест", "это лсв", "это эконом",
    "это в друг",
)


def label_from_comment(c: str) -> str:
    """Return 'dup_yes' / 'dup_no' / 'dup_amb'."""
    cl = (c or "").strip().lower()
    if not cl:
        return "dup_no"  # accepted-without-comment = not a dup
    # any positive dup phrase wins (editors are explicit)
    if any(p in cl for p in DUP_POS):
        return "dup_yes"
    # plain «ok» style → not dup
    if any(p in cl for p in DUP_NEG):
        return "dup_no"
    return "dup_amb"


def main() -> int:
    # ------------------------------------------------------ v41 rows
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{TAB}'!A1:P300",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])

    # v41 push: rows 3..54 (separator at row 2 = "20.05.2026 21:23 UTC")
    v41 = []
    for i, r in enumerate(rows, 1):
        if i < 3 or i > 54:
            continue
        if not r or not isinstance(r[0], str):
            continue
        if "━━" in r[0]:
            continue
        title = str(r[1] if len(r) > 1 else "")
        lede = str(r[2] if len(r) > 2 else "")
        section = str(r[3] if len(r) > 3 else "")
        url = str(r[9] if len(r) > 9 else "")
        comment = str(r[15] if len(r) > 15 else "").strip()
        label = label_from_comment(comment)
        v41.append({
            "row": i, "title": title, "lede": lede,
            "section": section, "url": url,
            "editor_comment": comment, "editor_label": label,
        })

    out1 = DATA / "embed_poc_v41.jsonl"
    with out1.open("w", encoding="utf-8") as f:
        for r in v41:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    cnt = {"dup_yes": 0, "dup_no": 0, "dup_amb": 0}
    for r in v41:
        cnt[r["editor_label"]] += 1
    print(f"v41 push rows: {len(v41)}")
    print(f"  editor said DUP        : {cnt['dup_yes']}")
    print(f"  editor said NOT dup    : {cnt['dup_no']}")
    print(f"  ambiguous (excluded)   : {cnt['dup_amb']}")

    # --------------------------------------------- history 30 days
    db = DATA / "news_agent.sqlite"
    con = sqlite3.connect(db)
    cur = con.cursor()
    hist = []
    # Take everything from last_seen_at BEFORE 2026-05-20 19:00 UTC
    # (v41 ran at 21:23 UTC; use a few hours before to be safe)
    rows_db = cur.execute(
        "SELECT title, canonical_url, source_domain, last_seen_at, "
        "       portal, cached_row_json "
        "FROM seen_articles "
        "WHERE last_seen_at < ? AND last_seen_at >= ? "
        "ORDER BY last_seen_at DESC",
        ("2026-05-20T19:00:00+00:00", "2026-04-20T00:00:00+00:00"),
    ).fetchall()
    for title, url, dom, ts, portal, blob in rows_db:
        try:
            j = json.loads(blob) if blob else {}
        except Exception:
            j = {}
        # We mostly need the title (lede isn't stored verbatim in
        # cached_row_json — but bullets/canonical_lede can be there)
        lede = (j.get("canonical_lede") or j.get("bullets")
                or j.get("lede") or "")[:400]
        hist.append({
            "title": title or "", "lede": lede,
            "url": url or "", "domain": dom or "",
            "ts": ts or "", "portal": portal or "",
            "event_brand": j.get("event_brand", ""),
            "event_model": j.get("event_model", ""),
            "event_type": j.get("event_type", ""),
        })

    out2 = DATA / "embed_poc_history.jsonl"
    with out2.open("w", encoding="utf-8") as f:
        for r in hist:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"\nhistory (prior 30d): {len(hist)} articles")
    print(f"  with cached lede     : {sum(1 for r in hist if r['lede'])}")
    print(f"  with event_brand     : {sum(1 for r in hist if r['event_brand'])}")
    print(f"\nwrote {out1.name}, {out2.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
