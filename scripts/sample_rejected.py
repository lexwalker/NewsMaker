"""Rejection-labelling ritual, step 1 — sample what the BOT REJECTED and
put it in front of the editor for a one-column "нужно / не нужно" verdict
(peer-review §2).

Why: the retrieval A/B proved negatives are a precision lever but their
effect is small at the current 186; we need a STREAM of fresh editor
decisions. Editor-labelled rejects feed three things — confirmed negatives
(grow the base), false rejects (recall holes + which heuristic wrongly
killed live content), and an honest recall number.

What it does:
  * pulls bot-rejected rows from SQLite within a window (CONTENT rejects
    only — S3 heuristic + S4 LLM; skips dups/stale/fetch-errors, which are
    recency/tech, not taste);
  * stratified sample across reject CAUSES (blacklist / off_topic /
    not_article / llm) so each is represented, capped per cause;
  * excludes rows already sent in a prior ritual (data/labeling_sent.jsonl);
  * writes them to the "Разметка отклонённого (ИИ)" tab with the bot's
    reason as context and an empty "Нужно?" column for the editor.

Editor then fills column E (да/нет) and, if да, F (раздел). Run
ingest_rejected_labels.py afterwards to route the verdicts.

Usage:
  python scripts/sample_rejected.py                 # ~32 rows, last 14 days
  python scripts/sample_rejected.py --per-cause 10 --days 21
"""

from __future__ import annotations

import argparse
import io
import json
import os
import random
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

from news_agent.core.labeling import stratified_sample  # noqa: E402
from news_agent.core.reject_stage import (  # noqa: E402
    S3_HEURISTIC,
    S4_LLM,
    classify_outcome,
)

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
TAB = "Разметка отклонённого (ИИ)"
SQLITE_PATH = DATA / "news_agent.sqlite"
SENT_LOG = DATA / "labeling_sent.jsonl"
HEADER = ["#", "Заголовок", "Причина бота", "Стадия",
          "Нужно? (да/нет)", "Раздел (если да)", "url_hash", "URL"]
INSTRUCTIONS = (
    "ИИ ОТКЛОНИЛ эти статьи. Отметьте в колонке E: «да» если новость "
    "нужна (ИИ ошибся), «нет» если отклонил верно. Если «да» — по "
    "возможности впишите раздел в F. Это учит ИИ на ваших решениях."
)


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def _load_sent() -> set[str]:
    if not SENT_LOG.exists():
        return set()
    out = set()
    for ln in SENT_LOG.read_text(encoding="utf-8").splitlines():
        if ln.strip():
            try:
                out.add(json.loads(ln)["url_hash"])
            except Exception:
                pass
    return out


def load_rejects(days: int) -> list[dict]:
    if not SQLITE_PATH.exists():
        return []
    con = sqlite3.connect(str(SQLITE_PATH))
    lo = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    rows = con.execute(
        "SELECT url_hash, canonical_url, title, first_seen_at, cached_row_json "
        "FROM seen_articles WHERE first_seen_at >= ? AND cached_row_json IS NOT NULL",
        (lo,)).fetchall()
    con.close()
    out = []
    for uh, url, title, seen, cj in rows:
        try:
            d = json.loads(cj)
        except Exception:
            continue
        outcome = classify_outcome(d.get("verdict", ""))
        # CONTENT rejects only — heuristic kills + LLM rejects
        if outcome.stage not in (S3_HEURISTIC, S4_LLM):
            continue
        reason = d.get("llm_reason") or d.get("article_reasons") or ""
        out.append({
            "url_hash": uh, "url": url or "", "title": title or "",
            "cause": outcome.cause, "stage": outcome.stage,
            "reason": reason[:140], "verdict": d.get("verdict", ""),
        })
    return out


def _ensure_tab(svc):
    meta = svc.spreadsheets().get(spreadsheetId=EDITOR).execute()
    if any(s["properties"]["title"] == TAB for s in meta["sheets"]):
        return
    svc.spreadsheets().batchUpdate(
        spreadsheetId=EDITOR,
        body={"requests": [{"addSheet": {"properties": {"title": TAB}}}]},
    ).execute()
    print(f"created tab {TAB!r}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-cause", type=int, default=8)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--seed", type=int, default=20260614)
    args = ap.parse_args()

    rejects = load_rejects(args.days)
    if not rejects:
        print("No content-rejects in window — widen --days or check the cache.")
        return 0
    sent = _load_sent()
    rng = random.Random(args.seed)
    sample = stratified_sample(
        rejects, key_fn=lambda r: r["cause"], per_bucket=args.per_cause,
        shuffle=rng.shuffle, exclude=lambda r: r["url_hash"] in sent)
    if not sample:
        print(f"All {len(rejects)} window-rejects already labelled — nothing new.")
        return 0

    from collections import Counter
    by = Counter(r["cause"] for r in sample)
    print(f"Sampled {len(sample)} bot-rejects for review "
          f"(of {len(rejects)} in {args.days}d, {len(sent)} already sent):")
    for c, n in by.most_common():
        print(f"  {n:3}  {c}")

    svc = _svc()
    _ensure_tab(svc)
    values = [[INSTRUCTIONS, "", "", "", "", "", "", ""], HEADER]
    for i, r in enumerate(sample, 1):
        values.append([i, r["title"][:300], r["reason"], r["cause"],
                       "", "", r["url_hash"], r["url"]])
    end = len(values)
    svc.spreadsheets().values().update(
        spreadsheetId=EDITOR, range=f"'{TAB}'!A1:H{end}",
        valueInputOption="USER_ENTERED", body={"values": values}).execute()

    with SENT_LOG.open("a", encoding="utf-8") as f:
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for r in sample:
            f.write(json.dumps({"url_hash": r["url_hash"],
                                "title": r["title"][:120],
                                "cause": r["cause"], "sent_at": ts},
                               ensure_ascii=False) + "\n")

    print(f"\nWrote {len(sample)} rows → tab {TAB!r} (A1:H{end}).")
    print(f"Logged sent hashes → {SENT_LOG.name} (won't be re-sampled).")
    print("Editor fills column E (да/нет) + F (раздел if да); then run "
          "ingest_rejected_labels.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
