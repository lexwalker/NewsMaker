"""Compare the bot's verdict across two versions of the article tab.

Matches rows by 'URL статьи' and computes:
  • how v2 differs from v1 per article
  • if row in v1 has a human label, how each version scored against it

Run:  python scripts/compare_versions.py
"""

from __future__ import annotations

import io
import os
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

V1_TAB = "ТЕСТ статьи"
V2_TAB = "ТЕСТ статьи v2"


def read_tab(svc, tab: str) -> list[dict[str, str]]:  # type: ignore[no-untyped-def]
    vals = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"'{tab}'")
        .execute()
        .get("values", [])
    )
    if not vals or len(vals) < 2:
        return []
    header = [h.strip() for h in vals[0]]
    out = []
    for row in vals[1:]:
        d = {}
        for i, h in enumerate(header):
            d[h] = row[i].strip() if i < len(row) else ""
        out.append(d)
    return out


def norm_human(raw: str) -> str:
    t = raw.strip().lower().replace("ё", "е")
    if not t:
        return ""
    if "не новост" in t:
        return "not_news"
    if "но не по теме" in t or "не по теме" in t:
        return "news_off_topic"
    if "новост" in t:
        return "news_in_topic"
    return ""


def main() -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    v1 = read_tab(svc, V1_TAB)
    v2 = read_tab(svc, V2_TAB)
    print(f"v1 rows: {len(v1)}  |  v2 rows: {len(v2)}")

    # URL-based join (v1 has URL in column E — "URL статьи")
    v1_by_url = {r.get("URL статьи", ""): r for r in v1 if r.get("URL статьи")}
    v2_by_url = {r.get("URL статьи", ""): r for r in v2 if r.get("URL статьи")}
    shared_urls = set(v1_by_url) & set(v2_by_url)
    print(f"URLs common to both versions: {len(shared_urls)}")

    changed = 0
    new_rejected_was_fp = 0
    new_rejected_was_tp = 0
    new_rejected_unknown = 0
    still_passing_fp = 0
    still_passing_tp = 0
    for url in shared_urls:
        r1, r2 = v1_by_url[url], v2_by_url[url]
        v1_verdict = r1.get("Итог бота", "")
        v2_verdict = r2.get("Итог бота", "")
        human = norm_human(r1.get("Ручная проверка (впишите: Новость / Не новость)", ""))
        if v1_verdict != v2_verdict:
            changed += 1
        if v1_verdict == "Отправить в LLM" and v2_verdict != "Отправить в LLM":
            if human == "not_news":
                new_rejected_was_fp += 1
            elif human in {"news_in_topic", "news_off_topic"}:
                new_rejected_was_tp += 1
            else:
                new_rejected_unknown += 1
        if v1_verdict == "Отправить в LLM" and v2_verdict == "Отправить в LLM":
            if human == "not_news":
                still_passing_fp += 1
            elif human in {"news_in_topic", "news_off_topic"}:
                still_passing_tp += 1

    print(f"\nVerdict changed in {changed} rows")
    print()
    print("Бот теперь отклоняет то, что в v1 пропускал в LLM:")
    print(f"  {new_rejected_was_fp:3}  настоящие FP → исправлено  (человек: Не новость)")
    print(f"  {new_rejected_was_tp:3}  настоящие TP → ухудшение! (человек: Новость)")
    print(f"  {new_rejected_unknown:3}  без разметки человека")

    print("\nВ v2 продолжают проходить в LLM:")
    print(f"  {still_passing_fp:3}  человек: Не новость  (остающиеся FP)")
    print(f"  {still_passing_tp:3}  человек: Новость      (остающиеся TP)")

    v1_verdicts = Counter(r.get("Итог бота", "") for r in v1_by_url.values() if r.get("URL статьи", "") in shared_urls)
    v2_verdicts = Counter(r.get("Итог бота", "") for r in v2_by_url.values() if r.get("URL статьи", "") in shared_urls)
    print(f"\n-- Итоги по общим URL --")
    print(f"{'verdict':40} v1  →  v2")
    for k in sorted(set(v1_verdicts) | set(v2_verdicts)):
        print(f"  {k:38}  {v1_verdicts.get(k, 0):3}  →  {v2_verdicts.get(k, 0):3}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
