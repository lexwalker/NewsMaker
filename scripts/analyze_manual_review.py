"""Compare bot verdicts in the ТЕСТ статьи tab against the manual review.

Reads columns:
  O = 'Итог бота'          (bot's decision)
  P = 'Ручная проверка'    (human label)
  M = 'Авто/эконом'        (topic filter)
  J = 'is_article'         (article filter)
  E, F = article URL + title (for sample output)

Human labels we understand (normalised, case-insensitive):
  news_in_topic:   "новость", "да", "+", "yes"
  news_off_topic:  "новость, но не по теме", "новость не по теме", "по теме нет"
  not_news:        "не новость", "нет", "-", "no"

Computes:
  • confusion matrix for the article-level filter (is_article)
  • confusion matrix for the topic-level filter (auto/economy)
  • combined funnel: human label vs final bot verdict
  • sample of mismatches (false positives / false negatives)
"""

from __future__ import annotations

import io
import os
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
# Always compare against the original tab — that's the one with the human labels.
# (Later iterations v2, v3 are the bot's re-runs and don't carry labels.)
TAB = os.environ.get("ANALYZE_TAB", "ТЕСТ статьи")

NEWS_IN_TOPIC = {"новость", "да", "+", "yes", "y", "новость по теме"}
NEWS_OFF_TOPIC = {
    "новость, но не по теме",
    "новость не по теме",
    "не по теме",
    "не по теме, новость",
    "не авто",
    "не про авто",
    "новость но не по теме",
}
NOT_NEWS = {"не новость", "нет", "-", "no", "n", "не статья"}


@dataclass
class Row:
    idx: int
    url: str
    title: str
    is_article: bool
    auto_topic: bool
    verdict: str
    human_raw: str
    human_norm: str  # "news_in_topic" | "news_off_topic" | "not_news" | ""


def norm_human(raw: str) -> str:
    t = raw.strip().lower().replace("ё", "е")
    if not t:
        return ""
    if t in NEWS_IN_TOPIC:
        return "news_in_topic"
    if t in NEWS_OFF_TOPIC:
        return "news_off_topic"
    if t in NOT_NEWS:
        return "not_news"
    # heuristics on the free text
    if "не новост" in t:
        return "not_news"
    if "но не по теме" in t or "не по теме" in t:
        return "news_off_topic"
    if "новост" in t:
        return "news_in_topic"
    return ""


def read_rows(svc) -> list[Row]:  # type: ignore[no-untyped-def]
    vals = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"'{TAB}'")
        .execute()
        .get("values", [])
    )
    if not vals or len(vals) < 2:
        return []
    # column indices: A=0, B=1, … P=15
    out: list[Row] = []
    for i, row in enumerate(vals[1:], start=2):
        def c(idx: int) -> str:
            return row[idx].strip() if idx < len(row) else ""

        out.append(
            Row(
                idx=i,
                url=c(4),
                title=c(5),
                is_article=c(9) == "Новость",
                auto_topic=c(12) == "Авто/эконом",
                verdict=c(14),
                human_raw=c(15),
                human_norm=norm_human(c(15)),
            )
        )
    return out


def fmt_matrix(tp: int, fp: int, fn: int, tn: int) -> str:
    total = tp + fp + fn + tn
    if total == 0:
        return "  (no labelled rows)"
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    accuracy = (tp + tn) / total
    return (
        f"                человек: ДА    человек: НЕТ\n"
        f"  бот: ДА         TP={tp:3}       FP={fp:3}\n"
        f"  бот: НЕТ        FN={fn:3}       TN={tn:3}\n"
        f"  → precision={precision:.3f}  recall={recall:.3f}  F1={f1:.3f}  acc={accuracy:.3f}"
    )


def main() -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    rows = read_rows(svc)
    if not rows:
        print(f"Tab {TAB!r} is empty.", file=sys.stderr)
        return 2

    labelled = [r for r in rows if r.human_norm]
    unlabelled = len(rows) - len(labelled)
    print(f"Total rows: {len(rows)}")
    print(f"  labelled by human: {len(labelled)}")
    print(f"  unlabelled:        {unlabelled}")
    if unlabelled:
        print("  (unlabelled rows are ignored for the stats below)")

    if not labelled:
        return 0

    # -------------------------------- label distribution
    dist = Counter(r.human_norm for r in labelled)
    print("\nHuman label distribution:")
    for k, v in dist.most_common():
        print(f"  {k:16} {v:3}")

    # -------------------------------- is_article filter
    print("\n=== Фильтр 1: это ли вообще статья? (is_article) ===")
    print("Положительный класс: человек сказал 'новость' (любая — по теме или не по теме)")
    tp = sum(1 for r in labelled if r.is_article and r.human_norm in {"news_in_topic", "news_off_topic"})
    fp = sum(1 for r in labelled if r.is_article and r.human_norm == "not_news")
    fn = sum(1 for r in labelled if not r.is_article and r.human_norm in {"news_in_topic", "news_off_topic"})
    tn = sum(1 for r in labelled if not r.is_article and r.human_norm == "not_news")
    print(fmt_matrix(tp, fp, fn, tn))

    # -------------------------------- auto/economy filter, on rows that are news
    news_rows = [r for r in labelled if r.human_norm in {"news_in_topic", "news_off_topic"}]
    print("\n=== Фильтр 2: про авто/эконом? (только среди настоящих новостей) ===")
    print(f"Рассматриваем {len(news_rows)} реальных новостей (по разметке).")
    if news_rows:
        t_tp = sum(1 for r in news_rows if r.auto_topic and r.human_norm == "news_in_topic")
        t_fp = sum(1 for r in news_rows if r.auto_topic and r.human_norm == "news_off_topic")
        t_fn = sum(1 for r in news_rows if not r.auto_topic and r.human_norm == "news_in_topic")
        t_tn = sum(1 for r in news_rows if not r.auto_topic and r.human_norm == "news_off_topic")
        print(fmt_matrix(t_tp, t_fp, t_fn, t_tn))

    # -------------------------------- combined verdict → human
    print("\n=== Итоговый вердикт бота vs ручная разметка ===")
    send_to_llm = [r for r in labelled if r.verdict == "Отправить в LLM"]
    rejected = [r for r in labelled if r.verdict != "Отправить в LLM"]
    print(f"Бот хочет отправить в LLM:  {len(send_to_llm)}")
    print(f"Бот отклонил:               {len(rejected)}")
    for bucket, name in ((send_to_llm, "отправленных в LLM"), (rejected, "отклонённых")):
        if not bucket:
            continue
        c = Counter(r.human_norm for r in bucket)
        print(f"  среди {name}:")
        for label, n in c.most_common():
            print(f"    {label:16} {n:3}")

    # -------------------------------- mismatches
    def show(bucket: list[Row], title: str) -> None:
        if not bucket:
            print(f"\n{title}: нет")
            return
        print(f"\n{title} ({len(bucket)}):")
        for r in bucket[:20]:
            print(f"  [row {r.idx:>3}] verdict={r.verdict!r:30}  human={r.human_raw!r}")
            print(f"      {r.title[:100]}")
            print(f"      {r.url}")
        if len(bucket) > 20:
            print(f"  … и ещё {len(bucket) - 20} строк")

    # Bot = yes (will send to LLM), Human = not news → strict false positive
    fp_strict = [r for r in labelled if r.verdict == "Отправить в LLM" and r.human_norm == "not_news"]
    # Bot = yes, Human = news off-topic → topic-filter miss
    fp_offtopic = [
        r for r in labelled if r.verdict == "Отправить в LLM" and r.human_norm == "news_off_topic"
    ]
    # Bot = rejected, Human = news in topic → false negative (worst case)
    fn_strict = [r for r in labelled if r.verdict != "Отправить в LLM" and r.human_norm == "news_in_topic"]

    show(fp_strict, "FALSE POSITIVE (бот: в LLM, человек: не новость)")
    show(fp_offtopic, "FALSE POSITIVE-off-topic (бот: в LLM, человек: новость не по теме)")
    show(fn_strict, "FALSE NEGATIVE (бот: отклонил, человек: новость по теме)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
