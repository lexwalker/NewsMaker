"""Rejection-labelling ritual, step 1 — sample what the BOT REJECTED and
put it in front of the editor for a one-column "нужно / не нужно" verdict
(peer-review §2).

Why: the retrieval A/B proved negatives are a precision lever but their
effect is small at the current 186; we need a STREAM of fresh editor
decisions. Editor-labelled rejects feed three things — confirmed negatives
(grow the base), false rejects (recall holes + which heuristic wrongly
killed live content), and an honest recall number.

What it does:
  * pulls bot-rejected rows from SQLite within a window, kept ONLY if the
    title is AUTO-related (brand / auto-marker / transport-civic) AND the
    reject cause carries editorial signal (llm / blacklist / off_topic).
    This is the fix for the random-sample problem: it was full of 100%-junk
    (sport, finance, politics, parts-ads) the editor would never want —
    labelling those teaches nothing. We want auto-BORDERLINE cases, where
    false-rejects and harmful blacklist phrases actually hide.
  * stratified sample across the kept causes, capped per cause;
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

from news_agent.core.brand_canonical import canonicalize_brand  # noqa: E402
from news_agent.core.heuristic_relevance import (  # noqa: E402
    _AUTO_STRONG_MARKERS,
    is_ru_transport_civic,
)
import review_tab  # noqa: E402  (shared review-tab helper, scripts/ on path)
from news_agent.core.labeling import stratified_sample  # noqa: E402
from news_agent.core.reject_stage import classify_outcome  # noqa: E402

# Only these reject CAUSES carry editorial signal worth the editor's time.
# We DROP not_article (parts ads / navigation: "Купить Колесо литое"),
# dzen_listicle / multi_news / supplier (format junk) — labelling those
# teaches nothing.
_BORDERLINE_CAUSES = {"llm", "blacklist", "off_topic"}


# Hard NEG gate: even if a brand/marker spuriously matched (e.g. "Волга"
# the river, "модельное предприятие"), these categories are never auto
# news — drop them. Covers exactly what the editor flagged: military,
# sport, obvious off-topic.
_NON_AUTO_NEG = (
    "военн", "мобилиз", "призывник", "дрон", "бпла", " всу", " пво",
    "abrams", "танк ", "ракет", "снаряд",          # military
    "футбол", "сборн", "хоккеист", "чемпионат мира по",  # sport (not motorsport)
    "косметик", "диетолог", "сериал",              # lifestyle junk
)


def _auto_signal(title: str) -> bool:
    """True if the title is AUTO-related — a brand, an auto-type marker, or
    transport-civic (ПДД/ОСАГО/каршеринг) — AND not in an obvious non-auto
    category. The point of the sample is auto-BORDERLINE cases, not the
    100%-junk (sport/finance/politics/diet/military) the editor flagged."""
    t = (title or "").lower()
    if any(neg in t for neg in _NON_AUTO_NEG):
        return False
    if canonicalize_brand(title):
        return True
    if any(m in t for m in _AUTO_STRONG_MARKERS):
        return True
    if is_ru_transport_civic(title):
        return True
    return False

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
SQLITE_PATH = DATA / "news_agent.sqlite"
SENT_LOG = DATA / "labeling_sent.jsonl"


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


def load_rejects(days: int, *, borderline_only: bool = True) -> list[dict]:
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
        # Borderline AUTO rejects only: a cause that carries editorial
        # signal AND an auto-related title. This is the whole fix — the
        # random sample was dominated by 100%-junk (sport/finance/politics/
        # parts-ads) where the editor's label teaches nothing.
        if outcome.cause not in _BORDERLINE_CAUSES:
            continue
        # The auto-signal filter is what makes this sample a MAGNIFYING GLASS
        # for false rejects — deliberately, see above. It also makes it useless
        # for estimating a RATE, and that distinction was lost: the 43% error
        # it showed for off_topic got read as the gate's error rate and drove a
        # plan to rewrite the gate. A random read of 40 rejected articles
        # (aug-04, body re-fetched, judged by the constitution) put the real
        # figure at 0. Callers that want a rate pass borderline_only=False.
        if borderline_only and not _auto_signal(title or ""):
            continue
        reason = d.get("llm_reason") or d.get("article_reasons") or ""
        out.append({
            "url_hash": uh, "url": url or "", "title": title or "",
            "cause": outcome.cause, "stage": outcome.stage,
            "reason": reason[:140], "verdict": d.get("verdict", ""),
        })
    return out




def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--total", type=int, default=50,
                    help="target sample size (round-robin across causes)")
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--seed", type=int, default=20260614)
    ap.add_argument(
        "--random", type=int, default=4,
        help="extra rows drawn from ALL rejects, not just auto-borderline "
             "ones. The borderline sample finds bad rules but cannot measure "
             "how often a gate misfires; these can. Marked in the context "
             "column so analysis can separate the two populations.")
    args = ap.parse_args()

    rejects = load_rejects(args.days)
    if not rejects:
        print("No content-rejects in window — widen --days or check the cache.")
        return 0
    sent = _load_sent()
    rng = random.Random(args.seed)
    sample = stratified_sample(
        rejects, key_fn=lambda r: r["cause"], per_bucket=args.total,
        shuffle=rng.shuffle, exclude=lambda r: r["url_hash"] in sent,
        total=args.total)
    if not sample:
        print(f"All {len(rejects)} window-rejects already labelled — nothing new.")
        return 0

    # Unbiased stratum: same causes, no auto-title requirement. Small on
    # purpose — most of it is obvious junk the editor answers in a glance, and
    # its only job is to give the denominator the borderline sample cannot.
    RND_MARK = "[случайная выборка] "
    if args.random > 0:
        picked = {r["url_hash"] for r in sample}
        pool = [r for r in load_rejects(args.days, borderline_only=False)
                if r["url_hash"] not in sent and r["url_hash"] not in picked]
        rng.shuffle(pool)
        for r in pool[:args.random]:
            r["reason"] = (RND_MARK + (r["reason"] or ""))[:140]
            sample.append(r)

    from collections import Counter
    by = Counter(r["cause"] for r in sample)
    print(f"Sampled {len(sample)} bot-rejects for review "
          f"(of {len(rejects)} in {args.days}d, {len(sent)} already sent):")
    for c, n in by.most_common():
        print(f"  {n:3}  {c}")

    svc = _svc()
    rows = [{
        "title": r["title"], "context": r["reason"],
        "type": f"{review_tab.TYPE_REJECTED}:{r['cause']}",
        "url_hash": r["url_hash"], "url": r["url"],
    } for r in sample]
    run_label = ("Отклонённое ИИ на разметку, "
                 + datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC"))
    added = review_tab.append_batch(svc, EDITOR, rows, run_label)

    with SENT_LOG.open("a", encoding="utf-8") as f:
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        for r in sample:
            f.write(json.dumps({"url_hash": r["url_hash"],
                                "title": r["title"][:120],
                                "cause": r["cause"], "sent_at": ts},
                               ensure_ascii=False) + "\n")

    print(f"\nAppended {added} rows → tab {review_tab.REVIEW_TAB!r} "
          f"(deduped; {len(sample)-added} already present).")
    print(f"Logged sent hashes → {SENT_LOG.name} (won't be re-sampled).")
    print("Editor fills column D (да/нет) + E (раздел if да); then run "
          "ingest_rejected_labels.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
