"""Mini-eval for a SECTION-only constitution edit — the edit-protocol gate.

Replays recently judged rows (bodies from lede_text, aug-24+) through the
WORKING-TREE constitution via the production editorial_review_batch, and
compares against each row's recorded verdict/section. Run BEFORE committing
a constitution edit; the pendulum incident (broad rules, −7 net, reverted)
is why this gate exists.

Gate for a section-only batch:
  * publish/reject flips ≤ ~1.5% (a section edit must not touch accept);
  * movement is TOWARD the editor: Local→Confirmed on model-launch rows,
    Other→Confirmed on model-centric rows, a modest Confirmed→Rumors flow;
  * everything else stays put.

SPENDS MONEY (~$0.5-0.7 at the default 130 rows). Hard-capped at $1.

Usage: python scripts/eval_constitution_sections.py [--per-bucket 40 30 30 30]
"""

from __future__ import annotations

import argparse
import io
import json
import random
import sqlite3
import sys
from collections import Counter
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)
import os  # noqa: E402

from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.core.budget import BudgetExceeded, BudgetTracker  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

SQLITE = ROOT / "data" / "news_agent.sqlite"
BUCKETS = (("Local specifics", "acc"), ("Other news", "acc"),
           ("Confirmed", "acc"), ("", "rej"))


def load_rows(per_bucket: list[int], seed: int) -> list[dict]:
    con = sqlite3.connect(str(SQLITE))
    raw = con.execute(
        "SELECT title, lede_text, cached_row_json FROM seen_articles "
        "WHERE first_seen_at >= '2026-08-24' AND cached_row_json IS NOT NULL "
        "AND lede_text IS NOT NULL AND lede_text != ''").fetchall()
    pools: dict[tuple[str, str], list[dict]] = {b: [] for b in BUCKETS}
    for title, lede, blob in raw:
        b = json.loads(blob)
        if not (b.get("llm_relevance") or "").strip():
            continue
        v = b.get("verdict")
        rec = {"title": title or "", "body": lede,
               "old_publish": v == "Точно новость",
               "old_section": b.get("llm_section") or ""}
        if v == "Точно новость":
            key = (rec["old_section"], "acc")
            if key in pools:
                pools[key].append(rec)
        elif v == "Отклонено LLM":
            pools[("", "rej")].append(rec)
    rng = random.Random(seed)
    out: list[dict] = []
    for (sec, kind), n in zip(BUCKETS, per_bucket):
        pool = pools[(sec, kind)]
        rng.shuffle(pool)
        out.extend(pool[:n])
    # Interleave buckets across batches. The first run of this harness fed
    # HOMOGENEOUS batches (ten Local market-stat rows together) — a batch
    # composition prod never produces, and the model judged the series, not
    # the articles: 23% publish flips that said nothing about the edit
    # under test. Prod batches are fetch-order mixed; mimic that.
    rng.shuffle(out)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-bucket", type=int, nargs=4, default=[40, 30, 30, 30],
                    metavar=("LOCAL", "OTHER", "CONFIRMED", "REJECTED"))
    ap.add_argument("--seed", type=int, default=20260829)
    ap.add_argument("--batch", type=int, default=10)
    args = ap.parse_args()

    rows = load_rows(args.per_bucket, args.seed)
    print(f"replaying {len(rows)} rows through the WORKING-TREE constitution")
    settings = get_settings()
    client = make_llm_client(settings)
    _ed = os.environ.get("EDITORIAL_MODEL", "").strip()
    if _ed and _ed != getattr(client, "model", ""):
        client = make_llm_client(settings)
        client.model = _ed
    print(f"model: {client.model}")
    sections = load_sections()
    budget = BudgetTracker(cap_usd=1.0)

    flips: list[tuple[str, str, str]] = []
    moves: dict[str, Counter] = {}
    unanswered = 0
    for i in range(0, len(rows), args.batch):
        chunk = rows[i:i + args.batch]
        try:
            reviews, u = client.editorial_review_batch(
                items=[(r["title"], r["body"]) for r in chunk],
                sections=sections, portal_country="Russia")
            budget.record(u)
        except BudgetExceeded:
            print("!!! budget cap $1 hit — stopping, partial results below")
            break
        for r, rev in zip(chunk, reviews):
            if rev is None:
                unanswered += 1
                continue
            old_lbl = (r["old_section"] or "REJECTED") if r["old_publish"] \
                else "REJECTED"
            if rev.should_publish != r["old_publish"]:
                flips.append((old_lbl,
                              "ACCEPT" if rev.should_publish else "REJECT",
                              r["title"][:70]))
                continue
            if r["old_publish"]:
                moves.setdefault(r["old_section"], Counter())[
                    rev.section or "?"] += 1

    print(f"\nspent: ${budget.spent_usd:.3f}   unanswered (singly in prod): "
          f"{unanswered}")
    print(f"\nPUBLISH/REJECT FLIPS: {len(flips)}")
    for old, new, t in flips:
        print(f"  {old:16} -> {new:6}  {t}")
    print("\nSECTION MOVEMENT (old -> new):")
    for sec, c in moves.items():
        tot = sum(c.values())
        stay = c.get(sec, 0)
        print(f"  {sec:16} n={tot:3} осталось {stay:3} "
              f"({stay / tot:4.0%})  " +
              "  ".join(f"->{s}:{n}" for s, n in c.most_common()
                        if s != sec))
    return 0


if __name__ == "__main__":
    sys.exit(main())
