"""Measure the archive-dedup JUDGE before wiring it anywhere.

Honest go/no-go: does "embedding-retrieve + LLM same-event judge" separate
real duplicates from genuine new stories better than the cosine-only
baseline (measured 60% dup-catch at 54% false-positive)?

Ground truth from the frozen eval set (editor comments):
  * DUPS    = label_publish False + comment says "постили / уже было"  (≈80)
  * GENUINE = label_publish True, comment NOT a dup-complaint            (≈169)

For each candidate we retrieve nearest published-archive stories and judge.
Self-exclusion (drop the single ≥0.95 archive match) simulates production
reality — a fresh candidate is not yet in the archive — so a GENUINE row's
own published copy doesn't count against it.

Outputs dup-catch-rate (recall) and false-positive-rate, with cost. This
script DECIDES whether the gate is worth wiring; it changes nothing.

Usage:
  python scripts/eval_archive_dedup.py            # full (~249 candidates)
  python scripts/eval_archive_dedup.py --limit 40 # cheap smoke
  python scripts/eval_archive_dedup.py --min-cos 0.6 --self-cos 0.95
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
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

from news_agent.core.archive_dedup import (  # noqa: E402
    ArchiveDedupJudge,
    ArchiveEntry,
    haiku_cost,
)

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
PUB_TAB = "Опубликованные (все)"
EVAL = DATA / "eval_set_v2.jsonl"
_DUP_RE = re.compile(r"постил|был[ао]|уже|дубл|повтор")


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def load_archive(svc) -> list[ArchiveEntry]:
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{PUB_TAB}'!A:R",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])
    out: list[ArchiveEntry] = []
    for r in rows[1:]:
        def c(i):
            return str(r[i]).strip() if len(r) > i and r[i] is not None else ""
        en, ru, date, url = c(3), c(4), c(5), c(11)
        title = en + ("\n" + ru if ru and ru != en else "")
        if title.strip():
            out.append(ArchiveEntry(title, date, url))
    return out


def load_truth() -> tuple[list[dict], list[dict]]:
    rows = [json.loads(ln) for ln in EVAL.read_text(encoding="utf-8").splitlines()
            if ln.strip()]
    dups, genuine = [], []
    for r in rows:
        c = (r.get("editor_comment") or "").lower()
        if r.get("label_publish") is False and _DUP_RE.search(c):
            dups.append(r)
        elif r.get("label_publish") is True and not _DUP_RE.search(c):
            genuine.append(r)
    return dups, genuine


def make_judge_fn(model="claude-haiku-4-5"):
    import anthropic
    # Explicit per-call timeout: one hung HTTPS connection froze a whole
    # measurement run for 5h (same failure class as the Playwright hang).
    # A timed-out call raises, is_duplicate catches it per-row (error
    # verdict), and the loop moves on instead of stalling.
    client = anthropic.Anthropic(timeout=45.0, max_retries=2)

    def fn(prompt: str) -> tuple[str, float]:
        resp = client.messages.create(
            model=model, max_tokens=200,
            messages=[{"role": "user", "content": prompt}])
        txt = "".join(getattr(b, "text", "") for b in resp.content)
        return txt, haiku_cost(resp.usage.input_tokens,
                               resp.usage.output_tokens)
    return fn


def load_date_lookup():
    """url → first_seen date ('YYYY-MM-DD') from the SQLite cache — when
    WE collected the candidate. Used as the archive-eligibility cutoff."""
    import sqlite3

    from news_agent.core.urls import canonicalise
    con = sqlite3.connect(str(DATA / "news_agent.sqlite"))

    def date_of(r: dict) -> str:
        u = canonicalise(r.get("url") or "")
        if not u:
            return ""
        row = con.execute(
            "SELECT first_seen_at FROM seen_articles WHERE canonical_url=?",
            (u,)).fetchone()
        return (row[0] or "")[:10] if row and row[0] else ""
    return date_of


def run_group(judge, rows, self_cos, limit, label, date_of=None):
    """date_of set → date-cutoff mode: judge sees only archive entries
    STRICTLY BEFORE the candidate's first_seen (production simulation);
    self_cos is not used. Candidates without a date are skipped."""
    flagged = judged = errors = n_run = skipped_nodate = 0
    cost = 0.0
    examples = []
    for i, r in enumerate(rows):
        if limit and n_run >= limit:
            break
        bd = None
        if date_of is not None:
            bd = date_of(r)
            if not bd:
                skipped_nodate += 1
                continue
        n_run += 1
        v = judge.is_duplicate(
            r.get("title", ""), r.get("body", "") or "",
            exclude_self_cos=(None if date_of is not None else self_cos),
            before_date=bd)
        cost += v.cost_usd
        if v.error:
            errors += 1
        if v.n_candidates > 0 and not v.error:
            judged += 1
        if v.is_duplicate:
            flagged += 1
            if len(examples) < 5:
                examples.append((r.get("title", "")[:55],
                                 v.matched_title[:45], v.confidence))
        if n_run % 20 == 0:
            print(f"    {label}: {n_run} done, flagged={flagged}, "
                  f"${cost:.3f}", flush=True)
    return {"flagged": flagged, "judged": judged, "errors": errors,
            "n": n_run, "skipped_nodate": skipped_nodate,
            "cost": cost, "examples": examples}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0,
                    help="cap candidates per group (0 = all)")
    ap.add_argument("--min-cos", type=float, default=0.55)
    ap.add_argument("--self-cos", type=float, default=0.95,
                    help="drop the top archive match above this cosine (self)")
    ap.add_argument("--date-cutoff", action="store_true",
                    help="eligible archive = entries STRICTLY BEFORE the "
                         "candidate's first_seen (correct production "
                         "simulation; replaces --self-cos)")
    ap.add_argument("--k", type=int, default=8)
    args = ap.parse_args()

    print("Loading archive + ground truth + embedding model …")
    svc = _svc()
    archive = load_archive(svc)
    dups, genuine = load_truth()
    print(f"  archive entries: {len(archive)}")
    print(f"  ground truth: {len(dups)} dups, {len(genuine)} genuine")

    judge = ArchiveDedupJudge(archive, judge_fn=make_judge_fn(),
                              min_cos=args.min_cos, k=args.k,
                              vectors_cache=DATA / "archive_dedup_vectors.npz")
    t0 = time.time()
    print("  building embedding index …", flush=True)
    judge.build_index()
    print(f"  index built ({time.time()-t0:.0f}s)\n")

    date_of = load_date_lookup() if args.date_cutoff else None
    mode = ("date-cutoff (eligible = published BEFORE candidate first_seen)"
            if args.date_cutoff else f"self_cos={args.self_cos}")
    print(f"Judging DUPS (should be flagged) — {mode}:")
    dr = run_group(judge, dups, args.self_cos, args.limit, "dups",
                   date_of=date_of)
    print(f"\nJudging GENUINE (should NOT be flagged):")
    gr = run_group(judge, genuine, args.self_cos, args.limit, "genuine",
                   date_of=date_of)

    nd, ng = dr["n"], gr["n"]
    catch = dr["flagged"] / nd if nd else 0.0
    fpr = gr["flagged"] / ng if ng else 0.0
    total_cost = dr["cost"] + gr["cost"]

    print("\n" + "=" * 60)
    print(f"ARCHIVE-DEDUP JUDGE — measured ({mode})")
    if args.date_cutoff:
        print(f"  skipped (no first_seen date): dups={dr['skipped_nodate']}, "
              f"genuine={gr['skipped_nodate']}")
    print(f"  dup-catch (recall):   {dr['flagged']}/{nd} = {catch:5.0%}  "
          f"(judged {dr['judged']}, err {dr['errors']})")
    print(f"  false-positive:       {gr['flagged']}/{ng} = {fpr:5.0%}  "
          f"(judged {gr['judged']}, err {gr['errors']})")
    print(f"  cost: ${total_cost:.3f}  ({time.time()-t0:.0f}s)")
    print("\n  baseline (cosine-only ≥0.75): 60% catch / 54% false-positive")
    print("  → the judge wins if catch is similar/higher AND FP is much lower.")
    print("\n  e.g. caught dups (candidate ~ matched):")
    for t, m, c in dr["examples"]:
        print(f"    «{t}» ~ «{m}» ({c:.0%})")
    if gr["examples"]:
        print("\n  e.g. FALSE positives (genuine flagged as dup — inspect!):")
        for t, m, c in gr["examples"]:
            print(f"    «{t}» ~ «{m}» ({c:.0%})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
