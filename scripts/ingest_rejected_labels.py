"""Rejection-labelling ritual, step 2 — read the editor's verdicts on the
sampled bot-rejects and route them (peer-review §2).

For each labelled row in the "Разметка отклонённого (ИИ)" tab:
  не нужно → confirmed_negative → appended to editor_decisions.json
             negatives (editor agrees the bot was right to reject)
  нужно    → false_reject → a RECALL HOLE: logged to data/recall_misses.jsonl
             with the heuristic that killed it (so we can prune harmful
             blacklist phrases / fix scoring), AND added as a POSITIVE
             precedent (title + the editor's section if given)
  (blank)  → skip

Idempotent: each row's url_hash is recorded in data/labeled_rejects.jsonl
once ingested, so re-running never double-counts. editor_decisions.json is
backed up before writing.

Usage: python scripts/ingest_rejected_labels.py        # dry run (report)
       python scripts/ingest_rejected_labels.py --write  # apply to base
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

from news_agent.core.labeling import (  # noqa: E402
    CONFIRMED_NEGATIVE,
    FALSE_REJECT,
    route_reject_label,
    summarise_labels,
)

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
TAB = "Разметка отклонённого (ИИ)"
DECISIONS = DATA / "editor_decisions.json"
RECALL_MISSES = DATA / "recall_misses.jsonl"
LABELED = DATA / "labeled_rejects.jsonl"


def _norm(t: str) -> str:
    return " ".join(str(t).lower().split())[:80]


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def _load_ingested() -> set[str]:
    if not LABELED.exists():
        return set()
    out = set()
    for ln in LABELED.read_text(encoding="utf-8").splitlines():
        if ln.strip():
            try:
                out.add(json.loads(ln)["url_hash"])
            except Exception:
                pass
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    svc = _svc()
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{TAB}'!A1:H300",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])

    def c(r, i):
        return str(r[i]).strip() if len(r) > i and r[i] is not None else ""

    ingested = _load_ingested()
    routed: list[tuple[str, dict]] = []
    fresh_hashes = []
    for r in rows:
        uh = c(r, 6)
        if not uh or uh == "url_hash":          # header / instructions / blank
            continue
        verdict_cell = c(r, 4)
        decision = route_reject_label(verdict_cell)
        row = {"url_hash": uh, "title": c(r, 1), "reason": c(r, 2),
               "cause": c(r, 3), "section": c(r, 5), "url": c(r, 7)}
        if decision == "skip":
            continue
        if uh in ingested:                       # already applied in a prior run
            continue
        routed.append((decision, row))
        fresh_hashes.append(uh)

    s = summarise_labels(routed)
    print(f"=== Ritual ingest — tab {TAB!r} ===")
    print(f"  new labelled rows: {s['labeled']}  "
          f"(confirmed-negatives {s['confirmed_negative']}, "
          f"false-rejects {s['false_reject']})")
    if s["false_reject_by_cause"]:
        print("  RECALL HOLES (bot wrongly rejected) by killing heuristic:")
        for cause, n in sorted(s["false_reject_by_cause"].items(),
                               key=lambda x: -x[1]):
            print(f"    {n:3}  {cause}")
    if not routed:
        print("  nothing new to ingest (editor hasn't labelled, or all done).")
        return 0

    # examples
    for d, row in routed[:6]:
        tag = "НУЖНА (recall-дыра)" if d == FALSE_REJECT else "подтв. отклонение"
        print(f"    [{tag}] {row['title'][:50]} ({row['cause']})")

    if not args.write:
        print("\n(dry run — pass --write to update editor_decisions.json + logs)")
        return 0

    dec = json.loads(DECISIONS.read_text(encoding="utf-8"))
    DECISIONS.with_suffix(".json.bak").write_text(
        json.dumps(dec, ensure_ascii=False), encoding="utf-8")
    pos, neg = dec.get("positive", []), dec.get("negative", [])
    pos_seen = {_norm(p["title"]) for p in pos}
    neg_seen = {_norm(n["title"]) for n in neg}
    added_neg = added_pos = 0
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")

    with RECALL_MISSES.open("a", encoding="utf-8") as rm, \
            LABELED.open("a", encoding="utf-8") as lab:
        for d, row in routed:
            k = _norm(row["title"])
            if d == CONFIRMED_NEGATIVE:
                if k and k not in neg_seen:
                    neg_seen.add(k)
                    neg.append({"title": row["title"],
                                "comment": "редактор подтвердил отклонение",
                                "decision": "ОТКЛОНИЛ"})
                    added_neg += 1
            elif d == FALSE_REJECT:
                rm.write(json.dumps({
                    "ts": ts, "title": row["title"], "cause": row["cause"],
                    "killed_by": row["reason"], "section": row["section"],
                    "url": row["url"]}, ensure_ascii=False) + "\n")
                if k and k not in pos_seen:
                    pos_seen.add(k)
                    pos.append({"title": row["title"],
                                "section": row["section"] or "Other news",
                                "decision": "ОПУБЛИКОВАЛ"})
                    added_pos += 1
            lab.write(json.dumps({"url_hash": row["url_hash"], "decision": d,
                                  "ts": ts}, ensure_ascii=False) + "\n")

    dec["negative"], dec["positive"] = neg, pos
    DECISIONS.write_text(json.dumps(dec, ensure_ascii=False, indent=0),
                        encoding="utf-8")
    print(f"\n✓ base updated: +{added_neg} negatives, +{added_pos} positives "
          f"(now {len(neg)} neg / {len(pos)} pos; backup written).")
    print(f"  recall holes → {RECALL_MISSES.name}; "
          f"ingested marks → {LABELED.name} (idempotent).")
    print("  vectors re-embed on next judge run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
