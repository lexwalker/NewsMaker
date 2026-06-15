"""Harvest editor rejections from ALL push blocks → categorise + enrich the
precedent base's negatives (peer-review §2: 88 negatives is the precision
ceiling; the editor has rejected far more across the whole table).

Sources of negatives (deduped by normalised title):
  1. data/editor_decisions.json  — existing 88 negatives.
  2. data/eval_set_v2.jsonl      — every label_publish=False row (the
     structured harvest of column-P comments across all synced blocks).
  3. "Новости (новые)" column P  — live re-scan, catches anything newer
     than the last sync.

A comment is a NEGATIVE only when it says the item should NOT have been
published (taste "не нужно", duplicate, stale). Section/primary-source
notes on KEPT items are NOT negatives — they're "published but fix X".

Honesty: the col-P comments ARE eval_set_v2 (it's built from them), so
once they're in the base, eval_set_v2 can no longer be a clean test for
the retrieval classifier — measure on a FUTURE fresh push instead. This
script only writes with --write, and backs up editor_decisions.json first.

Usage:
  python scripts/harvest_negatives.py            # categorise + report, no write
  python scripts/harvest_negatives.py --write     # also enrich editor_decisions.json
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
TAB = "Новости (новые)"
DECISIONS = DATA / "editor_decisions.json"
EVAL = DATA / "eval_set_v2.jsonl"
MARKUP = DATA / "editor_markup_full.jsonl"

# Comment classification (substring on lower-cased comment).
_OK = {"ок", "ok", "ок.", "ok."}
_REJECT = ("не нужно", "не постим", "не ставить", "не пишем", "не интересу",
           "ни о ч", "не годит", "не наша тема", "согласовани", "да не пишем",
           "не публику", "вообще не", "никакой ценной", "ни какой ценной",
           "сомневаюсь, что нам", "нет тут")
_DUPSTALE = ("дубль", "уже писали", "уже была", "уже было", "повтор",
             "старая новость", "старое", "писали", "было известно",
             "было ранее", "постили")
_SECTION = ("это факты", "это местные", "это другие", "раздел", "не слухи",
            "тест-драйв", "тест-драйвы", "в местные")
_PRIMARY = ("первоисточник", "офиц", "офсайт", "оф. перво", "ссылка непра",
            "ссылка не год", "нужен англ", "с их официальн", "пресс с офи")


def _norm(t: str) -> str:
    return " ".join(str(t).lower().split())[:80]


def categorise(comment: str) -> str:
    c = comment.lower().strip()
    if c in _OK:
        return "ok"
    is_dup = any(k in c for k in _DUPSTALE)
    is_rej = any(k in c for k in _REJECT)
    is_sec = any(k in c for k in _SECTION)
    is_pri = any(k in c for k in _PRIMARY)
    # priority: a clear "don't publish" wins (becomes a negative)
    if is_rej:
        return "reject"
    if is_dup:
        return "stale_dup"
    if is_sec:
        return "wrong_section"
    if is_pri:
        return "primary_source"
    # has a comment but none matched → treat as approved-with-note
    return "note"


# Only CONTENT/taste rejections become classifier negatives. "stale_dup"
# is a RECENCY rejection (already posted / old) — orthogonal to content,
# unsolvable by a content classifier, and would mislead it ("BYD Seal
# launch" rejected as a dup ≠ "don't publish BYD Seal launches"). Those
# belong to the dedup/portal problem, not the precedent base.
_NEGATIVE_CATS = {"reject"}


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


def scan_sheet() -> list[dict]:
    """Return [{block, row, title, section, comment, cat}] for every
    commented row across all push blocks of the editor sheet."""
    svc = _svc()
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{TAB}'!A1:R300",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])

    def c(r, i):
        return str(r[i]).strip() if len(r) > i and r[i] is not None else ""

    out = []
    block = "(head)"
    for i, r in enumerate(rows):
        if r and isinstance(r[0], str) and "━" in str(r[0]):
            m = re.search(r"от ([\d.]+)", c(r, 0))
            block = m.group(1) if m else c(r, 0)[:20]
            continue
        comment = c(r, 15)
        if not comment or "Комментарий" in comment:
            continue
        title = c(r, 1)
        if not title:
            continue
        out.append({
            "block": block, "row": i + 1, "title": title,
            "section": c(r, 3), "comment": comment,
            "cat": categorise(comment),
        })
    return out


def load_eval_negatives() -> list[dict]:
    if not EVAL.exists():
        return []
    out = []
    for ln in EVAL.read_text(encoding="utf-8").splitlines():
        if not ln.strip():
            continue
        r = json.loads(ln)
        if r.get("label_publish") is not False:
            continue
        # Exclude recency rejections — a dup/stale negative is not a
        # content signal the classifier can learn from.
        if r.get("label_dup_within") or r.get("label_dup_cross_run"):
            continue
        out.append({"title": r["title"],
                    "comment": (r.get("editor_comment") or "")[:200],
                    "decision": "ОТКЛОНИЛ"})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true",
                    help="enrich editor_decisions.json (backs it up first)")
    args = ap.parse_args()

    marks = scan_sheet()
    MARKUP.write_text(
        "\n".join(json.dumps(m, ensure_ascii=False) for m in marks),
        encoding="utf-8")

    # ── markup table: block × category ──────────────────────────────
    blocks = sorted({m["block"] for m in marks}, reverse=True)
    cats = ["reject", "stale_dup", "wrong_section", "primary_source",
            "note", "ok"]
    print("=== Editor markup — all commented rows by block × category ===")
    print(f"{'block':12} " + " ".join(f"{c[:9]:>9}" for c in cats) + "   tot")
    grand = Counter()
    for b in blocks:
        bm = [m for m in marks if m["block"] == b]
        cc = Counter(m["cat"] for m in bm)
        grand.update(cc)
        print(f"{b:12} " + " ".join(f"{cc.get(c,0):>9}" for c in cats)
              + f"   {len(bm)}")
    print(f"{'TOTAL':12} " + " ".join(f"{grand.get(c,0):>9}" for c in cats)
          + f"   {sum(grand.values())}")
    print(f"\nmarkup → {MARKUP.name} ({len(marks)} commented rows)")
    n_stale = grand.get("stale_dup", 0)
    print(f"NOTE: {n_stale} 'stale_dup' rows are RECENCY rejections — NOT "
          f"added to the content base (dedup/portal problem, not taste).")

    # ── build enriched negative corpus ──────────────────────────────
    dec = json.loads(DECISIONS.read_text(encoding="utf-8"))
    existing_neg = dec.get("negative", [])
    seen = {_norm(n["title"]) for n in existing_neg}
    merged = list(existing_neg)
    added = 0

    sources = []
    sheet_negs = [{"title": m["title"], "comment": m["comment"][:200],
                   "decision": "ОТКЛОНИЛ"}
                  for m in marks if m["cat"] in _NEGATIVE_CATS]
    sources.append(("sheet col-P rejects", sheet_negs))
    sources.append(("eval_set_v2 negatives", load_eval_negatives()))

    for label, negs in sources:
        s_added = 0
        for n in negs:
            k = _norm(n["title"])
            if k and k not in seen:
                seen.add(k)
                merged.append(n)
                added += 1
                s_added += 1
        print(f"  + {label}: {len(negs)} found, {s_added} new")

    print(f"\nNEGATIVE base: {len(existing_neg)} → {len(merged)} "
          f"(+{added} unique)")
    print(f"POSITIVE base unchanged: {len(dec.get('positive', []))}")

    if args.write:
        backup = DECISIONS.with_suffix(".json.bak")
        backup.write_text(DECISIONS.read_text(encoding="utf-8"),
                          encoding="utf-8")
        dec["negative"] = merged
        DECISIONS.write_text(json.dumps(dec, ensure_ascii=False, indent=0),
                            encoding="utf-8")
        print(f"\n✓ written (backup → {backup.name}). Vectors re-embed on "
              "next judge run (cache keyed by titles hash).")
        print("⚠ eval_set_v2 negatives are now IN the base — it is no longer "
              "a clean test. Measure retrieval on a FUTURE fresh push.")
    else:
        print("\n(dry run — pass --write to enrich editor_decisions.json)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
