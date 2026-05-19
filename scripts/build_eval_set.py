"""Build data/eval_set.jsonl — the frozen labeled ground-truth set.

Sources (all already collected this session):
  • editor_comments_latest / NEW_TABLE / new110 .json — the editor's
    col-P verdicts on bot-surfaced rows (positive, negative, section
    correction, duplicate). Deduped by (title, comment).
  • published_2.json — the editor's ACTUAL published feed: clean
    positive labels + true section.

Label semantics (honest, conservative):
  • label_publish True/False — only when the verdict is unambiguous.
  • soft=True for hedged verdicts ("не приоритет", "писать нечего",
    "можно и пропустить") → kept for reference, EXCLUDED from strict
    precision/recall by the harness.
  • label_section — canonical section when the editor corrected it,
    or the feed's section for published items.
  • label_dup True when the editor called it a duplicate.
  • provenance — col_P_comment | published_feed.

"not in published feed" is NOT used as a negative (we may simply never
have fetched it). Negatives come ONLY from explicit col-P "не постим".
"""

from __future__ import annotations

import hashlib
import io
import json
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

# --- Russian editor-shorthand → label -----------------------------------
_NEG = (
    "не постим", "не пост", "не ставим", "не нужно", "не относится",
    "не наша", "не наш", "неактуальн", "не публику", "не интересно",
    "это не новость", "не по теме", "вообще не", "нет смысла",
)
_POS = (
    "постим", "постит", "постили", "пост,", " да", "да,", "да.",
    " ок", "ок,", "ок.", "норм", "можно поставить", "оставля",
    "согласен", "годится", "это в друг", "это в мест", "это факт",
    "это рум", "это слух", "это дилер", "это лсв", "это lcv",
    "это эконом", "это другие", "это местные", "в факты", "в местные",
    "в другие", "в румор",
)
_DUP = (
    "дубль", "дублир", "уже было", "уже писал", "уже постил",
    "повтор", "было выше", "несколько раз", "та же новость",
)
_SOFT = (
    "не приоритет", "пограничн", "лучше уточнить", "писать нечего",
    "можно и пропустить", "скорее нет", "скорее да", "не думаю что",
    "вроде можно", "наверное", "под вопросом", "можно было бы",
)
_SECTION_CUES = (
    ("факт", "Confirmed"),
    ("местн", "Local specifics"),
    ("друг", "Other news"),
    ("слух", "Rumors"),
    ("рум", "Rumors"),
    ("лсв", "LCV news"),
    ("lcv", "LCV news"),
    ("эконом", "Economics"),
    ("дилер", "Dealer news / Promo"),
    ("моторшоу", "Motorshow"),
)


def _label_from_comment(comment: str) -> dict:
    c = (comment or "").strip().lower()
    if not c:
        return {}
    soft = any(p in c for p in _SOFT)
    dup = any(p in c for p in _DUP)
    # Negation check first — "не постим" must beat "постим" substring.
    neg = any(p in c for p in _NEG)
    pos = (not neg) and any(p in c for p in _POS)
    section = ""
    for cue, sec in _SECTION_CUES:
        # only trust a section cue in an assignment phrase
        if (f"это {cue}" in c or f"в {cue}" in c or f"в {sec.lower()}" in c
                or f"это в {cue}" in c):
            section = sec
            break
    label: dict = {}
    if dup:
        label["label_dup"] = True
        label["label_publish"] = False  # the duplicate copy isn't published
    elif neg:
        label["label_publish"] = False
    elif pos or section:
        label["label_publish"] = True
        if section:
            label["label_section"] = section
    else:
        return {}  # unparseable verdict — skip (don't guess)
    if soft:
        label["soft"] = True
    return label


def _rid(title: str, extra: str = "") -> str:
    return hashlib.sha1(
        (title.strip().lower() + "|" + extra).encode("utf-8")
    ).hexdigest()[:16]


def main() -> int:
    records: dict[str, dict] = {}

    # 1. col-P comments — latest first (most recent read, superset),
    #    then NEW_TABLE / new110 uniques.
    comment_files = [
        ("editor_comments_latest.json", "c", "sec"),
        ("editor_comments_NEW_TABLE.json", "comment", "section"),
        ("editor_comments_new110.json", "c", "sec"),
    ]
    seen_tc: set[tuple[str, str]] = set()
    for fname, ckey, _seckey in comment_files:
        fp = DATA / fname
        if not fp.exists():
            continue
        for rec in json.loads(fp.read_text(encoding="utf-8")):
            title = (rec.get("title") or "").strip()
            comment = (rec.get(ckey) or "").strip()
            if not title or not comment:
                continue
            tc = (title[:60], comment[:50])
            if tc in seen_tc:
                continue
            seen_tc.add(tc)
            lab = _label_from_comment(comment)
            if not lab:
                continue
            rid = _rid(title, comment[:30])
            records[rid] = {
                "id": rid,
                "title": title,
                "body": title,  # col-P rows: no stored lede → title only
                "url": rec.get("url", ""),
                "provenance": "col_P_comment",
                "editor_comment": comment[:200],
                **lab,
            }

    # 2. published feed — clean positives + true section.
    pf = DATA / "published_2.json"
    if pf.exists():
        for rec in json.loads(pf.read_text(encoding="utf-8")):
            title = (rec.get("title") or "").strip()
            if not title:
                continue
            rid = _rid(title, "pub")
            if rid in records:
                continue
            import re as _re

            bullets = _re.sub(r"<[^>]+>", " ",
                              rec.get("bullets") or "").strip()
            records[rid] = {
                "id": rid,
                "title": title,
                "body": (bullets or title)[:1000],
                "url": rec.get("url", ""),
                "provenance": "published_feed",
                "label_publish": True,
                "label_section": rec.get("section", ""),
            }

    out = DATA / "eval_set.jsonl"
    with out.open("w", encoding="utf-8") as f:
        for r in records.values():
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # summary
    vals = list(records.values())
    pos = sum(1 for r in vals if r.get("label_publish") is True)
    neg = sum(1 for r in vals if r.get("label_publish") is False)
    dup = sum(1 for r in vals if r.get("label_dup"))
    soft = sum(1 for r in vals if r.get("soft"))
    secn = sum(1 for r in vals if r.get("label_section"))
    byprov: dict[str, int] = {}
    for r in vals:
        byprov[r["provenance"]] = byprov.get(r["provenance"], 0) + 1
    print(f"eval_set.jsonl written: {len(vals)} records")
    print(f"  publish=True : {pos}")
    print(f"  publish=False: {neg}")
    print(f"  duplicates   : {dup}")
    print(f"  soft (excl)  : {soft}")
    print(f"  with section : {secn}")
    print(f"  by provenance: {byprov}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
