"""Closed-loop editor feedback sync — turn col-P into a measurable
training signal automatically.

What it does (idempotent, safe to run anytime):
  1. Pull the editor sheet ('Новости (новые)') current state.
  2. For every row with a non-empty editor comment, parse the comment
     into structured labels.
  3. Diff against last sync state — only NEW comments are processed.
  4. Append fresh labels to data/eval_set.jsonl (canonical eval set).
  5. Save sync state so the next run only processes what changed.

Why this exists:
  For the last month we were doing this by hand — pull col-P, parse,
  add to eval. That meant the eval set was always 1-2 weeks stale and
  every architecture change was calibrated on outdated data. Closing
  this loop is the precondition for measuring any future change.

Signal categories captured (mapped from real comments, 895 sampled):
  • APPROVE                   38% — positive label_publish=True
  • WRONG SECTION             21% — label_section_correction
  • REJECT                    17% — label_publish=False
  • REFERENCED URL            15% — referenced_urls = [...]
  • DUP (cross-run)           12% — label_dup_cross_run=True
  • WRONG PRIMARY              6% — label_wrong_primary=True
  • DUP (within batch)         4% — label_dup_within=True
  • SOFT (hedged)              2% — soft=True (excluded from strict)
  • NEEDS TRANSLATION          2% — label_needs_translation=True

Output schema (per row, appended to data/eval_set.jsonl):
  {
    "id": "<sha1 of title+comment[:30]>",
    "title", "body", "url", "section_at_push",
    "provenance": "editor_sheet",
    "editor_comment", "synced_at", "row",
    "label_publish": bool|None,
    "label_section": str|None,         # editor's section correction
    "label_dup_cross_run": bool,
    "label_dup_within": bool,
    "label_wrong_primary": bool,
    "label_needs_translation": bool,
    "referenced_urls": list[str],
    "soft": bool,
    "raw_signals": {...}               # for audit / debugging
  }
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
TAB = "Новости (новые)"
# Google Sheets client is built lazily inside main() so importing this
# module (e.g. for unit tests on parse_comment) doesn't require env vars.
_svc = None
_EDITOR = None


def _get_svc():
    """Build the Google Sheets client lazily on first use."""
    global _svc, _EDITOR
    if _svc is not None:
        return _svc, _EDITOR
    from dotenv import load_dotenv
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    load_dotenv(ROOT / ".env", override=True)
    _EDITOR = os.environ.get(
        "EDITOR_SPREADSHEET_ID",
        "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs",
    )
    sa_path = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa_path),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    _svc = build("sheets", "v4", credentials=creds)
    return _svc, _EDITOR

STATE_FILE = DATA / "editor_sync_state.json"
# v2 — rich-label schema; v1 (eval_set.jsonl) stays alongside as legacy
# until harness is updated to read both. See ARCHITECTURE note at top.
EVAL_SET = DATA / "eval_set_v2.jsonl"
AUDIT_LOG = DATA / "editor_feedback_audit.jsonl"

# ---------- signal extraction patterns ----------------------------------
# Patterns are case-insensitive, applied to (comment.lower())

P_DUP_WITHIN = (
    r"дубль\s+(?:строки\s+)?\d+",
    r"🔁",
    r"дубль\s+из\s+",
    r"опять дубль",
)
P_DUP_CROSS = (
    r"\bпостили\b(?!\s+(?:пресс|глобал|англ|только|по))",
    r"уже\s+(?:было|постил|публик)",
    r"повтор\b",
    r"выше\s+новость\s+о\s+том\s+же",
    r"^\s*дубль\.?\s*$",
    r"\bдубль\b",
    r"перепост\b",
    r"несколько\s+раз\s+было",
    # aug-03: the editor's most common wordings for "we ran this already" were
    # invisible here — «писали» was absent from the list entirely, and «было»
    # only counted with the «уже» prefix. Measured over all 1925 stored
    # comments: 106 dup complaints went unlabelled, so the dup rate we report
    # read 13.5% instead of 19%. Same failure shape as the jul-10 red-mark
    # audit that added the wrong-primary wordings below.
    r"(?<!не\s)\bписали\b",          # «писали», «вчера писали», «нет, писали уже»
    r"^\s*было\.?\s*$",              # the whole comment is «было»
    r"\bэто\s+было\b",               # «это было в прессе ранее»
    r"\bбыло\s+\d{1,2}[.\s/]",       # «было 25.06»
    r"\d{1,2}[.\s/]\d{0,2}\w*\s+было\b",   # «7 июля было», «31 июля было»
    r"стар\w+\s+новост",             # «очень старая новость от 04.06»
    r"^\s*уже\b",                    # «уже писали ранее»
    # aug-07, the same audit one layer further out. Counting the week's own
    # feed by hand against this parser: 48 of 108 rows it called «чисто» were
    # complaints it had not recognised, and the largest group was dup wordings
    # that name WHERE the story already ran rather than saying «было» alone.
    r"было\s+в\s+\d",                # «было в 1 части», «было в 1й части»
    r"было\s+(?:вчера|сегодня|тут|и\s+тут)",
    r"было\s+(?:в|на)\s+(?:файле|портале|прессе)",
    r"^\s*нет,\s*было\b",            # «нет, было»
    r"в\s+файле\s+уже\s+была",
    r"\d+\s+новост\w*\s+об\s+одном",  # «3 новости об одном и том же»
)
P_WRONG_PRIMARY = (
    r"постили\s+пресс",
    r"постили\s+глобал",
    r"нужен\s+пресс",
    r"нужен\s+(?:первоисточник|оригин)",
    r"был\s+пресс\b",
    # jul-10 red-mark audit: the editor's dominant wordings were invisible to
    # the old patterns (23 red-J marks vs 1 text-detected wrong_primary).
    r"ссылк[аи]\s+не\s+т[ае]",
    r"источник\s+не\s+тот",
    r"неверн\w*\s+источник",
    r"источник\s+нужен\s+друг",
    r"нужно\s+искать\s+на",
)
P_NEEDS_TRANS = (
    r"нужен\s+англ",
    r"нужно\s+англ",
    r"но\s+спидми\s+нельзя",
)
P_REJECT = (
    r"не\s+(?:наша|наш)\s+тема",
    r"не\s+нужно",
    r"не\s+пост(?:им|ите)?\b",
    r"не\s+публику",
    r"не\s+подходит",
    r"не\s+относится",
    r"нет\s+смысла",
    r"не\s+интересно",
    r"^нет[\.,]?\s*$",
    r"вообще\s+не\s+(?:наша|постим|нужно)",
    r"не\s+пишем\s+про",
    r"эт?о\s+просто\s+обзор",
    r"финансирование.*не\s+постим",
    r"не\s+ставим",
    r"\bне\s+факт\b",  # explicit "это не факт"
    # aug-07: «не нужно» is only the editor's tidiest wording. Measured on the
    # week's own feed, these carried the same meaning and were all being
    # counted as clean, which is how 60 real «ок» became a reported 108.
    r"(?:нечего|нечго)\s+писать",    # «нечего писать»
    r"писать\s+нечего",              # «там писать нечего»
    r"(?:никакой|нет)\s+конкретики",
    r"не\s+думаю[,\s].*нужн",        # «не думаю, что нам это нужно»
    r"вр?р?яд\s+ли\s+нужн",          # «врряд ли нужно» (editor's typo included)
    r"не\s+особо\s+нужн",
    r"бренд\s+вообще\s+не\s+нужен",
    r"ничего\s+важного",
    r"не\s+понятно,?\s+что\s+за",    # «не понятно, что за рейтинг»
    r"шакальн\w+\s+фото",            # unusable images
    r"^\s*нет,\s",                   # «нет, ...» — a rejection with a reason
)
# APPROVE patterns. NOTE: matched only AFTER P_REJECT misses, so we
# don't have to worry about "не постим" containing "постим".
P_APPROVE = (
    r"^\s*ок[\s\.,!]",                # "ок," "ок." "ок!"
    r"^\s*ок\s*$",                    # bare "ок"
    r"^\s*ok\b",                      # English ok
    r"^\s*да[\s\.,]",
    r"^\s*да\s*$",
    r"\bпостим\b",                    # safe — P_REJECT runs first
    r"^\s*норм",
    r"^\s*годится",
    r"можно\s+(?:поставить|публик|постит)",
    r"\bв\s+факт(?:ы|ах)?\b",         # "это в факты" / "в фактах"
    r"раздел\s+факт",
    r"это\s+факт",
)
P_SOFT = (
    r"можно\s+и\s+пропустить",
    r"наверное",
    r"скорее\s+(?:да|нет)",
    r"под\s+вопросом",
    r"пограничн",
    r"не\s+приоритет",
    r"вроде\s+можно",
    r"особо\s+нечего\s+писать",
)

# Section-correction phrases → canonical section.
# Two-tier matching: STRONG cues (prefixed with «это/в/раздел/а»)
# always win; WEAK cues (bare section name at start) fire only if no
# STRONG cue matched and no other content present.
SECTION_CUES_STRONG = (
    (r"(?:это|в|раздел|а)\s+(?:фaкт|факт)", "Confirmed"),
    (r"(?:это|в|раздел|а)\s+слух",          "Rumors"),
    (r"(?:это|в|раздел|а)\s+рум",           "Rumors"),
    (r"(?:это|в|раздел|а)\s+местн",         "Local specifics"),
    (r"(?:это|в|раздел|а)\s+local",         "Local specifics"),
    (r"(?:это|в|раздел|а)\s+друг",          "Other news"),
    (r"(?:это|в|раздел|а)\s+(?:lcv|лсв)",   "LCV news"),
    (r"(?:это|в|раздел|а)\s+эконом",        "Economics"),
    (r"(?:это|в|раздел|а)\s+дилер",         "Dealer news / Promo"),
    (r"(?:это|в|раздел|а)\s+мотор",         "Motorshow"),
)
SECTION_CUES_WEAK = (
    (r"^\s*слух[аи]?\b",       "Rumors"),
    (r"^\s*местн",              "Local specifics"),
    (r"^\s*факт[ыа]?\b",        "Confirmed"),
    (r"^\s*друг(?:ие)?\b",      "Other news"),
    (r"^\s*(?:lcv|лсв)\b",      "LCV news"),
)

P_REF_URL = re.compile(r"https?://[^\s|)\]>\" ]+")


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().strip())


def _match_any(patterns: tuple[str, ...], text: str) -> bool:
    return any(re.search(p, text) for p in patterns)


def parse_comment(comment: str) -> dict:
    """Map editor's free-text col-P comment to structured labels."""
    c = _norm(comment)
    if not c:
        return {}

    dup_within = _match_any(P_DUP_WITHIN, c)
    dup_cross = _match_any(P_DUP_CROSS, c) and not dup_within
    wrong_primary = _match_any(P_WRONG_PRIMARY, c)
    needs_trans = _match_any(P_NEEDS_TRANS, c)
    soft = _match_any(P_SOFT, c)

    # publish decision: REJECT wins absolutely (so "не постим" beats
    # "постим"). Then DUP. Then APPROVE. Then unknown.
    label_publish: bool | None = None
    rejected = _match_any(P_REJECT, c)
    if rejected or dup_within or dup_cross:
        label_publish = False
    elif _match_any(P_APPROVE, c):
        label_publish = True

    # section correction — strong cues first, then bare-section weak cues
    label_section = None
    for rx, sec in SECTION_CUES_STRONG:
        if re.search(rx, c):
            label_section = sec
            break
    if label_section is None:
        for rx, sec in SECTION_CUES_WEAK:
            if re.search(rx, c):
                label_section = sec
                break

    referenced_urls = P_REF_URL.findall(comment)  # original case kept

    out = {
        "label_publish": label_publish,
        "label_section": label_section,
        "label_dup_within": dup_within,
        "label_dup_cross_run": dup_cross,
        "label_wrong_primary": wrong_primary,
        "label_needs_translation": needs_trans,
        "referenced_urls": referenced_urls,
        "soft": soft,
        "raw_signals": {
            "rejected": rejected,
            "approved": _match_any(P_APPROVE, c),
        },
    }
    return out


# ---------- IO --------------------------------------------------------

def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"last_synced_at": None, "synced_keys": {}}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=1),
        encoding="utf-8",
    )


def _comment_key(title: str, comment: str) -> str:
    """Stable key per (title, comment) for dedup across runs."""
    h = hashlib.sha1(
        ((title or "")[:80] + "|" + (comment or "")[:60])
        .encode("utf-8")
    ).hexdigest()
    return h[:16]


def _row_id(title: str, comment: str) -> str:
    return hashlib.sha1(
        ((title or "")[:120].lower() + "|" + (comment or "")[:30])
        .encode("utf-8")
    ).hexdigest()[:16]


def _pull_sheet(max_row: int = 700) -> list[list]:
    svc, editor_id = _get_svc()
    rows = svc.spreadsheets().values().get(
        spreadsheetId=editor_id, range=f"'{TAB}'!A1:P{max_row}",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute().get("values", [])
    return rows


# Editors also mark problems with a RED CELL FILL, often WITHOUT any text
# (jul-10 audit of the top-300 feed rows: 101 red-marked rows — 81 on B
# (title = не нужно/было), 23 on J (первоисточник = ссылка не та) — while
# the text-only sync had caught just 1 wrong_primary). Read the fill colour
# so those marks become labels too.
_RED_COL_LETTERS = {1: "B", 3: "D", 9: "J", 12: "M"}  # cols we interpret


def _is_red(bg: dict | None) -> bool:
    if not bg:
        return False
    return (bg.get("red", 0) > 0.75 and bg.get("green", 0) < 0.65
            and bg.get("blue", 0) < 0.65)


def _pull_red_marks(max_row: int = 700) -> dict[int, list[str]]:
    """1-based sheet row -> list of red-filled column letters (subset we map:
    B=title, D=section, J=primary URL, M=source URLs)."""
    svc, editor_id = _get_svc()
    try:
        resp = svc.spreadsheets().get(
            spreadsheetId=editor_id, ranges=[f"'{TAB}'!A1:P{max_row}"],
            fields="sheets(data(rowData(values("
                   "effectiveFormat.backgroundColor))))",
        ).execute()
        row_data = resp["sheets"][0]["data"][0].get("rowData", [])
    except Exception as e:  # noqa: BLE001 — formatting read is best-effort
        print(f"  ! red-mark read failed: {type(e).__name__} {str(e)[:60]}")
        return {}
    out: dict[int, list[str]] = {}
    for ri, row in enumerate(row_data, start=1):
        cols = []
        for ci, cell in enumerate(row.get("values", [])[:16]):
            if ci in _RED_COL_LETTERS and _is_red(
                    (cell.get("effectiveFormat") or {}).get("backgroundColor")):
                cols.append(_RED_COL_LETTERS[ci])
        if cols:
            out[ri] = cols
    return out


def main() -> int:
    # UTF-8 stdout for Windows console — must NOT run at import time
    # (breaks pytest capture). Safe to call here.
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    args = sys.argv[1:]
    dry = "--dry-run" in args
    full = "--full-resync" in args  # ignore state, re-sync everything

    state = _load_state() if not full else {"last_synced_at": None,
                                             "synced_keys": {}}
    synced_keys: dict = state.get("synced_keys", {})
    print(f"State: {len(synced_keys)} comments synced previously, "
          f"last={state.get('last_synced_at')}")

    rows = _pull_sheet()
    print(f"Sheet rows pulled: {len(rows)}")
    red_marks = _pull_red_marks()
    print(f"Red-marked rows: {len(red_marks)}")

    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
    new_entries: list[dict] = []
    skipped = 0
    stats = {
        "with_comment": 0, "new": 0, "already_synced": 0,
        "auto_merged_marker": 0,  # 🔁 ones we wrote ourselves
        "publish_true": 0, "publish_false": 0, "publish_none": 0,
        "section_correction": 0, "dup_within": 0, "dup_cross": 0,
        "wrong_primary": 0, "needs_trans": 0, "soft": 0,
        "ref_url": 0, "red_only": 0,
    }

    for i, r in enumerate(rows, 1):
        if i == 1:
            continue  # header
        if not r or not isinstance(r[0], str):
            continue
        if "━━" in r[0] or "Прогон от" in r[0]:
            continue

        title = str(r[1] if len(r) > 1 else "").strip()
        body = str(r[2] if len(r) > 2 else "").strip()
        section = str(r[3] if len(r) > 3 else "").strip()
        url = str(r[9] if len(r) > 9 else "").strip()
        comment = str(r[15] if len(r) > 15 else "").strip()
        reds = red_marks.get(i, [])

        if not comment and not reds:
            continue
        if not comment and reds:
            # Red fill with no text is still a verdict — synthesize a stable
            # pseudo-comment so the row gets an entry + dedup key.
            comment = f"[красная метка: {','.join(reds)}]"
            stats["red_only"] += 1
        # Skip the markers we wrote ourselves during dup-merge
        if comment.startswith("🔁") and "дубль строки" in comment:
            stats["auto_merged_marker"] += 1
            continue

        stats["with_comment"] += 1
        key = _comment_key(title, comment)
        if key in synced_keys:
            stats["already_synced"] += 1
            continue
        stats["new"] += 1

        parsed = parse_comment(comment)
        # Overlay red-fill signals on top of (or instead of) text labels:
        # J/M = «ссылка не та» -> wrong_primary; B = title marked bad ->
        # reject unless the text explicitly approves («ок, но…» keeps True).
        if reds:
            parsed["red_cols"] = reds
            if "J" in reds or "M" in reds:
                parsed["label_wrong_primary"] = True
            if "B" in reds and parsed.get("label_publish") is not False:
                if not parsed.get("raw_signals", {}).get("approved"):
                    parsed["label_publish"] = False
        entry = {
            "id": _row_id(title, comment),
            "title": title,
            "body": body or title,  # fallback if no lede
            "url": url,
            "section_at_push": section,
            "provenance": "editor_sheet",
            "editor_comment": comment[:400],
            "synced_at": now_iso,
            "row": i,
            **parsed,
        }
        new_entries.append(entry)
        synced_keys[key] = now_iso

        # bump stats
        lp = parsed.get("label_publish")
        if lp is True: stats["publish_true"] += 1
        elif lp is False: stats["publish_false"] += 1
        else: stats["publish_none"] += 1
        if parsed.get("label_section"): stats["section_correction"] += 1
        if parsed.get("label_dup_within"): stats["dup_within"] += 1
        if parsed.get("label_dup_cross_run"): stats["dup_cross"] += 1
        if parsed.get("label_wrong_primary"): stats["wrong_primary"] += 1
        if parsed.get("label_needs_translation"): stats["needs_trans"] += 1
        if parsed.get("soft"): stats["soft"] += 1
        if parsed.get("referenced_urls"): stats["ref_url"] += 1

    print(f"\nSync stats:")
    for k, v in stats.items():
        print(f"  {k:25} {v:>4}")

    if dry:
        print("\n(dry-run — nothing written)")
        return 0

    if new_entries:
        with EVAL_SET.open("a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        with AUDIT_LOG.open("a", encoding="utf-8") as f:
            for e in new_entries:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        print(f"\nAppended {len(new_entries)} entries to "
              f"{EVAL_SET.name} (+ audit log)")
    else:
        print("\nNo new feedback to sync.")

    state["last_synced_at"] = now_iso
    state["synced_keys"] = synced_keys
    _save_state(state)
    print(f"State updated → {STATE_FILE.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
