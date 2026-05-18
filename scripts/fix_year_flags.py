"""Re-translate rows whose col-N flag reports an EN/RU year divergence
("⚠ EN/RU расходятся по годам" / "⚠ возможно неверный год").

The translator sometimes keeps a year/quarter on one side of the
EN/RU pair and drops it on the other. _flag_review (build_news_sheet)
only DETECTS this — nothing fixes it. This pass closes the loop:

  • pick the language line that carries the fuller set of years
    (the "complete" side)
  • re-translate THAT clean line via translate_title (now governed by
    the strengthened rule 1a — year/quarter symmetry)
  • re-run _flag_review on the fresh pair; only if it comes back clean
    do we write col B (the headline) and clear the col-N flag
  • if still divergent, leave the row untouched (no worse than before)

Dry-run by default; --apply writes. Reusable for future flag sweeps.
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from googleapiclient.discovery import build  # noqa: E402
from google.oauth2 import service_account  # noqa: E402

from news_agent.adapters.llm.factory import make_llm_client  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

import build_news_sheet as bns  # noqa: E402  (reuse _flag_review/_split_lines)

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs"
)
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
creds = service_account.Credentials.from_service_account_file(
    str(SA_PATH), scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
svc = build("sheets", "v4", credentials=creds)
TAB = "Новости (новые)"
_YEAR_RE = re.compile(r"\b(20\d{2})\b")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fix EN/RU year-divergence flags")
    p.add_argument("--start", type=int, default=1)
    p.add_argument("--end", type=int, default=700)
    p.add_argument("--apply", action="store_true")
    return p.parse_args()


_TAG_RE = re.compile(r"\(([A-Za-zА-Яа-яЁё]{2,4})\)\s*$")


def _orig_tags(combined: str) -> tuple[str, str] | None:
    """Recover the ORIGINAL "(EN)/(АНГЛ)" source-language tags from the
    existing headline so a re-translation can't relabel the source.

    Returns (en_tag, ru_tag) with leading space, or None if not found.
    """
    en_t = ru_t = ""
    for line in combined.splitlines():
        s = line.strip()
        m = _TAG_RE.search(s)
        if not m:
            continue
        if s.startswith("EN:"):
            en_t = f" ({m.group(1)})"
        elif s.startswith("RU:"):
            ru_t = f" ({m.group(1)})"
    if en_t and ru_t:
        return en_t, ru_t
    return None


def _tag(src_lang: str) -> tuple[str, str]:
    code = (src_lang or "EN").upper()
    en_tag = f" ({code})"
    ru_map = {"EN": "АНГЛ", "RU": "РУС", "DE": "НЕМ", "IT": "ИТАЛ",
              "FR": "ФР", "ZH": "КИТ", "JA": "ЯП", "KO": "КОР",
              "ES": "ИСП", "UZ": "УЗБ"}
    return en_tag, f" ({ru_map.get(code, code)})"


def main() -> int:
    args = _parse_args()
    client = make_llm_client(get_settings())

    rows = (
        svc.spreadsheets().values().get(
            spreadsheetId=EDITOR,
            range=f"'{TAB}'!A{args.start}:N{args.end}",
            valueRenderOption="UNFORMATTED_VALUE",
        ).execute().get("values", [])
    )

    targets = []
    for off, row in enumerate(rows):
        sr = args.start + off
        flag = str(row[13]).strip() if len(row) > 13 else ""
        if flag.startswith("⚠") and ("год" in flag or "расходятся" in flag):
            targets.append((sr, str(row[1]) if len(row) > 1 else ""))

    print(f"=== fix_year_flags {TAB!r} — {len(targets)} flagged rows ===\n")
    writes = []
    fixed = 0
    spent = 0.0

    for sr, combined in targets:
        en, ru = bns._split_lines(combined)
        en_c = bns._strip_lang_tag(en)
        ru_c = bns._strip_lang_tag(ru)
        en_years = set(_YEAR_RE.findall(en_c))
        ru_years = set(_YEAR_RE.findall(ru_c))
        # Feed the side with the SUPERSET of years as the clean source.
        if en_years >= ru_years and en_years != ru_years:
            source = en_c
        elif ru_years >= en_years and ru_years != en_years:
            source = ru_c
        else:
            source = en_c or ru_c  # equal/ambiguous — use EN

        try:
            tp, u = client.translate_title(
                title=source, source_language_hint=None
            )
            spent += u.cost_usd
        except Exception as e:  # noqa: BLE001
            print(f"  r{sr}: translate error {type(e).__name__} — skipped")
            continue

        # Preserve the ORIGINAL source-language tags — the re-translation
        # only saw one clean line and would mislabel (e.g. a RU-source
        # story re-tagged "(EN)"). Fall back to detected only if the old
        # headline had no parseable tags.
        tags = _orig_tags(combined) or _tag(tp.source_language)
        en_tag, ru_tag = tags
        new_combined = (
            f"EN: {tp.english.strip()}{en_tag}\n"
            f"RU: {tp.russian.strip()}{ru_tag}"
        )
        new_flag = bns._flag_review(new_combined)

        status = "FIXED" if not new_flag else f"STILL FLAGGED ({new_flag[:30]})"
        print(f"r{sr}: {status}")
        print(f"  old: {combined.replace(chr(10), ' || ')[:130]}")
        print(f"  new: {new_combined.replace(chr(10), ' || ')[:130]}")
        print()

        if args.apply and not new_flag:
            writes.append({"range": f"'{TAB}'!B{sr}",
                            "values": [[new_combined]]})
            writes.append({"range": f"'{TAB}'!N{sr}", "values": [[""]]})
            fixed += 1

    print(f"LLM spend: ${spent:.4f}")
    if not args.apply:
        print("(dry-run — pass --apply to write fixes)")
        return 0
    if writes:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=EDITOR,
            body={"valueInputOption": "USER_ENTERED", "data": writes},
        ).execute()
    print(f"APPLIED: {fixed} rows re-translated + flag cleared "
          f"({len(targets) - fixed} left untouched).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
