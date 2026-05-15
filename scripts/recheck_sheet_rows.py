"""Re-evaluate a range of already-pushed rows in the editor sheet against
the CURRENT classification rules (prompt + heuristics).

Use case: after tightening EDITORIAL_REVIEW_SYSTEM / heuristic_relevance,
re-check an earlier run's rows so editor-facing corrections appear
without a full re-fetch.

Two passes per row:
  1. deterministic heuristic — blacklist_hit (force-reject phrases,
     RU-aggregator P1-A), stale-archive URL guard (P1-C),
     heuristic_section (P2-B RU-market -> Local, rumor, LCV ...).
     Free, uses title + primary URL only.
  2. LLM editorial_review with the live prompt — applies the prompt-only
     rules (dealer P1-B, facelift P1-E, financial-source P2-D,
     limited-edition P2-E, motorshow/trademark P3-A/B). Uses the sheet's
     Лид (col C) as the body excerpt.

Output: a printed diff. With --apply, SAFE fixes are written back:
  • column D (Раздел) updated for section reclassifications
  • column P (Комментарий) annotated "v37-правило отклонило бы: ..."
    for new-rejects — rows are NEVER deleted; the editor stays the
    final authority. Existing non-empty col-P cells are left untouched.

NOTE: editorial_review is non-deterministic — borderline rows vary by
+/- a handful between runs. High-confidence rejects (anniversaries,
trivial facelifts, forecasts, single-country, recalls w/o RF) are
stable. Treat the annotation as a hint, not a verdict.

Usage:
  python scripts/recheck_sheet_rows.py --tab "Новости (новые)" \
      --start 64 --end 154 [--apply]

Defaults target the 14.05 v36 run (rows 64-154) for backwards-compat.
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from googleapiclient.discovery import build  # noqa: E402
from google.oauth2 import service_account  # noqa: E402

from news_agent.adapters.llm.factory import make_llm_client  # noqa: E402
from news_agent.core.config_loader import Blacklist, load_sections  # noqa: E402
from news_agent.core.heuristic_relevance import (  # noqa: E402
    _is_known_stale_archive_url,
    blacklist_hit,
    heuristic_section,
)
from news_agent.core.models import RawArticle  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Recheck pushed rows vs current rules")
    p.add_argument("--tab", default="Новости (новые)",
                   help="Target tab in the editor spreadsheet")
    p.add_argument("--start", type=int, default=64,
                   help="First data row (1-based, inclusive)")
    p.add_argument("--end", type=int, default=154,
                   help="Last data row (1-based, inclusive)")
    p.add_argument("--sheet-id", default=None,
                   help="Override target spreadsheet ID "
                        "(else EDITOR_SPREADSHEET_ID / built-in default)")
    p.add_argument("--apply", action="store_true",
                   help="Write fixes back (else dry-run)")
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    sheet_id = (
        args.sheet_id
        or os.environ.get("EDITOR_SPREADSHEET_ID")
        or "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs"
    )
    sa_path = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa_path), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = build("sheets", "v4", credentials=creds)

    rows = (
        svc.spreadsheets()
        .values()
        .get(
            spreadsheetId=sheet_id,
            range=f"'{args.tab}'!A{args.start}:S{args.end}",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
        .get("values", [])
    )
    bl = Blacklist()
    sections = load_sections()
    client = make_llm_client(get_settings())

    reject: list[tuple[int, str, str]] = []
    reclass: list[tuple[int, str, str, str]] = []
    unchanged = 0
    spent = 0.0

    for offset, row in enumerate(rows):
        sheet_row = args.start + offset
        if not row or "Прогон от" in str(row[0]):
            continue
        title = str(row[1]) if len(row) > 1 else ""
        lede = str(row[2]) if len(row) > 2 else ""
        cur_section = str(row[3]) if len(row) > 3 else ""
        url = str(row[9]) if len(row) > 9 else ""
        all_urls = str(row[12]) if len(row) > 12 else ""
        check_url = url or (all_urls.split()[0] if all_urls else "")
        if not title.strip():
            continue

        raw = RawArticle(
            url=check_url or "https://example.com/x",
            title=title,
            body=lede or title,
            source_name="recheck",
            source_url=check_url or "https://example.com/",
        )

        bh = blacklist_hit(raw, bl)
        if bh.hit:
            reject.append((sheet_row, title[:65], f"heuristic: {bh.reason}"))
            continue
        if _is_known_stale_archive_url(check_url or ""):
            reject.append((sheet_row, title[:65], "stale-archive-url (P1-C)"))
            continue

        try:
            review, u = client.editorial_review(
                title=title,
                body=lede or title,
                sections=sections,
                portal_country="Russia",
            )
            spent += u.cost_usd
        except Exception as e:  # noqa: BLE001
            print(f"  r{sheet_row}: LLM error {type(e).__name__} — skipped")
            unchanged += 1
            continue

        if not review.should_publish:
            reject.append(
                (sheet_row, title[:65], f"LLM new-prompt: {review.reason[:90]}")
            )
            continue

        h = heuristic_section(title=title, body_excerpt=lede, domain="")
        new_section = h.section if (h and h.section) else review.section
        if new_section and new_section != cur_section:
            reclass.append((sheet_row, title[:55], cur_section, new_section))
            continue
        unchanged += 1

    print(f"=== recheck {args.tab!r} rows {args.start}-{args.end} ===")
    print(f"LLM spend: ${spent:.4f}")
    print(f"  unchanged:   {unchanged}")
    print(f"  NEW-REJECT:  {len(reject)}")
    print(f"  RECLASSIFY:  {len(reclass)}")
    print()
    print("--- NEW-REJECT ---")
    for sr, t, why in reject:
        print(f"  r{sr}: {t}\n        -> {why}")
    print()
    print("--- RECLASSIFY ---")
    for sr, t, old, new in reclass:
        print(f"  r{sr}: {t}  |  {old!r} -> {new!r}")

    if not args.apply:
        print("\n(dry-run — pass --apply to write fixes)")
        return 0

    # Re-read col P so we never clobber an editor comment that already
    # exists on a target row.
    p_now = (
        svc.spreadsheets()
        .values()
        .get(
            spreadsheetId=sheet_id,
            range=f"'{args.tab}'!P{args.start}:P{args.end}",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
        .get("values", [])
    )

    def _p_busy(sr: int) -> bool:
        idx = sr - args.start
        if idx < 0 or idx >= len(p_now):
            return False
        cell = p_now[idx]
        val = (cell[0] if cell else "").strip()
        return bool(val) and not val.startswith("⚠ v37")

    data = []
    skipped = 0
    for sr, _t, _old, new in reclass:
        data.append({"range": f"'{args.tab}'!D{sr}", "values": [[new]]})
    for sr, _t, why in reject:
        if _p_busy(sr):
            skipped += 1
            continue
        data.append(
            {
                "range": f"'{args.tab}'!P{sr}",
                "values": [[f"⚠ v37-правило отклонило бы: {why}"]],
            }
        )
    if data:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": data},
        ).execute()
    print(
        f"\nAPPLIED: {len(reclass)} section fixes, "
        f"{len(reject) - skipped} reject annotations "
        f"({skipped} skipped — editor comment already present)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
