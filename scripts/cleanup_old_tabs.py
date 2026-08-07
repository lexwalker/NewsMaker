"""Delete old 'ТЕСТ прогон vN' / 'ТЕСТ статьи vN' working tabs.

Every run allocates a fresh tab pair and nothing ever removed them: 73 tabs /
2.26M cells by jul-2026, on a 10M-cell Google Sheets ceiling that a 2-run/day
August schedule would hit. These are the BOT's working tabs (report + articles
snapshot); the editor-facing feed («Новости (новые)»), the rejected-markup tab
and the editor's archive are untouched by name-pattern construction.

Safety:
  * keeps the newest KEEP versions of each family (default 12 ≈ two weeks);
  * always keeps the tab pointed to by data/state.json (articles_tab) and its
    paired report tab, regardless of age;
  * dry-run by default — prints the plan; deletes only with --apply.

Run:  python scripts/cleanup_old_tabs.py            # dry-run
      python scripts/cleanup_old_tabs.py --apply    # delete
      python scripts/cleanup_old_tabs.py --keep 20 --apply
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from google.oauth2.service_account import Credentials  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
FAMILIES = ("ТЕСТ прогон v", "ТЕСТ статьи v",
            "ТЕСТ прогон (гор) v", "ТЕСТ статьи (гор) v")


def _svc():
    creds = Credentials.from_service_account_file(
        str(SA_PATH), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", type=int, default=12,
                    help="newest versions of EACH family to keep (default 12)")
    ap.add_argument("--apply", action="store_true",
                    help="actually delete (default: dry-run)")
    args = ap.parse_args()

    svc = _svc()
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()

    # Handoff pointers of EVERY lane, not just the full one. The hot lane has
    # its own state file and its own tab family; it survived the aug-07 cleanup
    # only because its pinned tab happened to be among the newest 12 of its
    # family. That is luck, not a guarantee — a lane that has been quiet while
    # the other one ran would age its pointer out and the next cluster/push
    # would look for a tab that no longer exists. Cheap to make explicit.
    protected: set[str] = set()
    for state_file in sorted((ROOT / "data").glob("state*.json")):
        try:
            st = json.loads(state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        tab = (st.get("articles_tab") or "").strip()
        if not tab:
            continue
        protected.add(tab)
        # …and its paired report tab, whatever family it belongs to.
        m = re.match(r"^ТЕСТ статьи( \(гор\))? v(\d+)$", tab)
        if m:
            protected.add(f"ТЕСТ прогон{m.group(1) or ''} v{m.group(2)}")

    total_cells = 0
    victims: list[tuple[str, int, int]] = []   # (title, sheetId, cells)
    for fam in FAMILIES:
        members = []
        for s in meta.get("sheets", []):
            p = s["properties"]
            t = p["title"]
            if t.startswith(fam) and t[len(fam):].isdigit():
                g = p.get("gridProperties", {})
                cells = g.get("rowCount", 0) * g.get("columnCount", 0)
                members.append((int(t[len(fam):]), t, p["sheetId"], cells))
        members.sort(reverse=True)          # newest first
        for _, t, sid, cells in members[args.keep:]:
            if t in protected:
                continue
            victims.append((t, sid, cells))
        total_cells += sum(c for _, _, _, c in members)

    freed = sum(c for _, _, c in victims)
    print(f"working tabs: cells now ~{total_cells:,}; "
          f"keep {args.keep}/family + protected {sorted(protected)}")
    print(f"to delete: {len(victims)} tabs, freeing ~{freed:,} cells")
    for t, _, c in victims:
        print(f"  - {t}  ({c:,} cells)")

    if not victims:
        return 0
    if not args.apply:
        print("\nDRY-RUN (no changes). Re-run with --apply to delete.")
        return 0

    # Last gate before an irreversible batch, and the reason it exists: from
    # aug-07 this runs UNATTENDED at the end of every chain. The family+isdigit
    # filter above should make a non-working tab unreachable, but "should" is
    # not what you want standing between a scheduled task and the editor's
    # archive. Re-derive the name test independently here: anything that is not
    # literally «ТЕСТ прогон/статьи [(гор)] vЧИСЛО» aborts the whole deletion
    # rather than being skipped, because if one unexpected title got this far
    # the selection logic is wrong and the rest of the list is not trustworthy
    # either.
    SAFE = re.compile(r"^ТЕСТ (прогон|статьи)( \(гор\))? v\d+$")
    stray = [t for t, _, _ in victims if not SAFE.match(t)]
    if stray:
        print(f"\n!!! ОТМЕНА: в списке на удаление есть листы вне шаблона: "
              f"{stray[:5]} — ничего не удалено.", file=sys.stderr)
        return 2
    hit = sorted(set(t for t, _, _ in victims) & protected)
    if hit:
        print(f"\n!!! ОТМЕНА: под удаление попали закреплённые листы {hit} "
              f"— ничего не удалено.", file=sys.stderr)
        return 2

    # Batch the deletions (irreversible — that's why --apply is explicit).
    reqs = [{"deleteSheet": {"sheetId": sid}} for _, sid, _ in victims]
    CHUNK = 40
    for i in range(0, len(reqs), CHUNK):
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID, body={"requests": reqs[i:i + CHUNK]}
        ).execute()
    print(f"\nDELETED {len(victims)} tabs (~{freed:,} cells freed).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
