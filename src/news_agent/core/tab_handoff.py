"""Explicit articles-tab handoff between pipeline stages.

The fetch stage allocates a fresh 'ТЕСТ статьи vN' tab per run; cluster,
push and retry used to pick their input as "the newest vN tab" — three
divergent implementations, each with a hard-coded 'ТЕСТ статьи v18'
fallback. That made the production chain's state "whatever tab is newest":
a manual 1-source push created v24 and the production cluster+push consumed
it as input (incident I9); a renamed tab family would silently reprocess
the ancient v18.

Since the anti-silent-failure package, a HEALTHY batch run writes the tab
it produced into data/state.json ("articles_tab"). This resolver prefers
that explicit pointer; the newest-vN scan remains only as a WARNED fallback
for state written before the field existed. No silent last resort: if
neither source yields a tab, it raises.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

TAB_PATTERN = re.compile(r"^ТЕСТ статьи v(\d+)$")


def resolve_articles_tab(
    svc,
    spreadsheet_id: str,
    *,
    state_path: Path,
    argv_tab: str = "",
) -> str:
    """The articles tab the pipeline stage should consume.

    Priority:
      1. explicit ``argv_tab`` (operator override — always wins);
      2. state.json ``articles_tab`` — the tab the last HEALTHY batch run
         actually wrote (verified to still exist in the spreadsheet);
      3. newest 'ТЕСТ статьи vN' by version — WARNED fallback only;
      4. nothing → RuntimeError (fail loud, never a hard-coded tab).
    """
    if argv_tab.strip():
        return argv_tab.strip()

    pointed = ""
    try:
        state = json.loads(Path(state_path).read_text(encoding="utf-8"))
        pointed = (state.get("articles_tab") or "").strip()
    except (OSError, json.JSONDecodeError):
        pass

    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]

    if pointed and pointed in titles:
        return pointed
    if pointed:
        print(
            f"WARNING: state.json articles_tab={pointed!r} not found in the "
            f"spreadsheet — falling back to newest-vN scan.", file=sys.stderr,
        )

    best, best_n = "", -1
    for t in titles:
        m = TAB_PATTERN.match(t)
        if m and int(m.group(1)) > best_n:
            best_n, best = int(m.group(1)), t
    if best:
        print(
            f"WARNING: no articles_tab pointer in {state_path} — using newest "
            f"tab {best!r}. If a manual/ad-hoc tab is newest, this is the I9 "
            f"poisoning case; prefer running after a healthy batch run.",
            file=sys.stderr,
        )
        return best

    raise RuntimeError(
        "No articles tab found: state.json has no valid articles_tab pointer "
        "and no 'ТЕСТ статьи vN' tab exists in the spreadsheet."
    )
