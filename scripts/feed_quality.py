"""How many USEFUL rows did each run actually deliver to the editor?

The KPI we have tracked so far answers "of what the editor published, how much
did we find" — coverage, currently ~56%. That is a recall metric, and every
improvement we shipped in the last weeks moved it: source recovery, the TLS
fix, +73% fetched volume. None of it changed what the editor experiences,
because his pain is the other direction: of the rows we HAND HIM, how many are
usable. Measured aug-04 over 822 pushed rows — 41% useful, 40% junk (25% «не
нужна», 15% duplicates). Raising coverage without raising this ratio makes
things worse: more volume, more noise, a bigger bill, same usable output.

So this is the number to optimise, and it is deliberately expressed as an
ABSOLUTE count per run («useful rows delivered»), not only as a percentage —
a run that pushes 4 rows of which 3 are good is worse for the desk than one
that pushes 12 of which 7 are.

Ground truth, in priority order per row:
  1. the editor's own verdict in his feedback (eval_set_v2, label_publish /
     the dup labels) — the strongest signal, it is his literal answer;
  2. an exact url_key hit in «Опубликованные (все)» — he ran it;
  3. otherwise unknown (usually just not reviewed yet).

Rates are computed over REVIEWED rows only, because a fresh run has almost no
feedback yet and would otherwise read as 0% useful.

Usage:
  python scripts/feed_quality.py                 # last 15 runs
  python scripts/feed_quality.py --runs 40
  python scripts/feed_quality.py --since 2026-07-28
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from news_agent.core.console import force_utf8_stdio  # noqa: E402

force_utf8_stdio()

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402

from news_agent.core.published_dedup import url_key  # noqa: E402

SHEET = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
FEED_TAB = os.environ.get("NEWS_TAB", "Новости (новые)")
PUB_TAB = "Опубликованные (все)"
EVAL = ROOT / "data" / "eval_set_v2.jsonl"

USEFUL, JUNK, UNKNOWN = "useful", "junk", "unknown"


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def load_editor_labels() -> dict[str, dict]:
    """{url_key: feedback record} from the synced editor comments."""
    if not EVAL.exists():
        return {}
    out: dict[str, dict] = {}
    for ln in EVAL.read_text(encoding="utf-8").splitlines():
        if not ln.strip():
            continue
        try:
            r = json.loads(ln)
        except (json.JSONDecodeError, ValueError):
            continue
        if r.get("provenance") != "editor_sheet":
            continue
        u = (r.get("url") or "").strip()
        if u.startswith("http"):
            out[url_key(u)] = r          # later sync wins — it is the newer read
    return out


def load_published_keys(svc) -> set[str]:  # type: ignore[no-untyped-def]
    rows = svc.spreadsheets().values().get(
        spreadsheetId=SHEET, range=f"'{PUB_TAB}'!A:R",
        valueRenderOption="UNFORMATTED_VALUE").execute().get("values", [])
    keys = set()
    for r in rows[1:]:
        u = str(r[11]).strip() if len(r) > 11 and r[11] is not None else ""
        if u.startswith("http"):
            keys.add(url_key(u))
    return keys


def verdict(key: str, labels: dict[str, dict], published: set[str]) -> str:
    """USEFUL / JUNK / UNKNOWN for one pushed row."""
    fb = labels.get(key)
    if fb:
        if fb.get("label_dup_cross_run") or fb.get("label_dup_within"):
            return JUNK
        if fb.get("label_publish") is False:
            return JUNK
        if fb.get("label_publish") is True:
            return USEFUL
    if key in published:
        return USEFUL
    return UNKNOWN


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=15,
                    help="how many recent runs to show")
    ap.add_argument("--since", default="", help="only runs on/after YYYY-MM-DD")
    args = ap.parse_args()

    svc = _svc()
    # Open-ended range: the feed only grows, and a hard A1:P2000 window was
    # silently truncating runs that crossed row 2000 — their «полезных строк
    # за прогон» read lower than reality (aug-05 audit).
    feed = svc.spreadsheets().values().get(
        spreadsheetId=SHEET, range=f"'{FEED_TAB}'!A1:P").execute().get(
            "values", [])
    labels = load_editor_labels()
    published = load_published_keys(svc)

    by_run: dict[str, dict[str, int]] = defaultdict(
        lambda: {USEFUL: 0, JUNK: 0, UNKNOWN: 0})
    for r in feed[1:]:
        run = (r[0] if r else "").strip()[:16]
        url = (r[9] if len(r) > 9 else "").strip()
        if not run or not url.startswith("http"):
            continue          # separator rows and manual entries
        if args.since and run[:10] < args.since:
            continue
        by_run[run][verdict(url_key(url), labels, published)] += 1

    runs = sorted(by_run)[-args.runs:]
    print(f"=== КАЧЕСТВО ЛЕНТЫ по прогонам ({FEED_TAB}) ===")
    print("полезно = редактор одобрил или опубликовал; брак = дубль или «не нужна»\n")
    print(f"{'прогон':18}{'строк':>7}{'полезно':>9}{'брак':>7}{'?':>5}   доля полезного")
    for run in runs:
        c = by_run[run]
        n = c[USEFUL] + c[JUNK] + c[UNKNOWN]
        rev = c[USEFUL] + c[JUNK]
        rate = f"{c[USEFUL]/rev:>4.0%}" if rev else "   —"
        print(f"{run:18}{n:>7}{c[USEFUL]:>9}{c[JUNK]:>7}{c[UNKNOWN]:>5}   {rate}"
              f"   ({rev} размечено)")

    tu = sum(by_run[r][USEFUL] for r in runs)
    tj = sum(by_run[r][JUNK] for r in runs)
    tk = sum(by_run[r][UNKNOWN] for r in runs)
    rev = tu + tj
    print(f"\nза {len(runs)} прогонов: строк {tu+tj+tk} | полезных {tu} | "
          f"брака {tj} | без вердикта {tk}")
    if rev:
        print(f"  доля полезного (по размеченным): {tu/rev:.0%}")
    if runs:
        print(f"  ПОЛЕЗНЫХ СТРОК ЗА ПРОГОН: {tu/len(runs):.1f}   ← целевая метрика")
    return 0


if __name__ == "__main__":
    sys.exit(main())
