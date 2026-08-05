"""Second chance for the diverts most likely to be wrong.

The push sends suspected duplicates to «Разметка отклонённого (ИИ)». Scored
against the editor's own да/нет on 1174 diverted rows, that call is right 84%
of the time overall — but not evenly:

    arbiter (reads the text)   25 wanted of  95  -> 26% wrong
    archive tier              39 of 190        -> 21%
    other hints               27 of 171        -> 16%
    our own pushes            12 of  88        -> 14%
    event key                 83 of 630        -> 13%

So this releases the WORST tier only, and only when the story never turned up
anywhere afterwards — not in the feed, not in the editor's archive, by URL or
by brand-gated title.

Deliberately narrow, and the numbers say why. Releasing every divert whose
story never landed was validated offline first: 549 rows at 13% useful, against
~47% in the feed today — it would have tripled the noise. Even the arbiter tier
alone runs at 26%, still below the feed, so this is capped hard and flagged
loudly: the editor must be able to tell a second-chance row at a glance and
skip the batch wholesale if it is not paying off.

Nothing here calls an LLM.

  python scripts/release_second_chance.py            # dry run, prints only
  python scripts/release_second_chance.py --write
  python scripts/release_second_chance.py --write --hours 48 --max 8
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from news_agent.core.console import force_utf8_stdio  # noqa: E402

force_utf8_stdio()

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402
from rapidfuzz import fuzz  # noqa: E402

from news_agent.core.primary_source import normalise_title  # noqa: E402
from news_agent.core.published_dedup import url_key  # noqa: E402

SHEET = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
FEED_TAB = os.environ.get("NEWS_TAB", "Новости (новые)")
REVIEW_TAB = "Разметка отклонённого (ИИ)"
PUB_TAB = "Опубликованные (все)"

# Only the tier the editor's answers show as the least reliable.
ARBITER_MARK = "ии-арбитр"
# Below this the title comparison is noise — measured: at 70 it "matches" 17%
# of real duplicates and 15% of wanted stories. 75 with a brand gate is used
# here to DECLINE a release, never to declare one, so a false match only costs
# us a row we were already withholding.
TITLE_THRESHOLD = 75
MIN_TITLE_LEN = 26


def _svc():
    sa = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
    creds = service_account.Credentials.from_service_account_file(
        str(sa), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _cell(row: list, i: int) -> str:
    return row[i].strip() if len(row) > i else ""


def _title_lines(blob: str) -> list[str]:
    out = []
    for line in (blob or "").splitlines():
        n = normalise_title(re.sub(r"^(en|ru):\s*", "", line.strip(), flags=re.I))
        if n and len(n) >= MIN_TITLE_LEN:
            out.append(n)
    return out


class Landed:
    """Everything the editor has already been shown or has published."""

    def __init__(self, svc, brand_lex: list[str]) -> None:  # type: ignore[no-untyped-def]
        self._rx = {b: re.compile(rf"(?<![a-zа-яё0-9]){re.escape(b)}(?![a-zа-яё0-9])")
                    for b in brand_lex}
        self.urls: set[str] = set()
        self.by_brand: dict[str, list[str]] = defaultdict(list)

        feed = svc.spreadsheets().values().get(
            spreadsheetId=SHEET, range=f"'{FEED_TAB}'!A1:P").execute().get("values", [])
        for r in feed[1:]:
            u = _cell(r, 9)
            if u.startswith("http"):
                self.urls.add(url_key(u))
            for m in _cell(r, 12).splitlines():
                if m.strip().startswith("http"):
                    self.urls.add(url_key(m.strip()))
            for n in _title_lines(_cell(r, 1)):
                self._index(n)

        pub = svc.spreadsheets().values().get(
            spreadsheetId=SHEET, range=f"'{PUB_TAB}'!A:R",
            valueRenderOption="UNFORMATTED_VALUE").execute().get("values", [])
        for r in pub[1:]:
            u = str(r[11]).strip() if len(r) > 11 and r[11] is not None else ""
            if u.startswith("http"):
                self.urls.add(url_key(u))
            for i in (3, 4):        # EN and RU headline columns
                t = str(r[i]).strip() if len(r) > i and r[i] is not None else ""
                for n in _title_lines(t):
                    self._index(n)

    def brands(self, s: str) -> set[str]:
        return {b for b, rx in self._rx.items() if rx.search(s)}

    def _index(self, n: str) -> None:
        for b in (self.brands(n) or {""}):
            self.by_brand[b].append(n)

    def has(self, url: str, title_blob: str) -> bool:
        if url.startswith("http") and url_key(url) in self.urls:
            return True
        for q in _title_lines(title_blob):
            for b in (self.brands(q) or {""}):
                for n in self.by_brand.get(b, ()):
                    if fuzz.token_set_ratio(q, n) >= TITLE_THRESHOLD:
                        return True
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true",
                    help="actually prepend to the feed (default: dry run)")
    ap.add_argument("--hours", type=int, default=24,
                    help="how far back to look for diverted rows")
    ap.add_argument("--max", type=int, default=5,
                    help="hard cap per invocation — this tier is 26% useful, "
                         "so it must never be able to flood the feed")
    args = ap.parse_args()

    import build_news_sheet as bns

    svc = _svc()
    landed = Landed(svc, bns._build_brand_lexicon())
    print(f"уже доставлено: {len(landed.urls)} url, "
          f"{sum(len(v) for v in landed.by_brand.values())} заголовков")

    rev = svc.spreadsheets().values().get(
        spreadsheetId=SHEET, range=f"'{REVIEW_TAB}'!A1:H").execute().get("values", [])
    cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    seen_urls: set[str] = set()
    candidates, skipped = [], defaultdict(int)
    # Newest first — the review tab prepends.
    for r in rev[1:]:
        if _cell(r, 2) != "дубль":
            continue
        ctx = _cell(r, 1).lower()
        if ARBITER_MARK not in ctx:
            skipped["не тир арбитра"] += 1
            continue
        if _cell(r, 3).strip():
            skipped["редактор уже ответил"] += 1
            continue
        url, title = _cell(r, 6), _cell(r, 0)
        if not url.startswith("http"):
            skipped["без ссылки"] += 1
            continue
        if url_key(url) in seen_urls:
            skipped["повтор внутри партии"] += 1
            continue
        if landed.has(url, title):
            skipped["сюжет уже дошёл"] += 1
            continue
        seen_urls.add(url_key(url))
        candidates.append((title, url, _cell(r, 1)))
        if len(candidates) >= args.max:
            break

    print(f"\nотобрано к выпуску: {len(candidates)} (потолок {args.max}, "
          f"окно {args.hours}ч)")
    for k, v in sorted(skipped.items(), key=lambda x: -x[1]):
        print(f"   пропущено — {k}: {v}")
    print()
    for t, u, why in candidates:
        print(f"  • {t.splitlines()[0][:70]}")
        print(f"    {u[:78]}")
        print(f"    отвёл: {why[:74]}")

    if not args.write:
        print("\n(сухой прогон — ничего не записано; для записи добавьте --write)")
        return 0
    if not candidates:
        return 0

    rows = []
    stamp = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    for t, u, why in candidates:
        row = [""] * 19
        row[0] = stamp
        row[1] = t
        row[9] = u
        row[11] = "1"
        row[13] = "ВТОРОЙ ШАНС: арбитр счёл дублем, но сюжет нигде не вышел — проверьте"
        row[18] = why[:300]
        rows.append(row)
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET, range=f"'{FEED_TAB}'!A1",
        valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS",
        body={"values": rows}).execute()
    print(f"\nдобавлено в ленту: {len(rows)} строк, помечены «ВТОРОЙ ШАНС»")
    return 0


if __name__ == "__main__":
    sys.exit(main())
