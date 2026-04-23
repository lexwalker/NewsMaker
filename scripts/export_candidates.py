"""Export candidate articles from 'ТЕСТ статьи vN' to JSON with full body.

Pulls rows whose ``Итог бота`` column is "Точно новость" or "Возможно
новость", re-fetches each URL through our regular HTTP pipeline (with
curl_cffi + Playwright fallbacks) and extracts title+body via trafilatura,
then dumps everything into ``data/candidates_<tab>.json``.

The resulting JSON is what an out-of-band classifier (e.g. this session's
model) reads to produce classifications without burning API budget.

Usage:
    python scripts/export_candidates.py "ТЕСТ статьи v14"
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.adapters.fetchers.base import make_http_client  # noqa: E402
from news_agent.adapters.fetchers.html import extract_article  # noqa: E402
from news_agent.adapters.fetchers.impersonate import (  # noqa: E402
    CURL_CFFI_AVAILABLE,
    ImpersonateAllowlist,
    ImpersonateFetcher,
)
from news_agent.adapters.fetchers.playwright_fetcher import (  # noqa: E402
    PLAYWRIGHT_AVAILABLE,
    PlaywrightAllowlist,
    PlaywrightFetcher,
)
from news_agent.core.config_loader import load_http_quirks  # noqa: E402

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

CANDIDATE_VERDICTS = {"Точно новость", "Возможно новость"}

# Column indices from the articles tab (matches write_articles()).
# Layout after the "Лид" insert (post-v16):
COL_TITLE_CELL = 1   # B — "EN: ...\nRU: ..." or plain title
COL_URL = 3          # D
COL_VERDICT = 13     # N

# Cap body excerpt — ~1800 chars is plenty for classification and keeps
# the overall JSON portable.
BODY_MAX = 1800


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _http_get(client, url: str, pw: PlaywrightFetcher | None, pw_allow, imp, imp_allow):
    """Same routing order as batch_fetch_test._http_get."""
    if pw is not None and pw_allow is not None and pw_allow.matches(url):
        try:
            status, html = pw.fetch(url)
            return status, html
        except Exception as e:  # noqa: BLE001
            print(f"   ! Playwright failed on {url}: {type(e).__name__}")
    if imp is not None and imp_allow is not None and imp_allow.matches(url):
        try:
            status, html = imp.fetch(url)
            return status, html
        except Exception as e:  # noqa: BLE001
            print(f"   ! curl_cffi failed on {url}: {type(e).__name__}")
    r = client.get(url)
    return r.status_code, r.text


def main() -> int:
    tab = sys.argv[1] if len(sys.argv) > 1 else "ТЕСТ статьи v14"
    svc = _svc()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A2:Z"
    ).execute()
    rows = resp.get("values", [])
    print(f"Loaded {len(rows)} rows from '{tab}'.")

    candidates: list[dict] = []
    for i, row in enumerate(rows, start=2):  # sheet row number (1 = header)
        verdict = row[COL_VERDICT] if len(row) > COL_VERDICT else ""
        if verdict not in CANDIDATE_VERDICTS:
            continue
        url = row[COL_URL] if len(row) > COL_URL else ""
        title_cell = row[COL_TITLE_CELL] if len(row) > COL_TITLE_CELL else ""
        candidates.append({
            "sheet_row": i,
            "url": url,
            "title_cell": title_cell,
            "verdict": verdict,
        })
    print(f"Candidates needing classification: {len(candidates)}")

    # --- init fetchers with quirks ----------------------------------------
    quirks = load_http_quirks()
    pw_allow = PlaywrightAllowlist(quirks.playwright_domains)
    imp_allow = ImpersonateAllowlist(quirks.impersonate_domains)
    imp = (
        ImpersonateFetcher(timeout=15.0)
        if CURL_CFFI_AVAILABLE and quirks.impersonate_domains
        else None
    )
    pw_cm = None
    pw = None
    if PLAYWRIGHT_AVAILABLE and quirks.playwright_domains:
        pw_cm = PlaywrightFetcher(timeout_ms=15_000)
        pw = pw_cm.__enter__()

    # --- re-fetch each candidate to get body ------------------------------
    out_path = ROOT / "data" / f"candidates_{tab.replace(' ', '_')}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    enriched: list[dict] = []
    client = make_http_client(
        timeout=15.0,
        ssl_insecure_domains=quirks.ssl_insecure,
        url_rewrites=quirks.url_rewrites,
    )
    try:
        for idx, c in enumerate(candidates, start=1):
            if not c["url"]:
                continue
            t0 = time.monotonic()
            try:
                status, html = _http_get(client, c["url"], pw, pw_allow, imp, imp_allow)
                art = extract_article(
                    html=html, url=c["url"],
                    source_name="", source_url="", source_language=None,
                )
            except Exception as e:  # noqa: BLE001
                print(f"  [{idx}/{len(candidates)}] FETCH FAIL {c['url'][:60]}  {type(e).__name__}")
                continue
            if art is None:
                print(f"  [{idx}/{len(candidates)}] EXTRACT FAIL {c['url'][:60]}")
                continue
            body = (art.body or "").strip()
            dt_ms = int((time.monotonic() - t0) * 1000)
            enriched.append({
                "sheet_row": c["sheet_row"],
                "url": c["url"],
                "verdict": c["verdict"],
                "title": art.title,
                "body_excerpt": body[:BODY_MAX],
                "body_len": len(body),
                "language_hint": art.source_language or "",
                "published_at": art.published_at.isoformat() if art.published_at else "",
                "outbound_links_sample": (art.outbound_links or [])[:10],
            })
            print(
                f"  [{idx}/{len(candidates)}]  {dt_ms:>5}ms  "
                f"{len(body):>6}b  {art.title[:70]}"
            )
    finally:
        client.close()
        if pw_cm is not None:
            try:
                pw_cm.__exit__(None, None, None)
            except Exception:  # noqa: BLE001
                pass

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(enriched, f, ensure_ascii=False, indent=2)
    print(f"\nExported {len(enriched)} enriched candidates to: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
