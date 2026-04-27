"""Repair junk primary-source URLs in the existing 'Новости' sheet.

Walks every data row, checks whether the current primary URL is junk
(facebook share, login page, root-only homepage, tracking redirector),
and if so replaces it with the article's own canonical URL. Sets
confidence to ``high`` when the article is on a press-release host /
whitelist, otherwise ``low``.

No re-fetch — uses only what's already in the sheet.

Run:  python scripts/backfill_primary_source.py
"""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.core.config_loader import (  # noqa: E402
    load_primary_source_cues,
    load_whitelist_domains,
)
from news_agent.core.primary_source import _is_junk_link, _normalise_domain  # noqa: E402
from news_agent.core.urls import domain_of  # noqa: E402

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

NEWS_TAB = "Новости"
COL_TITLE = 1   # B
COL_PRIMARY_DOM = 8  # I
COL_PRIMARY_URL = 9  # J
COL_PRIMARY_CONF = 10  # K
COL_MEMBER_URLS = 12  # M


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(r: list[str], i: int) -> str:
    return r[i] if i < len(r) else ""


def main() -> int:
    cues = load_primary_source_cues()
    whitelist = {_normalise_domain(d) for d in load_whitelist_domains()}
    pr_hosts = {_normalise_domain(h) for h in cues.press_release_hosts}

    svc = _svc()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{NEWS_TAB}'!A2:P"
    ).execute()
    rows = resp.get("values", []) or []
    print(f"Loaded {len(rows)} rows from '{NEWS_TAB}'.")

    updates: list[dict] = []
    fixed_junk = 0
    promoted_self = 0
    for i, r in enumerate(rows, start=2):
        title = _get(r, COL_TITLE)
        if not title or "EN:" not in title:
            continue  # separator or bad row
        cur_url = _get(r, COL_PRIMARY_URL)
        cur_dom = _get(r, COL_PRIMARY_DOM)
        cur_conf = _get(r, COL_PRIMARY_CONF)

        members_cell = _get(r, COL_MEMBER_URLS)
        canonical_url = ""
        for u in members_cell.splitlines():
            u = u.strip()
            if u:
                canonical_url = u
                break
        if not canonical_url:
            continue
        canonical_domain = _normalise_domain(domain_of(canonical_url))

        new_url = cur_url
        new_dom = cur_dom
        new_conf = cur_conf
        changed = False

        # 1) Junk primary URL → replace with canonical
        if cur_url and _is_junk_link(cur_url):
            new_url = canonical_url
            new_dom = domain_of(canonical_url)
            new_conf = "low"
            fixed_junk += 1
            changed = True

        # 2) Article is on a press-release host or whitelist → promote
        # to "high" self-source if currently low.
        if (
            (canonical_domain in pr_hosts
             or any(canonical_domain.endswith("." + h) for h in pr_hosts)
             or canonical_domain in whitelist)
            and (new_conf or "low") == "low"
        ):
            new_url = canonical_url
            new_dom = domain_of(canonical_url)
            new_conf = "high"
            promoted_self += 1
            changed = True

        if changed:
            updates.append({"range": f"'{NEWS_TAB}'!I{i}", "values": [[new_dom]]})
            updates.append({"range": f"'{NEWS_TAB}'!J{i}", "values": [[new_url]]})
            updates.append({"range": f"'{NEWS_TAB}'!K{i}", "values": [[new_conf]]})

    print(f"  - junk primary URLs replaced: {fixed_junk}")
    print(f"  - low → high (self on PR/whitelist host): {promoted_self}")
    print(f"  - total cell updates: {len(updates)}")

    if not updates:
        return 0

    CHUNK = 200
    for i in range(0, len(updates), CHUNK):
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": updates[i:i + CHUNK]},
        ).execute()
    print(f"Wrote updates.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
