"""Re-run the LLM editorial review on rows the main run left unclassified
because the LLM failed mid-batch (``editorial_review error: 403`` etc. in the
``Пометка бота`` column, or a row with no section / no EN: translation).

Used when the API hit a transient block/hard-cap mid-run; once it lifts this
script touches only the broken rows and rewrites their LLM fields IN PLACE:
section, region, EN/RU title, AND the editorial reason ("Обоснование LLM",
col AE). It uses the SAME consolidated ``editorial_review`` call as the main
run (the editorial constitution), so recovered rows are judged identically and
the reason column + the rejected-markup tab (which is built from that reason)
are populated. Far cheaper than a full re-fetch.

Run:  python scripts/retry_failed_llm.py          # auto-picks newest "ТЕСТ статьи vN"
      python scripts/retry_failed_llm.py "ТЕСТ статьи v18"   # explicit tab
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

from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.core.budget import BudgetTracker  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.core.dedup import published_dup_hint  # noqa: E402
from news_agent.core.models import RawArticle  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

sys.path.insert(0, str(ROOT / "scripts"))  # published_archive.py lives here
import published_archive  # noqa: E402  reads "Опубликованные (все)" archive

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column indices (matches write_articles())
COL_TITLE = 1
COL_LEDE = 2
COL_URL = 3
COL_SECTION = 4
COL_REGION = 5
COL_NOTE = 12
COL_VERDICT = 14
COL_LLM_REL = 24
COL_COST = 25


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(r: list[str], i: int) -> str:
    return r[i] if i < len(r) else ""


def main() -> int:
    svc = _svc()
    # Tab name: explicit argv, else auto-pick the newest "ТЕСТ статьи vN".
    # Auto-pick avoids passing a Cyrillic tab name on argv (Windows/PowerShell
    # mangles it), which is why this is the preferred invocation.
    if len(sys.argv) > 1:
        tab = sys.argv[1]
    else:
        _meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
        _cand = [s["properties"]["title"] for s in _meta["sheets"]
                 if s["properties"]["title"].startswith("ТЕСТ статьи v")]

        def _vn(t: str) -> int:
            try:
                return int(t.rsplit("v", 1)[1])
            except (ValueError, IndexError):
                return -1

        tab = max(_cand, key=_vn) if _cand else "ТЕСТ статьи v18"
        print(f"  auto-picked newest tab: {tab}")
    settings = get_settings()
    sections = load_sections()
    budget = BudgetTracker(getattr(settings, "max_cost_usd", 5.0))
    client = make_llm_client(settings)
    # Route editorial_review through EDITORIAL_MODEL (e.g. Sonnet) like the main
    # run, so a recovered batch is judged by the same model; translate stays on
    # the cheaper default.
    editorial_client = client
    _ed_model = os.environ.get("EDITORIAL_MODEL", "").strip()
    if _ed_model and _ed_model != getattr(client, "model", ""):
        editorial_client = make_llm_client(settings)
        editorial_client.model = _ed_model
    print(f"  provider: {client.provider_name}  model: {client.model}"
          + (f"  editorial: {editorial_client.model}" if editorial_client is not client else ""))

    # Anti-dup parity with the main run: load the editor's published archive
    # so a recovered row that paraphrases an already-published story gets the
    # same "возможно дубль" hint (→ diverted to review by the push). Resilient:
    # any failure → empty set → published_dup_hint stays silent, never breaks.
    try:
        _editor_id = os.environ.get(
            "EDITOR_SPREADSHEET_ID",
            "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
        _pub_urls, pub_titles = published_archive.load_published_index(
            svc, _editor_id)
        print(f"  published-archive dedup: {len(pub_titles)} recent titles loaded")
    except Exception as e:  # noqa: BLE001 — never break recovery over the gate
        pub_titles = set()
        print(f"  published-archive load failed (paraphrase dedup off): {e!s:80}")

    resp = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab}'!A1:AB"
    ).execute()
    rows = resp.get("values", [])
    header = rows[0] if rows else []
    rows = rows[1:]
    # Find rows that need a retry: classify error in note OR Точно новость
    # with empty section.
    targets: list[tuple[int, list[str]]] = []
    for i, r in enumerate(rows, start=2):
        verdict = _get(r, COL_VERDICT)
        note = _get(r, COL_NOTE)
        section = _get(r, COL_SECTION)
        title = _get(r, COL_TITLE)
        body = _get(r, COL_LEDE)
        # Three retry-eligible cases:
        #  • verdict == "Точно новость" but classify/translate failed
        #    (looks_failed below) — the v18-era hard-cap-mid-run scenario
        #  • verdict == "Возможно новость" — heuristic-only graded, never
        #    saw LLM relevance check (e.g. --no-llm prog)
        #  • verdict == "Точно новость" without EN: prefix — same offline
        #    case for higher-confidence rows
        if verdict not in ("Точно новость", "Возможно новость"):
            continue
        looks_failed = (
            "classify error" in note
            or "translate error" in note
            or "relevance error" in note
            or "editorial_review error" in note
            or (not section)
            or ("EN:" not in title)
        )
        if looks_failed and title and body:
            targets.append((i, r))
    print(f"Found {len(targets)} rows needing retry.")
    if not targets:
        return 0

    updates: list[dict] = []
    section_names = {s.name for s in sections}
    portal_country = "Russia"  # RU portal hard-coded for now
    rejected_count = 0
    for idx, (sheet_row, r) in enumerate(targets, start=1):
        url = _get(r, COL_URL)
        title = _get(r, COL_TITLE)
        verdict = _get(r, COL_VERDICT)
        # If the title was the original scraped string (no EN: prefix),
        # use it as the source headline. Otherwise drop the prefix
        # because LLM should translate from the original.
        clean_title = title
        if "EN:" in title:
            for line in title.splitlines():
                if line.strip().startswith("EN:"):
                    clean_title = line.split(":", 1)[1].strip()
                    break
        body = _get(r, COL_LEDE)

        # Consolidated editorial review — the SAME path as the main run (so a
        # recovered row is judged by the editorial constitution, not the old
        # legacy is_automotive+classify_section it replaced) AND, crucially, it
        # returns the `reason` that fills "Обоснование LLM" (col AE). The legacy
        # path never produced a reason, so a mid-run-failure recovery left col AE
        # — and the rejected-markup tab built from it — empty.
        try:
            review, ur = editorial_client.editorial_review(
                title=clean_title, body=body,
                sections=sections, portal_country=portal_country,
            )
            budget.record(ur)
        except Exception as e:  # noqa: BLE001
            print(f"  [{idx}/{len(targets)}] EDITORIAL_REVIEW FAILED row {sheet_row}: {e!s:80}")
            continue
        reason = (review.reason or "")[:300]
        _rlow = (review.reason or "").lower()
        # Reject if the model says skip, OR says publish while its own reason
        # rejects (same consistency guard as the main run). Write the reason
        # either way so the editor sees it in the rejected markup.
        if (not review.should_publish) or any(
            p in _rlow for p in ("отклонить", "reject", "не публику", "не наша тема")
        ):
            updates.append({"range": f"'{tab}'!O{sheet_row}",
                            "values": [["Отклонено LLM"]]})
            updates.append({"range": f"'{tab}'!M{sheet_row}",
                            "values": [[f"LLM (retry): {reason[:120]}"]]})
            updates.append({"range": f"'{tab}'!AE{sheet_row}", "values": [[reason]]})
            rejected_count += 1
            print(f"  [{idx}/{len(targets)}] REJECTED row {sheet_row}: {clean_title[:55]!r}")
            continue
        section = review.section if review.section in section_names else "Other news"
        region = review.region or "Global"
        # Archive paraphrase dedup — parity with the main run's LLM pass: an
        # already-published story recovered here under a divergent headline
        # gets a 'возможно дубль' hint appended to the reason, so the push
        # diverts it to review instead of the clean feed. Advisory only.
        _es = getattr(review, "event_signature", None)
        if _es is not None and pub_titles:
            _dup = published_dup_hint(clean_title, _es.brand, _es.model, pub_titles)
            if _dup:
                reason = ((reason + " " + _dup) if reason else _dup)[:300]
        try:
            tp, ut = client.translate_title(title=clean_title, source_language_hint=None)
            budget.record(ut)
        except Exception as e:  # noqa: BLE001
            print(f"  [{idx}/{len(targets)}] TRANSLATE FAILED row {sheet_row}: {e!s:80}")
            continue
        # Build new title cell with EN/RU + lang tags
        lang = (tp.source_language or "").lower()[:2]
        lang_map = {
            "en": ("EN", "АНГЛ"), "ru": ("RU", "РУС"), "de": ("DE", "НЕМ"),
            "fr": ("FR", "ФР"),  "it": ("IT", "ИТАЛ"), "es": ("ES", "ИСП"),
            "zh": ("ZH", "КИТ"), "ja": ("JA", "ЯП"),
        }
        en_tag, ru_tag = lang_map.get(lang, (lang.upper(), lang.upper()))
        en_suffix = f" ({en_tag})" if en_tag else ""
        ru_suffix = f" ({ru_tag})" if ru_tag else ""
        new_title = (
            f"EN: {tp.english[:220]}{en_suffix}\n"
            f"RU: {tp.russian[:220]}{ru_suffix}"
        )
        cost = round(ur.cost_usd + ut.cost_usd, 5)
        spent = budget.spent_usd
        print(
            f"  [{idx}/{len(targets)}] OK row {sheet_row}: "
            f"section={section} region={region} cost=${cost} (run total ${spent:.4f})"
        )
        # Schedule cell updates: B (title), E (section), F (region), M (clear),
        # O (promote 'Возможно' → 'Точно'), AA (cost), AE (editorial reason —
        # the fix: fills "Обоснование LLM" so the column + rejected markup
        # aren't empty after a recovery).
        updates.append({"range": f"'{tab}'!B{sheet_row}", "values": [[new_title]]})
        updates.append({"range": f"'{tab}'!E{sheet_row}", "values": [[section]]})
        updates.append({"range": f"'{tab}'!F{sheet_row}", "values": [[region]]})
        updates.append({"range": f"'{tab}'!M{sheet_row}", "values": [[""]]})
        if verdict == "Возможно новость":
            updates.append({"range": f"'{tab}'!O{sheet_row}",
                            "values": [["Точно новость"]]})
        updates.append({"range": f"'{tab}'!AA{sheet_row}", "values": [[cost]]})
        updates.append({"range": f"'{tab}'!AE{sheet_row}", "values": [[reason]]})

    if updates:
        # Apply in chunks
        CHUNK = 200
        for i in range(0, len(updates), CHUNK):
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=SHEET_ID,
                body={"valueInputOption": "USER_ENTERED", "data": updates[i:i + CHUNK]},
            ).execute()
        n_translated = len(targets) - rejected_count
        print(
            f"\nDone. {n_translated} translated, {rejected_count} rejected by LLM. "
            f"Total cost: ${budget.spent_usd:.4f}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
