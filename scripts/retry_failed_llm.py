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
import json
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
from news_agent.core.articles_schema import (  # noqa: E402
    COL,
    check_header,
    col_letter,
)
from news_agent.core.budget import BudgetExceeded, BudgetTracker  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.core.editorial_pass import (  # noqa: E402
    dup_hint_for,
    has_reject_directive,
    lang_tags_for,
    looks_like_usage_limit,
    resolve_section_region,
)
from news_agent.core.models import RawArticle  # noqa: E402
from news_agent.core.tab_handoff import resolve_articles_tab  # noqa: E402
from news_agent.core.urls import canonicalise, domain_of, url_hash  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402
from news_agent.adapters.llm.base import EDITORIAL_REVIEW_SYSTEM  # noqa: E402
from news_agent.core.cache_version import (  # noqa: E402
    compute_classifier_version,
    compute_split_versions,
)
from news_agent.core import heuristic_relevance as _heuristic_mod  # noqa: E402


def _compute_classifier_version() -> str:
    """Same recipe as the main run — the cache rows this script writes must
    carry a stamp a later run recognises, or they are silently re-classified."""
    try:
        heur_src = Path(_heuristic_mod.__file__).read_bytes()
    except Exception:  # noqa: BLE001
        heur_src = b""
    return compute_classifier_version(EDITORIAL_REVIEW_SYSTEM, heur_src)


CLASSIFIER_VERSION = _compute_classifier_version()


def _split_versions() -> tuple[str, str]:
    try:
        heur_src = Path(_heuristic_mod.__file__).read_bytes()
    except Exception:  # noqa: BLE001
        heur_src = b""
    return compute_split_versions(EDITORIAL_REVIEW_SYSTEM, heur_src)


PROMPT_VERSION, HEURISTICS_VERSION = _split_versions()

sys.path.insert(0, str(ROOT / "scripts"))  # published_archive.py lives here
import published_archive  # noqa: E402  reads "Опубликованные (все)" archive

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column indices come from the canonical schema module. The hand-copied
# block this replaces had ALREADY drifted: COL_LLM_REL=24 pointed at
# 'Hits темы' and COL_COST=25 at 'LLM relevance' (dead constants then,
# loaded traps for the next edit).
COL_TITLE = COL.TITLE
COL_LEDE = COL.LEDE
COL_URL = COL.URL
COL_SECTION = COL.SECTION
COL_REGION = COL.REGION
COL_NOTE = COL.NOTE
COL_VERDICT = COL.VERDICT
COL_LLM_REL = COL.LLM_RELEVANCE
COL_COST = COL.COST


def _cache_entry(
    *, row: list, url: str, title: str, verdict: str, relevance: str,
    section: str, region: str, confidence, title_en: str, title_ru: str,
    note: str, reason: str, eb: str, em: str, et: str, portal: str,
) -> tuple:
    """Build one SQLite cache entry mirroring the main run's cached_row shape.

    Why this exists: the main batch deliberately does NOT cache accept-graded
    rows the LLM never classified ("not a final state" — a --no-llm prog), and
    this script never added them afterwards. So a prog finished through recovery
    put NOTHING in the cache: its stories never entered recent_event_keys, and
    the NEXT run was blind to them (jul-17: 64 stories, and the editor flagged 8
    already-published dups in the following push).

    Fields this script cannot know — the heuristic scores (is_article,
    article_score, article_reasons, auto_topic, auto_hits) and primary_method —
    are omitted ON PURPOSE: the restore path defaults them safely (it keeps the
    freshly-computed score and labels the method "cached"). Everything a later
    run actually reads back for CORRECTNESS — the classification, primary_* and
    event_* — is supplied, primary_* read straight off the sheet row the fetch
    already filled.

    Honest scope note (red-team jul-17): only ACCEPTED entries short-circuit a
    future run (llm_section present → cache_is_authoritative). REJECTED entries
    have llm_section="" and a verdict outside the rule/identity sets, so a
    future run that re-fetches the URL re-classifies it — the reject entry's
    value is its EVENT KEY feeding recent_event_keys (30-day semantic dedup),
    not an LLM-cost saving.
    """
    canon_u = canonicalise(url)
    cached_row = {
        "cls_ver": CLASSIFIER_VERSION,
        # split stamps (jul-27) — same recipe as the batch, else a
        # recovered row would look legacy and re-run needlessly.
        "prompt_ver": PROMPT_VERSION,
        "heur_ver": HEURISTICS_VERSION,
        "verdict": verdict,
        "llm_relevance": relevance,
        "llm_section": section,
        "llm_region": region,
        "llm_confidence": confidence,
        "llm_title_en": (title_en or "")[:300],
        "llm_title_ru": (title_ru or "")[:300],
        "llm_note": note,
        "llm_reason": (reason or "")[:300],
        "primary_url": _get(row, COL.PRIMARY_URL),
        "primary_domain": _get(row, COL.PRIMARY_DOM),
        "primary_confidence": _get(row, COL.PRIMARY_CONF),
        "launch_brand_model": _get(row, COL.LAUNCH_BRAND_MODEL),
        "event_brand": eb,
        "event_model": em,
        "event_type": et,
    }
    return (
        url_hash(canon_u), canon_u, (title or "")[:500], None,
        domain_of(canon_u), portal,
        json.dumps(cached_row, ensure_ascii=False),
        # The LEDE, straight off the sheet row the fetch already filled. Same
        # omission as in the main run (aug-05): the store has taken an 8th
        # element since the column existed, both callers passed seven, and so
        # every dedup layer we own compares headlines because no body text was
        # ever kept anywhere.
        (_get(row, COL_LEDE) or "")[:600] or None,
    )


def _net_retry(fn, *, what: str = "sheets write", attempts: int = 5):
    """Run a Sheets write, retrying transient network failures with backoff.

    The classification this script writes is already PAID FOR and is not cached
    anywhere, so an unretried blip is pure loss (see the call site)."""
    import time as _time
    delay = 2.0
    for a in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — any transport error is worth a retry
            if a == attempts:
                print(f"  !! {what} FAILED after {attempts} attempts: "
                      f"{type(e).__name__}: {str(e)[:120]}")
                raise
            print(f"  ! {what} attempt {a}/{attempts} failed "
                  f"({type(e).__name__}: {str(e)[:70]}) — retrying in {delay:.0f}s")
            _time.sleep(delay)
            delay = min(delay * 2, 30.0)


def _svc():
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _get(r: list[str], i: int) -> str:
    return r[i] if i < len(r) else ""


HINT_PATH = ROOT / "data" / "llm_abort_recovery.json"


def _load_hint() -> dict | None:
    """Recovery handoff written by an LLM-aborted batch run (see
    batch_fetch_test): carries the aborted run's tab + window so this script
    can finish the classification and advance the state itself. state.json
    still points at the LAST healthy tab on abort — by design — so without
    the hint we would retry the WRONG tab."""
    if not HINT_PATH.exists():
        return None
    try:
        import json
        return json.loads(HINT_PATH.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        print(f"  ! recovery hint unreadable ({e}) — ignoring", file=sys.stderr)
        return None


def _consume_hint(hint: dict, cost_usd: float) -> None:
    """Full recovery succeeded: advance state to the recovered run (merge —
    RunState.save replaces the file, and the archive baselines written by the
    batch must survive), point the stage handoff at the recovered tab, drop
    the hint."""
    import json
    from news_agent.core.run_state import RunState
    st = RunState(ROOT / "data" / "state.json")
    data = st.load()
    data.update({
        "articles_tab": hint["articles_tab"],
        "last_run_at": hint["run_at"],
        "last_run_status": "ok",
        "last_run_window_start": hint["window_start"],
        "last_run_articles": int(hint.get("articles", 0)),
        "last_run_cost_usd": round(float(hint.get("cost_usd", 0)) + cost_usd, 4),
    })
    st.save(data)
    HINT_PATH.unlink(missing_ok=True)
    print(f"  state advanced to recovered run (tab '{hint['articles_tab']}'); "
          f"hint consumed — cluster/push will pick this tab up.")


def main() -> int:
    svc = _svc()
    hint = _load_hint()
    if hint and len(sys.argv) <= 1:
        tab = hint["articles_tab"]
        print(f"  tab: {tab}  (from LLM-abort recovery hint)")
    else:
        # Tab: explicit argv override, else the EXPLICIT handoff pointer from
        # the last healthy batch run (state.json articles_tab); newest-vN only
        # as a warned fallback (see tab_handoff — kills the I9 "newest tab
        # wins" poisoning and the hard-coded v18 last resort). Auto-resolve
        # also avoids Cyrillic argv (Windows/PowerShell mangles it).
        tab = resolve_articles_tab(
            svc, SHEET_ID,
            state_path=ROOT / "data" / "state.json",
            argv_tab=sys.argv[1] if len(sys.argv) > 1 else "",
        )
        print(f"  tab: {tab}")
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
        _pub_urls, _pub_recent, pub_titles = published_archive.load_published_index(
            svc, _editor_id)
        print(f"  published-archive dedup: {len(pub_titles)} all-time titles loaded")
    except Exception as e:  # noqa: BLE001 — never break recovery over the gate
        pub_titles = set()
        print(f"  !!! published-archive load FAILED (paraphrase dedup OFF this "
              f"run): {str(e)[:160]}", file=sys.stderr)

    # Dup-hint tiers 2-3 (semantic event-key + lexical brand_model vs our own
    # recent runs) — parity with the main run, which had all 3 tiers while
    # recovery had only the archive paraphrase. Read-only; failure → hints off.
    recent_ev: dict = {}
    recent_bm: dict = {}
    own_titles: set = set()
    store = None          # kept for the cache WRITE-BACK below, not just hints
    # HARDCODED for read/write parity: the main batch reads the cache under a
    # hardcoded "RU" (batch_fetch_test.py DEDUP_PORTAL), so honouring an env
    # override here would write rows the batch never reads back — silently
    # voiding the whole write-back (red-team jul-17).
    portal = "RU"
    try:
        from news_agent.adapters.storage import DedupStore
        _sqlite = ROOT / "data" / "news_agent.sqlite"
        if _sqlite.exists():
            store = DedupStore(_sqlite)
            recent_ev = store.recent_event_keys(portal, days=30)
            recent_bm = store.recent_brand_models(portal, days=30)
            own_titles = store.recent_pushed_titles(portal, days=30)
            print(f"  dup hints: {len(recent_ev)} event-keys, "
                  f"{len(recent_bm)} brand-models, "
                  f"{len(own_titles)} own titles (30d)")
    except Exception as e:  # noqa: BLE001 — advisory only
        print(f"  dup-hint store unavailable ({type(e).__name__}) — tiers 2-3 off")

    # jul-26: the initial tab READ was the last bare .execute() in this
    # script — a transient ssl.SSLEOFError here killed the whole recovery
    # chain in 16s while every WRITE was already _net_retry-wrapped.
    resp = _net_retry(
        lambda: svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"'{tab}'!A1:AH"
        ).execute(),
        what="initial tab read",
    )
    rows = resp.get("values", [])
    header = rows[0] if rows else []
    rows = rows[1:]
    # Validate the tab's header against the canonical schema — a mid-header
    # insert silently shifts every positional read AND write below (verdicts
    # into the wrong column). Fail loud instead.
    problems = check_header(header, context=tab)
    if problems:
        for p in problems:
            print(f"!!! SCHEMA MISMATCH: {p}", file=sys.stderr)
        print("Refusing to retry against a shifted schema.", file=sys.stderr)
        return 2
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
        if hint:
            _consume_hint(hint, 0.0)  # nothing broken -> run is whole
        return 0

    updates: list[dict] = []
    cache_entries: list[tuple] = []  # SQLite write-back (see _cache_entry)
    row_marks: list[int] = []  # len(updates) after each ROW's cell group —
    # chunk flushes split only at these boundaries, so a mid-flush failure can
    # never leave a row half-written (verdict updated but EVENT_*/reason not:
    # such a row LOOKS healthy to the looks_failed re-pick detector and its
    # missing cells would be silently lost forever — red-team jul-17).
    section_names = {s.name for s in sections}
    portal_country = "Russia"  # RU portal hard-coded for now
    rejected_count = 0
    consec_errors = 0  # circuit breaker — parity with the main run's LLM pass
    aborted_early = False  # any break below = PARTIAL recovery, chain must stop
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
        except BudgetExceeded as e:
            # Cap tripped mid-recovery: STOP but FLUSH what's already paid for
            # (previously this propagated and discarded every accumulated
            # update — $1-2 of recovery work thrown away, sheet unchanged).
            print(f"\n!!! LLM budget exceeded at {idx}/{len(targets)} — stopping; "
                  f"flushing {len(updates)} accumulated updates.", file=sys.stderr)
            aborted_early = True
            break
        except Exception as e:  # noqa: BLE001
            print(f"  [{idx}/{len(targets)}] EDITORIAL_REVIEW FAILED row {sheet_row}: {str(e)[:160]}")
            consec_errors += 1
            # Same abort semantics as the main run (parity for I2): a
            # usage-limit 400 or N consecutive failures of any wording is a
            # persistent outage — burning one failing call per remaining row
            # would replay the exact incident this tool exists to fix.
            if looks_like_usage_limit(str(e)) or consec_errors >= 5:
                print(f"\n!!! persistent LLM failure ({str(e)[:120]}) — aborting "
                      f"retry pass at {idx}/{len(targets)}; flushing "
                      f"{len(updates)} accumulated updates.", file=sys.stderr)
                aborted_early = True
                break
            continue
        consec_errors = 0
        reason = (review.reason or "")[:300]
        # Event-signature, normalised EXACTLY as the main run (strip → lower →
        # 40/60/24). Computed BEFORE the reject branch so BOTH paths can cache
        # it — a rejected row's key is just as useful to the next run's dedup.
        _es = getattr(review, "event_signature", None)
        _eb = ((_es.brand if _es else "") or "").strip().lower()[:40]
        _em = ((_es.model if _es else "") or "").strip().lower()[:60]
        _et = ((_es.event_type if _es else "") or "").strip().lower()[:24]
        # Reject if the model says skip, OR says publish while its own reason
        # rejects (SHARED consistency guard — same phrase list as the main
        # run). Write the reason either way so the editor sees it in the
        # rejected markup.
        if (not review.should_publish) or has_reject_directive(review.reason):
            _note = f"LLM (retry): {reason[:120]}"
            updates.append({"range": f"'{tab}'!{col_letter(COL_VERDICT)}{sheet_row}",
                            "values": [["Отклонено LLM"]]})
            updates.append({"range": f"'{tab}'!{col_letter(COL_NOTE)}{sheet_row}",
                            "values": [[_note]]})
            updates.append({"range": f"'{tab}'!{col_letter(COL.LLM_REASON)}{sheet_row}",
                            "values": [[reason]]})
            if url:
                cache_entries.append(_cache_entry(
                    row=r, url=url, title=clean_title, verdict="Отклонено LLM",
                    relevance="Нет", section="", region="", confidence="",
                    title_en="", title_ru="", note=_note, reason=reason,
                    eb=_eb, em=_em, et=_et, portal=portal))
            rejected_count += 1
            row_marks.append(len(updates))
            print(f"  [{idx}/{len(targets)}] REJECTED row {sheet_row}: {clean_title[:55]!r}")
            continue
        # Section/region via the SHARED resolver — parity fix: recovery rows
        # now get the same heuristic pre-classifier override as the main run
        # (previously a recovered row could land in a different section than
        # a mainline row with identical content).
        dec = resolve_section_region(
            review_section=review.section,
            review_region=review.region,
            review_confidence=review.confidence,
            title=clean_title,
            body_excerpt=body,
            domain=domain_of(url) if url else "",
            section_names=section_names,
        )
        section, region = dec.section, dec.region
        # Advisory dup hint — SHARED 3-tier helper, parity with the main run
        # (was: archive-paraphrase tier only). PREPENDED so the [:300] cap
        # can't amputate it; the push diverts on "возможно дуб".
        _hint = dup_hint_for(
            title=clean_title,
            event_brand=_eb,
            event_model=_em,
            event_type=_et,
            launch_brand_model="",
            canon_url=canonicalise(url) if url else "",
            pub_titles=pub_titles,
            recent_ev=recent_ev,
            recent_bm=recent_bm,
            own_titles=own_titles,
        )
        if _hint:
            reason = (_hint + (" | " + reason if reason else ""))[:300]
        try:
            tp, ut = client.translate_title(title=clean_title, source_language_hint=None)
            budget.record(ut)
        except BudgetExceeded as e:
            print(f"\n!!! LLM budget exceeded at {idx}/{len(targets)} — stopping; "
                  f"flushing {len(updates)} accumulated updates.", file=sys.stderr)
            aborted_early = True
            break
        except Exception as e:  # noqa: BLE001
            print(f"  [{idx}/{len(targets)}] TRANSLATE FAILED row {sheet_row}: {str(e)[:160]}")
            continue
        # Build new title cell with EN/RU + lang tags (SHARED 15-language map;
        # the inline 8-language copy this replaces gave a recovered Korean
        # title "(KO)" instead of the editor's "(КОР)").
        en_tag, ru_tag = lang_tags_for(tp.source_language or "")
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
        # Schedule cell updates (letters derived from the schema): title,
        # section, region, note (heuristic-override note or clear), verdict
        # promotion, cost, editorial reason.
        _L = col_letter
        updates.append({"range": f"'{tab}'!{_L(COL_TITLE)}{sheet_row}", "values": [[new_title]]})
        updates.append({"range": f"'{tab}'!{_L(COL_SECTION)}{sheet_row}", "values": [[section]]})
        updates.append({"range": f"'{tab}'!{_L(COL_REGION)}{sheet_row}", "values": [[region]]})
        updates.append({"range": f"'{tab}'!{_L(COL_NOTE)}{sheet_row}",
                        "values": [[dec.heuristic_note or ""]]})
        if verdict == "Возможно новость":
            updates.append({"range": f"'{tab}'!{_L(COL_VERDICT)}{sheet_row}",
                            "values": [["Точно новость"]]})
        updates.append({"range": f"'{tab}'!{_L(COL_COST)}{sheet_row}", "values": [[cost]]})
        updates.append({"range": f"'{tab}'!{_L(COL.LLM_REASON)}{sheet_row}", "values": [[reason]]})
        # Hybrid dedup Stage 1: persist the semantic event-signature. It was
        # already computed above (for the advisory dup hint) but never written —
        # so every row recovered through this script reached the clustering with
        # EMPTY event columns, and build_news_clusters (which reads COL.EVENT_*
        # to build its cross-run event keys) had nothing to compare. jul-17: a
        # --no-llm prog finished via this script left 64 of 76 accepted rows
        # without signatures, silently disabling the strongest 30-day dedup for
        # 84% of the batch — the editor then flagged 8 already-published stories
        # («вчера писали») in one push. Parity with the main run, which writes
        # these in write_articles.
        # (_eb/_em/_et were normalised above, before the reject branch.)
        updates.append({"range": f"'{tab}'!{_L(COL.EVENT_BRAND)}{sheet_row}", "values": [[_eb]]})
        updates.append({"range": f"'{tab}'!{_L(COL.EVENT_MODEL)}{sheet_row}", "values": [[_em]]})
        updates.append({"range": f"'{tab}'!{_L(COL.EVENT_TYPE)}{sheet_row}", "values": [[_et]]})
        if url:
            cache_entries.append(_cache_entry(
                row=r, url=url, title=clean_title,
                verdict="Точно новость", relevance="Да",
                section=section, region=region, confidence=dec.confidence,
                title_en=tp.english, title_ru=tp.russian,
                note=dec.heuristic_note or "", reason=reason,
                eb=_eb, em=_em, et=_et, portal=portal))
        row_marks.append(len(updates))

    # Persist the classification to SQLite BEFORE the network writes —
    # checkpoint-first, same ordering as the main batch. This is the local,
    # can't-SSL-fail store; writing it after the Sheets flush meant a
    # persistent Sheets outage skipped it entirely, losing the checkpoint at
    # exactly the moment it exists for (red-team jul-17). Never fatal.
    if cache_entries and store is not None:
        try:
            store.mark_many_with_cache(cache_entries)
            print(f"SQLite cache: +{len(cache_entries)} rows stored "
                  f"(cls_ver={CLASSIFIER_VERSION}) — the next run reuses these "
                  f"for dedup instead of re-classifying them.")
        except Exception as e:  # noqa: BLE001
            print(f"  ! SQLite cache write failed ({type(e).__name__}: "
                  f"{str(e)[:100]}) — continuing; next run will re-classify.",
                  file=sys.stderr)

    if updates:
        # Apply in chunks, each retried with backoff (jul-17: an unretried
        # ssl.SSLEOFError discarded ~430 classified rows, $2.67). Chunks split
        # ONLY at row boundaries (row_marks): a flat 200-cell split could cut
        # one row's cell group in half, and a mid-flush permanent failure
        # would then leave a row with its verdict written but EVENT_*/reason
        # missing — looks healthy to the looks_failed detector, never
        # re-picked, cells silently lost.
        CHUNK = 200
        chunks: list[list[dict]] = []
        cur: list[dict] = []
        prev = 0
        for mark in row_marks:
            group = updates[prev:mark]
            prev = mark
            if not group:
                continue
            if cur and len(cur) + len(group) > CHUNK:
                chunks.append(cur)
                cur = []
            cur.extend(group)
        if cur:
            chunks.append(cur)
        for ci, chunk in enumerate(chunks, start=1):
            _net_retry(
                lambda c=chunk: svc.spreadsheets().values().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={"valueInputOption": "USER_ENTERED", "data": c},
                ).execute(),
                what=f"batchUpdate chunk {ci}/{len(chunks)}",
            )
        n_translated = len(targets) - rejected_count
        print(
            f"\nDone. {n_translated} translated, {rejected_count} rejected by LLM. "
            f"Total cost: ${budget.spent_usd:.4f}"
        )
    if hint:
        if aborted_early:
            # Partial recovery: keep the hint so the next retry resumes THIS
            # tab, and stop the chain — clustering a half-classified tab would
            # push rows the editor never vetted.
            print("  hint kept (partial recovery) — re-run retry to finish; "
                  "chain stopped.", file=sys.stderr)
            return 3
        _consume_hint(hint, budget.spent_usd)
    elif aborted_early:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
