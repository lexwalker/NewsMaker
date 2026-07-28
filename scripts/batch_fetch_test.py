"""Offline batch test: fetch + extract from N real sources, no LLM calls.

What it does for each source:
  1. try to detect an RSS feed (autodiscovery + /rss, /feed, /feed.xml)
  2. if RSS found  → parse entries, for first K entries fetch + extract
     else           → treat as HTML index: discover article-like links,
                      for first K fetch + extract
  3. per article, decide "is this a news item?" with heuristics only:
       - title present
       - body ≥ 200 chars
       - published_at present OR URL path looks like an article slug
  4. collect per-source stats and write them into the ТЕСТ прогон tab.

Run:  python scripts/batch_fetch_test.py
"""

from __future__ import annotations

import argparse
import io
import json
import os
import socket
import ssl
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlparse

import feedparser
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.adapters.fetchers.html import (  # noqa: E402
    extract_article,
    _DATE_PATH_RE,
    _looks_like_article,
)
from news_agent.adapters.fetchers.telegram import (  # noqa: E402
    is_telegram_url,
    parse_channel_html,
    to_channel_preview_url,
)
from news_agent.adapters.fetchers import nhtsa_recalls  # noqa: E402
from news_agent.adapters.fetchers.base import make_http_client, host_matches  # noqa: E402
from news_agent.adapters.fetchers.impersonate import (  # noqa: E402
    CURL_CFFI_AVAILABLE,
    ImpersonateAllowlist,
    ImpersonateFetcher,
)
from news_agent.adapters.fetchers.playwright_fetcher import (  # noqa: E402
    PLAYWRIGHT_AVAILABLE,
    STEALTH_AVAILABLE,
    PlaywrightAllowlist,
    PlaywrightFetcher,
)
from news_agent.core.config_loader import (  # noqa: E402
    SourceQuality,
    load_blacklist,
    load_brand_domains,
    load_http_quirks,
    load_primary_source_cues,
    load_hot_sources,
    load_source_quality,
    load_whitelist_domains,
)
from news_agent.core.freshness import is_fresh, is_in_window  # noqa: E402
from news_agent.core.sheets_util import clamp_cells  # noqa: E402
from news_agent.core.run_state import RunState, RunWindow  # noqa: E402
from news_agent.core import fetch_checkpoint  # noqa: E402
from news_agent.core.primary_source import (  # noqa: E402
    CorpusEntry,
    arbitration_candidates,
    detect_earliest_in_corpus,
    detect_primary_source,
    _is_deep_url,
)
from news_agent.core.urls import url_hash, year_in_url_path  # noqa: E402
from news_agent.core.heuristic_relevance import (  # noqa: E402
    blacklist_hit,
    grade_article,
    heuristic_section,
    is_auto_or_economy,
    is_dzen_listicle,
    is_multi_news_title,
    is_ru_transport_civic,
    is_supplier_abstract_showcase,
    looks_like_article,
)
from news_agent.core import heuristic_relevance as _heuristic_mod  # noqa: E402
from news_agent.core.cache_version import (  # noqa: E402
    cache_is_authoritative,
    compute_split_versions,
    llm_cache_survives_prompt_change,
    PROMPT_CHANGE_BOTH,
    PROMPT_CHANGE_PUBLISH_ONLY,
    PROMPT_CHANGE_REJECT_ONLY,
    compute_classifier_version,
)
from news_agent.core.published_dedup import already_published  # noqa: E402
import published_archive  # noqa: E402  (scripts/ on path; reads daily archive)
from news_agent.core.launch_stages import (  # noqa: E402
    detect_launch_stages,
    extract_brand_model,
)
from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.adapters.llm.base import (  # noqa: E402
    EDITORIAL_REVIEW_SYSTEM,
    LLMClient,
)
from news_agent.adapters.storage import DedupStore  # noqa: E402
from news_agent.core.budget import BudgetExceeded, BudgetTracker  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.core.urls import (  # noqa: E402
    canonicalise,
    domain_of,
    normalise_source_entry,
)
from news_agent.settings import get_settings  # noqa: E402

# ----------------------------------------------------------------- config
NUM_SOURCES = 400         # effectively all active sources (there are ~357 flagged "1")
MAX_ARTICLES = 10_000     # soft cap
ITEMS_PER_SOURCE = 15  # default cap; high-volume sources get more (see below).

# High-volume sources publish 30-50+ articles/day. After may-2026 recall
# audit (May 7-8 missed ~37 articles from sources in our list), we bumped
# these specifically to 35. Other sources stay at 15 — most publish <10/day
# anyway and the cap doesn't bite. Match by domain substring.
_HIGH_VOLUME_DOMAINS = frozenset({
    # RU mass-market portals
    "auto.ru", "auto.mail.ru", "autonews.ru", "autostat.ru",
    "kolesa.ru", "abreview.ru", "autoreview.ru", "iz.ru",
    # interfax was missing entirely (jul-24 funnel: 6 editor pubs/week from
    # a wire agency stuck at the default cap 15) — plain omission.
    "interfax.ru",
    "tass.ru", "ria.ru", "kommersant.ru", "rg.ru", "vedomosti.ru",
    "lenta.ru", "rbc.ru", "naavtotrasse.ru", "motor.ru",
    "ixbt.com", "drom.ru", "motorpage.ru", "zr.ru",
    # Global mass-market portals (English)
    "carnewschina.com", "cnevpost.com", "carscoops.com",
    "motor1.com", "autoevolution.com", "thekoreancarblog.com",
    "motortrend.com", "bmwblog.com", "electrek.co",
    "cleantechnica.com", "hearst.com",
    # Telegram aggregator channels
    "t.me",
})
ITEMS_PER_SOURCE_HIGH_VOLUME = 35

# Pure-automotive portals whose index page exposes 40-60 FRESH auto articles —
# the flat 35 cap was silently truncating their tail every run (measured
# jul-08 via the prod extractor: auto.ru/mag 55, naavtotrasse 56, carscoops 53,
# electrek 52, ixbt 47, zr.ru 44). Those dropped 20 articles/fetch are a real
# chunk of the S2 miss-funnel (auto.ru alone = 18 missed editor pubs / 2wk).
# Deepen the cap for THESE only. General-news portals (ria/lenta/vedomosti/
# kommersant/rbc/rg/tass/iz) expose 150-200 links that are MOSTLY non-auto, so
# a deep cap there just burns fetches the cheap relevance heuristic drops before
# the LLM — they stay at 35. The 90s per-source wall-clock budget bounds either.
_DEEP_INDEX_DOMAINS = frozenset({
    "carscoops.com", "electrek.co", "auto.ru", "zr.ru", "autostat.ru",
    "naavtotrasse.ru", "auto.mail.ru", "ixbt.com", "carnewschina.com",
    "cnevpost.com", "motor1.com", "motortrend.com", "bmwblog.com",
    "drom.ru", "kolesa.ru", "motorpage.ru", "autoreview.ru", "abreview.ru",
    "autonews.ru", "motor.ru", "autoevolution.com", "thekoreancarblog.com",
    "cleantechnica.com",
    # jul-24 coverage push (boss: охват первичен). Probe-measured: iz.ru
    # rubric pages expose 65-67 fresh links each (root 165) vs the old cap
    # 35 — ~2x truncation, 3 editor pubs/week lost. iz.ru is fetch-healthy
    # (WARP exclusion + Playwright for DDoS-Guard already in place).
    "iz.ru",
})
ITEMS_PER_SOURCE_DEEP = 60

# Source-specific caps above the deep tier (jul-24, probe-measured).
# tass.ru/rss/v2.xml is a FIXED 100-item rolling window covering only ~4h of
# TASS output (~600 items/day): cap 60 discards 40% of every fetch for no
# reason — take the whole window; the run cadence (day prog + 23:30 evening
# prog + 4h hot lane) does the rest. The cheap relevance heuristic drops
# non-auto items before any LLM cost.
_SOURCE_CAP_OVERRIDES: dict[str, int] = {
    "tass.ru": 100,
}


def _items_cap_for(url: str) -> int:
    """Per-source item cap: explicit overrides first, then pure-auto
    deep-index portals 60, other high-volume domains 35, everything else 15.
    Matches by domain incl. subdomains."""
    host = domain_of(url)

    def _member(pool: frozenset[str]) -> bool:
        return host in pool or any(host.endswith("." + d) for d in pool)

    for d, cap in _SOURCE_CAP_OVERRIDES.items():
        if host == d or host.endswith("." + d):
            return cap
    if _member(_DEEP_INDEX_DOMAINS):
        return ITEMS_PER_SOURCE_DEEP
    if _member(_HIGH_VOLUME_DOMAINS):
        return ITEMS_PER_SOURCE_HIGH_VOLUME
    return ITEMS_PER_SOURCE
HTTP_TIMEOUT = 20.0  # was 10.0 — several slow-but-alive sources (gov.ru, OEM
                     # press rooms) need more patience. Retries are still
                     # DISABLED on timeouts, so this just widens the window once.
ENABLE_LLM = True         # flip to False to run pure heuristics again
LLM_BUDGET_USD = 5.0      # hard cap — abort LLM calls if exceeded
# Circuit breaker: this many CONSECUTIVE editorial_review failures = a
# persistent outage (org-disabled, credit-exhausted, bad model id, 429 storm)
# → abort the pass loudly instead of silently burning every remaining call.
CONSEC_LLM_ERRORS_ABORT = 5
# Published-archive sanity floors (the archive is ~6000 rows; thousands of
# URLs / dozens of recent titles are the legitimate minimum). Below these the
# dedup gate is degraded — a state that has silently occurred 3 times.
ARCHIVE_URLS_FLOOR = 1000
ARCHIVE_TITLES_FLOOR = 20
# Per-source wall-clock budget. One slow source used to eat 26+ min (the
# thekoreancarblog case: every article ran the full impersonate→playwright
# fallback chain) and the team's workaround was disabling Playwright wholesale.
# The budget bounds ANY slow source: once exceeded, remaining article links are
# skipped and the truncation is recorded in r.error (so slow sources SURFACE in
# the report instead of hiding). This is the prerequisite for re-enabling
# Playwright safely. 0 disables the budget.
SOURCE_BUDGET_S = float(os.environ.get("SOURCE_BUDGET_S", "90") or 0)
FRESHNESS_HOURS = int(os.environ.get("FRESHNESS_HOURS", "24"))  # legacy fallback (ad-hoc runs)
SQLITE_PATH = Path(os.environ.get("SQLITE_PATH", "./data/news_agent.sqlite"))
STATE_PATH = Path(os.environ.get("RUN_STATE_PATH", "./data/state.json"))
RUNS_LOG_PATH = Path(os.environ.get("RUNS_LOG_PATH", "./data/runs.log"))

# Active fetch window for this run. Populated in main() from RunState +
# CLI flags, then read by _score_article when filtering articles by date.
# Set to None when the run is in legacy mode (--no-window) and falls back
# to FRESHNESS_HOURS for backwards-compat with manual one-off runs.
RUN_WINDOW: RunWindow | None = None

# Real Telegram channels that the editor uses (from 'Новости опубликованные').
# We prepend them to the source list so the test run actually exercises the
# Telegram adapter end-to-end, not just the HTML/RSS paths.
TELEGRAM_SEED_URLS = [
    "https://t.me/chinamashina_news",
    "https://t.me/sergtselikov",
    "https://t.me/autopotoknews",
]

# NHTSA US-recalls structured source (editor request: "все отзывные компании
# по США искать тут"). Appended to the source list when enabled (default on);
# routed to the nhtsa_recalls adapter, NOT scraped. Lookback is wide to absorb
# NHTSA's ~3-5 day publishing lag — the published-archive + cache dedup
# collapses anything already surfaced, so a wide window only adds new
# campaigns. Disable with NHTSA_RECALLS=0.
ENABLE_NHTSA_RECALLS = os.environ.get("NHTSA_RECALLS", "1").strip() != "0"
NHTSA_RECALLS_SENTINEL = nhtsa_recalls.RECALLS_ENDPOINT
NHTSA_RECALL_LOOKBACK_DAYS = int(os.environ.get("NHTSA_RECALL_LOOKBACK_DAYS", "10") or 10)
NHTSA_RECALL_MAX_ITEMS = int(os.environ.get("NHTSA_RECALL_MAX_ITEMS", "50") or 50)

# LLM primary-source arbitration: when a redistribution-portal repost links to
# ≥2 plausible primaries at once, the heuristic tiers can't tell the brand's own
# announcement from a related-reading link — a cheap targeted LLM call reads the
# body and picks (editor: «не видит первоисточник»; the autonews→carscoops vs
# jeland.ru bug). Fires rarely (contested reposts only), result is cached with
# the row. Disable with LLM_PRIMARY_PICK=0.
PRIMARY_LLM_PICK = os.environ.get("LLM_PRIMARY_PICK", "1").strip() != "0"

# LLM dup arbiter (jul-23, editor: «писали уже давно» slipping through): the
# deterministic dedup layers stay silent on near-misses (cross-language
# paraphrase vs the all-time archive, gray-zone event keys); a cheap Haiku
# call reads those candidates and answers «то же событие?». Advisory only —
# a match diverts the row to the review tab. Cleared by the jul-23 offline
# eval on editor labels (6/6 reachable dups caught incl. the Geely-A7
# «писали давно» case; 2/27 approved rows flagged, both factually-correct
# repeat calls; ~$0.002/call). Disable with LLM_DUP_ARBITER=0.
DUP_ARBITER_ON = os.environ.get("LLM_DUP_ARBITER", "1").strip() != "0"

# Resumable fetch: checkpoint each source's rows so a mid-fetch crash (power
# outage) resumes from where it stopped instead of re-parsing all ~340 sources
# (jul-15 outage killed a run at 173/341). Disable with FETCH_RESUME=0. A
# checkpoint older than FETCH_RESUME_MAX_AGE_H is treated as an abandoned run
# (stale window) and discarded → clean full fetch.
FETCH_RESUME = os.environ.get("FETCH_RESUME", "1").strip() != "0"
FETCH_RESUME_MAX_AGE_H = float(os.environ.get("FETCH_RESUME_MAX_AGE_H", "12") or 12)

USER_AGENT = os.environ.get("USER_AGENT", "NewsMakerBot/0.1 (+test)")

# Global politeness throttle (jun-21): minimum seconds between ANY two fetches,
# to cap the IP's request rate so WAF/CDN (Akamai on brand media) don't
# rate-block mid-run. The bulk run blocks sources that pass in ISOLATION ->
# load-based rate-limiting (verified: 9/10 blocked brand-media return 200 when
# probed alone). Off by default (0); set FETCH_MIN_INTERVAL to enable.
_FETCH_MIN_INTERVAL = float(os.environ.get("FETCH_MIN_INTERVAL", "0") or 0)
_LAST_FETCH_T = [0.0]
REPORT_TAB_BASE = "ТЕСТ прогон"
ARTICLES_TAB_BASE = "ТЕСТ статьи"

# Populated in main(). Global so HTML + Telegram branches share the same
# final-URL deduplication set.
WHITELIST: set[str] = set()
SEEN_FINAL_URLS: set[str] = set()
# Split-tunnel proxy (config/http_quirks.yaml). Set in main() so the curl_cffi
# path in _http_get can route foreign hosts through the proxy while .ru go
# direct — mirroring the httpx client's routing.
PROXY_URL: str = ""
PROXY_DIRECT: set[str] = set()
# Editor's published archive (loaded fresh each run in main): url_keys of
# every published source + normalised titles of recently-published items.
PUBLISHED_URLS: set[str] = set()
PUBLISHED_TITLES: set[str] = set()  # recent 21d — hard-reject gate
PUBLISHED_ALL_TITLES: set[str] = set()  # all-time (minus test-drive) — advisory dup-hint
BLACKLIST = None  # type: ignore[assignment]  # set to a Blacklist instance in main()
BRANDS: list = []  # type: ignore[type-arg]  # BrandDomainEntry list from config
DEDUP_STORE = None  # type: ignore[assignment]  # DedupStore instance in main()
# url_hash → cached classification fields from earlier runs (SQLite).
# A row whose hash is here doesn't go through the LLM again — instead its
# cached verdict / section / titles / primary source get copied back.
PREVIOUSLY_SEEN: dict[str, dict] = {}


# ── Classifier versioning (cache invalidation) ──────────────────────
# The classification cache (PREVIOUSLY_SEEN) used to be keyed by url_hash
# ALONE. Consequence: a prompt/heuristic edit had ZERO effect on already
# cached articles — they short-circuited with the stale verdict, so the
# only way to see a rule change was to wait for genuinely fresh articles
# (the documented "топчемся" pain; same-day re-runs were degenerate).
#
# Fix (peer-review §4): stamp every cache row with a CLASSIFIER_VERSION =
# sha256(EDITORIAL_REVIEW_SYSTEM + heuristic_relevance.py source)[:8].
# On restore, a row whose stamp != the current version is NOT honoured
# for rule-based verdicts → it re-runs through the live heuristics + LLM.
# Editing the prompt OR any heuristic auto-bumps the version (we hash the
# module SOURCE, so there's no manual constant to forget), so rule edits
# apply on the very next prog — even a same-day re-run. Identity verdicts
# (URL-dups, stale-by-date) stay version-independent: a dup is a dup.
def _heuristics_source() -> bytes:
    try:
        return Path(_heuristic_mod.__file__).read_bytes()
    except Exception:
        return b""  # missing source → degrade to prompt-only fingerprint


def _compute_classifier_version() -> str:
    return compute_classifier_version(EDITORIAL_REVIEW_SYSTEM, _heuristics_source())


CLASSIFIER_VERSION = _compute_classifier_version()

# jul-27 cost work — SPLIT stamps. The combined version above stays (legacy
# rows, forensics), but restore decisions now use two independent stamps:
#   PROMPT_VERSION      — drives LLM verdicts only
#   HEURISTICS_VERSION  — drives rule verdicts only, comments/docstrings
#                         stripped (editing a COMMENT used to invalidate the
#                         whole expensive LLM cache — the #1 own-goal).
# Plus PROMPT_CHANGE: when a constitution edit only ADDS a reject rule, it
# cannot rescue anything, so cached LLM REJECTS remain valid and only the
# ~5x fewer accepted rows re-run. Declare it with PROMPT_CHANGE=reject_only
# (or publish_only) on the first run after such an edit; default "both" is
# the safe, always-correct behaviour.
PROMPT_VERSION, HEURISTICS_VERSION = compute_split_versions(
    EDITORIAL_REVIEW_SYSTEM, _heuristics_source())
def _resolve_prompt_change() -> tuple[str, str]:
    """(direction, source). Env wins; otherwise a pending declaration file.

    jul-28: constitution edits happen BETWEEN runs, and the scheduled
    23:30 prog knows nothing about them — the jul-27 evening run paid
    PROMPT_CHANGE=both after a publish-only edit (~$1 wasted). Whoever
    edits the prompt now drops the direction into
    data/prompt_change.json together with the prompt_ver it applies to;
    the next run picks it up automatically. The file is consumed after a
    SUCCESSFUL classification pass (see _consume_prompt_change) and is
    ignored outright once the live prompt_ver no longer matches — a stale
    declaration must never silently keep skipping re-classification.
    """
    env = os.environ.get("PROMPT_CHANGE", "").strip()
    if env:
        return env, "env"
    try:
        d = json.loads(PROMPT_CHANGE_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — missing/unreadable = no declaration
        return PROMPT_CHANGE_BOTH, "default"
    direction = str(d.get("direction", "")).strip()
    applies_to = str(d.get("prompt_ver", "")).strip()
    if direction not in (PROMPT_CHANGE_REJECT_ONLY, PROMPT_CHANGE_PUBLISH_ONLY):
        return PROMPT_CHANGE_BOTH, "default"
    if applies_to and applies_to != PROMPT_VERSION:
        return PROMPT_CHANGE_BOTH, f"file-stale(for {applies_to})"
    return direction, "file"


PROMPT_CHANGE_PATH = Path(__file__).resolve().parents[1] / "data" / "prompt_change.json"
PROMPT_CHANGE, PROMPT_CHANGE_SRC = _resolve_prompt_change()


def _consume_prompt_change() -> None:
    """Drop the declaration once a run has actually used it."""
    if PROMPT_CHANGE_SRC != "file":
        return
    try:
        PROMPT_CHANGE_PATH.unlink(missing_ok=True)
        print(f"  prompt-change declaration consumed ({PROMPT_CHANGE}) — "
              f"later runs go back to '{PROMPT_CHANGE_BOTH}'.")
    except Exception as e:  # noqa: BLE001 — never fail a run over this
        print(f"  ! could not consume prompt-change file: {type(e).__name__}")
DEDUP_PORTAL = "RU"  # current batch treats every source as RU portal
PRIMARY_CUES = None  # type: ignore[assignment]  # PrimarySourceCues from config
SOURCE_QUALITY: SourceQuality | None = None  # set in main()
# Playwright fallback — only used when a URL matches PW_ALLOWLIST.
PW_FETCHER: PlaywrightFetcher | None = None
PW_ALLOWLIST: PlaywrightAllowlist | None = None
# curl_cffi (Chrome JA3 impersonation) — for sites that block httpx at
# the TLS layer but serve static HTML once past the gate.
IMP_FETCHER: ImpersonateFetcher | None = None
IMP_ALLOWLIST: ImpersonateAllowlist | None = None

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SOURCES_TAB = os.environ["SOURCES_TAB_RU"]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# -------------------------------------------------------------- data types
@dataclass
class ArticleRow:
    source_idx: int
    source_url: str
    article_idx: int
    article_url: str
    title: str = ""
    published_at: str = ""
    body_len: int = 0
    has_image: bool = False
    image_url: str = ""           # actual URL (P0 — task spec asks for URL, not flag)
    source_language: str = ""     # e.g. "en" / "de" / "ru" — used in the title tag "(de)"
    is_article: bool = False
    article_score: float = 0.0
    article_reasons: str = ""
    auto_topic: bool = False
    auto_hits: str = ""
    verdict: str = ""  # "Точно новость" | "Возможно новость" | ...
    # Kept in-memory only (not written to Sheets) — feeds the LLM.
    body_excerpt: str = ""
    # LLM fields — populated only for certain_news / possible_news
    llm_relevance: str = ""       # "Да" / "Нет" / ""
    llm_section: str = ""
    llm_region: str = ""
    llm_confidence: float | str = ""
    llm_title_en: str = ""
    llm_title_ru: str = ""
    llm_cost_usd: float | str = ""
    llm_note: str = ""            # e.g. "требует ручной проверки — Test-drive"
    # Primary-source detection (levels 1+2)
    primary_url: str = ""
    primary_domain: str = ""
    primary_confidence: str = ""  # "high" / "medium" / "low"
    primary_method: str = ""      # "body-link" / "corpus-earlier" / "self" / "llm-pick"
    source_hint_domain: str = ""  # outlet the article credits («источник: X»)
    # Contested outbound links for LLM primary arbitration (redistribution-host
    # link-soup only). Non-empty → _run_llm_pass asks the LLM which is the true
    # source. Transient (not cached): a cache hit already restores the resolved
    # primary_url from the prior run's arbitration.
    primary_candidates: list[str] = field(default_factory=list)
    # Reconstructed from SQLite cache on a repeat run — skip the LLM pass.
    from_cache: bool = False
    # Phase-1 model-launch lifecycle tracking (heuristic, no LLM)
    launch_stages: list[str] = field(default_factory=list)
    launch_brand_model: str = ""
    # LLM editorial review: short reason for accept/reject. Visible to
    # editor in dedicated sheet column so they understand what the bot saw.
    llm_reason: str = ""
    # Hybrid dedup Stage 1: semantic event-signature from the LLM
    # (normalised brand/model/event_type). "" when absent.
    event_brand: str = ""
    event_model: str = ""
    event_type: str = ""


@dataclass
class SourceResult:
    url: str
    detected_type: str = ""
    feed_url: str = ""
    http_status: int | str = ""
    error: str = ""
    articles_attempted: int = 0
    articles_with_title: int = 0
    articles_with_body: int = 0
    articles_with_date: int = 0
    articles_with_image: int = 0
    news_like: int = 0
    passed_is_article: int = 0
    passed_auto_topic: int = 0
    elapsed_ms: int = 0
    sample_titles: list[str] = field(default_factory=list)
    sample_passed: list[str] = field(default_factory=list)


# ---------------------------------------------------- sheet helpers
def sheets_client():  # type: ignore[no-untyped-def]
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def read_active_sources(svc, limit: int) -> list[str]:  # type: ignore[no-untyped-def]
    resp = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"'{SOURCES_TAB}'")
        .execute()
    )
    rows = resp.get("values", [])
    out: list[str] = []
    normalised = 0
    dropped: list[str] = []
    for row in rows[1:]:  # skip header
        if not row:
            continue
        active = (row[0] if len(row) > 0 else "").strip()
        raw = (row[1] if len(row) > 1 else "").strip()
        # Accept anything that is NOT explicitly "0" (flags "1", "2", empty
        # are all considered active). Editor has three flags in the sheet:
        #   1 — daily-monitored automotive sources
        #   2 — OEM pressrooms and IR pages (secondary)
        #   (empty) — unmarked, but URL is valid
        if active == "0" or not raw:
            continue
        # The editor writes sources in mixed forms ("kolesa.ru",
        # "vesti.ru/auto"). Requiring an http scheme here used to SILENTLY
        # skip 33 active sources (drom, motor.ru, zr.ru, lenta, rbc …) —
        # the biggest single coverage hole the miss-funnel found.
        url = normalise_source_entry(raw)
        if url is None:
            dropped.append(raw[:40])
            continue
        if not raw.startswith(("http://", "https://")):
            normalised += 1
        out.append(url)
        if len(out) >= limit:
            break
    if normalised:
        print(f"sources: {normalised} scheme-less entries normalised to https://")
    if dropped:
        print(f"sources: {len(dropped)} non-URL rows dropped: {dropped}")
    return out


import re as _re


def _next_version(existing: list[str], base: str) -> int:
    """Given existing tab names and a base like 'ТЕСТ статьи', pick the next version.

    Rules:
      • if the bare `base` exists but `base v2`/`base v3`… do not, the next is 2;
      • else next is max(found) + 1;
      • starts at 2 so that v1 ≡ the original `base` tab (with manual review).
    """
    pat = _re.compile(rf"^{_re.escape(base)}\s+v(\d+)$")
    versions = [int(m.group(1)) for t in existing if (m := pat.match(t))]
    if versions:
        return max(versions) + 1
    return 2  # first new version after the bare base


def allocate_new_tabs(svc) -> tuple[str, str]:  # type: ignore[no-untyped-def]
    """Create a fresh pair of tabs for this run. Returns (report_tab, articles_tab)."""
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    tabs = [s["properties"]["title"] for s in meta["sheets"]]
    v_report = _next_version(tabs, REPORT_TAB_BASE)
    v_articles = _next_version(tabs, ARTICLES_TAB_BASE)
    # Keep report and articles on the same version number for readability.
    v = max(v_report, v_articles)
    report_tab = f"{REPORT_TAB_BASE} v{v}"
    articles_tab = f"{ARTICLES_TAB_BASE} v{v}"
    requests = []
    for t in (report_tab, articles_tab):
        if t not in tabs:
            requests.append({"addSheet": {"properties": {"title": t}}})
    if requests:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID, body={"requests": requests}
        ).execute()
    return report_tab, articles_tab


HEADER = [
    "Прогон (UTC)",
    "№",
    "URL источника",
    "Тип",
    "Feed URL",
    "HTTP",
    "Попыток",
    "С заголовком",
    "С телом (>200)",
    "С датой",
    "С картинкой",
    "Похожих на новость",
    "Прошли is_article",
    "Прошли тему авто/эконом",
    "Время, мс",
    "Ошибка",
    "Примеры заголовков",
    "Примеры прошедших оба фильтра",
]


# ----- Re-designed 4-block layout (28 columns, 17 visible, 11 hidden) ------
# The canonical 34-column schema lives in ONE place now
# (news_agent.core.articles_schema) — it was hand-copied across ~10 scripts
# and had already drifted (retry carried stale indices). Add new columns AT
# THE END, in the schema module.
from news_agent.core.articles_schema import ARTICLES_HEADER  # noqa: E402

# Portal → country label (visible in the sheet) + numeric code (kept for
# future CMS integration — per the task spec RU=7, UZ=608, KZ=14).
PORTAL_COUNTRY: dict[str, tuple[str, int]] = {
    "RU": ("Russia", 7),
    "UZ": ("Uzbekistan", 608),
    "KZ": ("Kazakhstan", 14),
}

# Lang-tag map + helper are shared with retry_failed_llm via
# news_agent.core.editorial_pass (they had drifted: retry's inline copy had
# 8 languages vs 15 — a recovered Korean title got "(KO)" not "(КОР)").
from news_agent.core.dup_arbiter import (  # noqa: E402
    archive_candidates as arbiter_archive_candidates,
    build_fresh_display as arbiter_fresh_display,
    graykey_candidates as arbiter_graykey_candidates,
    statistics_candidates as arbiter_stat_candidates,
)
from news_agent.core.dedup import (  # noqa: E402
    EVENT_HINT_TRUSTED_DAYS,
    archive_model_hint_is_weak,
    event_hint_is_stale,
)
from news_agent.core.editorial_pass import (  # noqa: E402
    dup_hint_for,
    has_reject_directive,
    lang_tags_for as _shared_lang_tags_for,
    looks_like_usage_limit,
    resolve_section_region,
)


# Thin alias — the implementation is shared (editorial_pass.lang_tags_for).
_lang_tags_for = _shared_lang_tags_for


def _net_retry(fn, *args, what="network-op", tries=5, base_delay=2.0, **kwargs):
    """Run a network operation, retrying transient failures with backoff.

    A multi-hour prog drops connections — laptop sleep, SSL EOF, a 503
    from Sheets. A single such blip on the FINAL Sheets write used to kill
    the whole run and lose the classification (three v48 deaths). The
    wrapped calls (write_report / write_articles / formatting) are
    idempotent overwrites of a tab, so a full re-attempt is safe."""
    last_exc = None
    for attempt in range(1, tries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            name = type(e).__name__
            status = getattr(getattr(e, "resp", None), "status", 0)
            transient = (
                isinstance(e, (ssl.SSLError, socket.error, OSError,
                               ConnectionError, TimeoutError))
                or "SSL" in name or "Timeout" in name
                or status in (429, 500, 502, 503, 504)
            )
            if not transient or attempt == tries:
                raise
            wait = base_delay * (2 ** (attempt - 1))
            print(f"  [{what}] transient {name} (status={status or '-'}): "
                  f"retry {attempt}/{tries - 1} in {wait:.0f}s",
                  file=sys.stderr, flush=True)
            time.sleep(wait)
            last_exc = e
    if last_exc:  # pragma: no cover - loop always returns or raises
        raise last_exc


def write_articles(svc, run_ts: str, rows: list[ArticleRow], tab: str) -> None:  # type: ignore[no-untyped-def]
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=f"'{tab}'"
    ).execute()
    country_label, _country_code = PORTAL_COUNTRY.get(DEDUP_PORTAL, ("Russia", 7))
    out = [ARTICLES_HEADER]
    for r in rows:
        # Source-language tags in the format the editorial team uses in
        # "Новости опубликованные": Latin uppercase on the EN line and a
        # Russian 3-letter abbreviation on the RU line.
        lang_en, lang_ru = _lang_tags_for(r.source_language)
        en_tag = f" ({lang_en})" if lang_en else ""
        ru_tag = f" ({lang_ru})" if lang_ru else ""
        if r.llm_title_en and r.llm_title_ru:
            combined_title = (
                f"EN: {r.llm_title_en[:220]}{en_tag}\n"
                f"RU: {r.llm_title_ru[:220]}{ru_tag}"
            )
        elif r.title:
            combined_title = f"{r.title[:400]}{en_tag}"
        else:
            combined_title = ""
        # Test-drive rows are published with "неактивный" status per spec.
        section_cell = r.llm_section
        if section_cell == "Test-drive":
            section_cell = "Test-drive (неактивный)"
        # is_article heuristic + topic heuristic — compact debug string
        is_article_label = "Да" if r.is_article else "Нет"
        topic_label = "авто/эконом" if r.auto_topic else "не авто"
        combined_reasons = r.article_reasons[:400]
        # Lede — first ~300 chars of body, cleaned of leading/trailing
        # whitespace. Lets the editor skim and judge articles with generic
        # titles like "Соллерс" or "Новости" without opening the URL.
        lede = (r.body_excerpt or "").strip().replace("\n\n", "\n")[:300]
        out.append(
            [
                # Block 1 — «Что за новость»
                run_ts,
                combined_title,
                lede,
                r.article_url,
                section_cell,
                r.llm_region,
                country_label,
                r.published_at,
                r.image_url or "",
                # Block 2 — «Первоисточник»
                r.primary_domain,
                r.primary_url,
                r.primary_confidence,
                # Block 3 — «Для редактора»
                r.llm_note,
                r.llm_confidence,
                r.verdict,
                "",  # manual check column
                "",  # free-form comment
                # Block 4 — «Отладка»
                r.source_url,
                r.source_idx,
                r.article_idx,
                r.body_len,
                is_article_label,
                r.article_score,
                combined_reasons,
                f"{topic_label}: {r.auto_hits[:180]}" if r.auto_hits else topic_label,
                r.llm_relevance,
                r.llm_cost_usd,
                r.primary_method,
                # Phase-1 lifecycle metadata
                ", ".join(r.launch_stages),
                r.launch_brand_model,
                # LLM editorial reason
                r.llm_reason,
                # Hybrid dedup Stage 1: semantic event-signature
                r.event_brand,
                r.event_model,
                r.event_type,
            ]
        )
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{tab}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": clamp_cells(out)},
    ).execute()


def write_report(svc, run_ts: str, results: list[SourceResult], tab: str) -> None:  # type: ignore[no-untyped-def]
    # (re)write the tab from scratch so each run replaces the previous report
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=f"'{tab}'"
    ).execute()
    rows = [HEADER]
    for i, r in enumerate(results, start=1):
        rows.append(
            [
                run_ts,
                i,
                r.url,
                r.detected_type,
                r.feed_url,
                str(r.http_status),
                r.articles_attempted,
                r.articles_with_title,
                r.articles_with_body,
                r.articles_with_date,
                r.articles_with_image,
                r.news_like,
                r.passed_is_article,
                r.passed_auto_topic,
                r.elapsed_ms,
                r.error,
                " | ".join(t[:80] for t in r.sample_titles[:3]),
                " | ".join(t[:80] for t in r.sample_passed[:3]),
            ]
        )
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{tab}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": clamp_cells(rows)},
    ).execute()


# -------------------------------------------------- HTTP + RSS discovery
def make_client():  # -> RetryingHttpClient, but annotated loose for httpx.Client
    """Return the shared browser-like HTTP client with retries + SSL/URL quirks.

    Quirks come from ``config/http_quirks.yaml``: per-domain TLS-insecure
    allowlist + old→new URL rewrites for rotten sheet entries.
    """
    q = load_http_quirks()
    return make_http_client(
        user_agent=USER_AGENT,
        timeout=HTTP_TIMEOUT,
        ssl_insecure_domains=q.ssl_insecure,
        url_rewrites=q.url_rewrites,
        proxy_url=q.proxy_url,
        proxy_direct=q.proxy_direct,
    )


class _PwResponse:
    """Minimal httpx.Response-like wrapper for Playwright-fetched HTML.

    Only implements the surface that ``process_source`` touches: ``.text``,
    ``.content``, ``.status_code``, ``.headers``, ``.raise_for_status()``.
    """

    def __init__(self, url: str, status_code: int, html: str) -> None:
        self.url = url
        self.status_code = status_code
        self.text = html
        self.content = html.encode("utf-8", errors="replace")
        self.headers = {"Content-Type": "text/html; charset=utf-8"}

    def raise_for_status(self) -> None:
        if 400 <= self.status_code < 600:
            raise httpx.HTTPStatusError(
                f"{self.status_code} from Playwright",
                request=None,  # type: ignore[arg-type]
                response=None,  # type: ignore[arg-type]
            )


# Per-host Accept header overrides for the plain-httpx route (see _http_get).
_ACCEPT_OVERRIDES: dict[str, str] = {
    "autocarindia.com": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
}


def _http_get(client, url: str):
    """Route ``url`` through the right backend based on allowlist membership.

    Priority:
      1. Playwright — only when JS rendering is required (SPA / CF challenge)
      2. curl_cffi  — fast TLS-impersonation for Cloudflare/Akamai 403
      3. httpx      — the default for everything else

    Keeps the caller's existing code untouched — returns a duck-typed object
    with the same three attributes ``process_source`` needs.
    """
    if _FETCH_MIN_INTERVAL > 0:
        _wait = _FETCH_MIN_INTERVAL - (time.monotonic() - _LAST_FETCH_T[0])
        if _wait > 0:
            time.sleep(_wait)
        _LAST_FETCH_T[0] = time.monotonic()
    if (
        PW_FETCHER is not None
        and PW_ALLOWLIST is not None
        and PW_ALLOWLIST.matches(url)
    ):
        try:
            status, html = PW_FETCHER.fetch(url)
            return _PwResponse(url, status, html)
        except Exception as e:  # noqa: BLE001
            # Fall through to httpx on PW crash — we'd rather lose the JS
            # rendering than skip the source entirely.
            print(f"   ! Playwright failed on {url}: {type(e).__name__}: {str(e)[:80]}")

    if (
        IMP_FETCHER is not None
        and IMP_ALLOWLIST is not None
        and IMP_ALLOWLIST.matches(url)
    ):
        try:
            # Split-tunnel: foreign hosts via the proxy, .ru direct (same rule
            # as the httpx client). curl (libcurl) does SOCKS natively.
            _proxy = (PROXY_URL if (PROXY_URL and not host_matches(domain_of(url), PROXY_DIRECT))
                      else None)
            status, html = IMP_FETCHER.fetch(url, proxy=_proxy)
            return _PwResponse(url, status, html)
        except Exception as e:  # noqa: BLE001
            # jul-27: one immediate curl_cffi retry before the httpx
            # fallback. On WAF-guarded hosts the httpx fallback is a
            # GUARANTEED 202/403 shell, so a single transient TLS error
            # (SSLError) used to cost the whole source for the run.
            try:
                status, html = IMP_FETCHER.fetch(url, proxy=_proxy)
                return _PwResponse(url, status, html)
            except Exception:  # noqa: BLE001
                print(f"   ! curl_cffi failed on {url}: {type(e).__name__}: {str(e)[:80]}")

    # Per-host Accept override (jul-27: autocarindia's RSS 406s on the httpx
    # default Accept, while its Cloudflare 403s the curl_cffi route — the fix
    # is a plain httpx GET with an explicit feed Accept header).
    host = domain_of(url)
    for _d, _accept in _ACCEPT_OVERRIDES.items():
        if host == _d or host.endswith("." + _d):
            return client.get(url, headers={"Accept": _accept})
    return client.get(url)


def discover_feed(client: httpx.Client, index_url: str, index_html: str) -> str | None:
    # 1. <link rel="alternate" type="application/rss+xml">
    soup = BeautifulSoup(index_html, "lxml")
    for link in soup.find_all("link", rel=lambda v: v and "alternate" in v):
        t = (link.get("type") or "").lower()
        if "rss" in t or "atom" in t or "xml" in t:
            href = link.get("href")
            if href:
                return urljoin(index_url, href)
    # 2. common paths
    base = f"{urlparse(index_url).scheme}://{urlparse(index_url).netloc}"
    for path in ("/rss", "/feed", "/feed.xml", "/rss.xml", "/atom.xml", "/index.xml"):
        candidate = base + path
        try:
            r = client.head(candidate, timeout=5.0)
            if r.status_code == 200 and any(
                t in (r.headers.get("Content-Type", "").lower())
                for t in ("xml", "rss", "atom")
            ):
                return candidate
        except httpx.HTTPError:
            pass
    return None


def looks_like_news(title: str, body: str, published_at, url: str) -> bool:
    if not title.strip():
        return False
    if len(body.strip()) < 200:
        return False
    if published_at is not None:
        return True
    # No date but URL looks like an article slug? Acceptable.
    return _looks_like_article(url)


# ------------------------------------------------------------- core
def _score_recall(article, r: SourceResult, row: ArticleRow) -> bool:  # type: ignore[no-untyped-def]
    """Populate an ArticleRow from a NHTSA recall RawArticle.

    Reuses the published-archive + cache dedup (so a campaign never
    republishes), but DELIBERATELY skips two gates that don't fit recalls:
      * the freshness window — NHTSA lags ~3-5 days, so a recall's
        report_received_date is "old" the moment we first see it; "new"
        is decided by dedup, not by a clock;
      * the heuristic relevance grade — a recall is always a candidate;
        the editorial constitution (Sonnet) makes the real keep/section
        decision (foreign-brand recall → Other/Global; trailer/RV/bus → reject).
    Always returns True (the row is written; verdict says whether it's a
    candidate, an exact dup, or already published)."""
    row.article_url = urldefrag(article.url)[0]
    row.title = article.title
    row.body_len = len(article.body)
    row.body_excerpt = article.body[:1000]
    row.published_at = article.published_at.isoformat() if article.published_at else ""
    row.source_language = "en"
    row.is_article = True
    row.article_score = 1.0
    row.article_reasons = "nhtsa-recall (structured)"
    row.auto_topic = True
    row.auto_hits = "recall"
    # The NHTSA campaign IS the authoritative primary source; when the
    # adapter resolved the official Part-573 recall-report PDF (carried in
    # outbound_links — editor: «нужен сам документ по NHTSA»), the PDF wins
    # as the primary URL. static.nhtsa.gov keeps the nhtsa.gov match for the
    # cluster-priority and feed-dedup NHTSA rules.
    row.primary_url = (article.outbound_links[0]
                       if article.outbound_links else row.article_url)
    row.primary_domain = "nhtsa.gov"
    row.primary_confidence = "high"
    row.primary_method = "nhtsa-official"

    # Within-run final-URL dedup.
    canon = canonicalise(article.url)
    if canon in SEEN_FINAL_URLS:
        row.verdict = "Отклонить (дубль финального URL)"
        row.article_reasons = "duplicate-final-url"
        return True
    SEEN_FINAL_URLS.add(canon)

    # Already in the editor's published archive? (campaign URL or recent title)
    pub_reason = already_published(
        article.url, article.title, PUBLISHED_URLS, PUBLISHED_TITLES)
    if pub_reason:
        row.verdict = "Отклонить (уже опубликовано редактором)"
        row.article_reasons = f"published-archive-{pub_reason}"
        return True

    # Classified on a prior run at the current rules? Restore — no re-LLM,
    # no re-push (build_news_sheet also dedups by URL as a backstop).
    uh = url_hash(canon)
    if uh in PREVIOUSLY_SEEN:
        cached = PREVIOUSLY_SEEN[uh]
        version_ok = cached.get("cls_ver", "") == CLASSIFIER_VERSION
        if cache_is_authoritative(
            cached.get("verdict", ""), bool(cached.get("llm_section")), version_ok,
            prompt_ok=(cached.get("prompt_ver") == PROMPT_VERSION
                       if cached.get("prompt_ver") else None),
            heuristics_ok=(cached.get("heur_ver") == HEURISTICS_VERSION
                           if cached.get("heur_ver") else None),
            prompt_change=PROMPT_CHANGE,
        ):
            row.verdict = cached.get("verdict", "") or "Возможно новость"
            row.llm_relevance = cached.get("llm_relevance", "")
            row.llm_section = cached.get("llm_section", "")
            row.llm_region = cached.get("llm_region", "")
            row.llm_confidence = cached.get("llm_confidence", "")
            row.llm_title_en = cached.get("llm_title_en", "")
            row.llm_title_ru = cached.get("llm_title_ru", "")
            row.llm_reason = cached.get("llm_reason", "")
            row.event_brand = cached.get("event_brand", "")
            row.event_model = cached.get("event_model", "")
            row.event_type = cached.get("event_type", "")
            row.llm_cost_usd = 0
            row.llm_note = "из кэша"
            row.from_cache = True
            return True

    # Fresh campaign → force into the editorial LLM pass.
    row.verdict = "Возможно новость"
    return True


def _process_nhtsa_recalls(  # type: ignore[no-untyped-def]
    client: httpx.Client,
    url: str,
    source_idx: int,
    article_rows: list[ArticleRow],
    t0: float,
) -> SourceResult:
    """Structured source: pull recent US recall campaigns from the NHTSA ODI
    dataset and inject each as a candidate ArticleRow (see _score_recall)."""
    r = SourceResult(url=url)
    r.detected_type = "nhtsa"
    r.feed_url = url
    try:
        articles = nhtsa_recalls.fetch_recent_recalls(
            client,
            lookback_days=NHTSA_RECALL_LOOKBACK_DAYS,
            limit=NHTSA_RECALL_MAX_ITEMS,
            now=(RUN_WINDOW.now if RUN_WINDOW is not None else None),
        )
    except Exception as e:  # noqa: BLE001 — a source failure must not crash the run
        r.error = f"{type(e).__name__}: {e}"[:200]
        r.elapsed_ms = int((time.monotonic() - t0) * 1000)
        return r
    r.articles_attempted = len(articles)
    for idx, article in enumerate(articles, start=1):
        row = ArticleRow(
            source_idx=source_idx,
            source_url=url,
            article_idx=idx,
            article_url=article.url,
        )
        if _score_recall(article, r, row):
            article_rows.append(row)
    r.elapsed_ms = int((time.monotonic() - t0) * 1000)
    return r


def process_source(
    client: httpx.Client, url: str, source_idx: int, article_rows: list[ArticleRow]
) -> SourceResult:
    r = SourceResult(url=url)
    t0 = time.monotonic()

    # NHTSA recalls branch (structured Socrata JSON) — pull recall campaigns
    # directly, no HTML fetch. Handled first because it owns its own freshness
    # (dedup-based, not the 24-48h window — NHTSA lags ~3-5 days).
    if nhtsa_recalls.is_recalls_source(url):
        return _process_nhtsa_recalls(client, url, source_idx, article_rows, t0)

    # Telegram branch (t.me / telegram.me) — use the public web preview.
    if is_telegram_url(url):
        preview = to_channel_preview_url(url)
        if not preview:
            r.detected_type = "telegram"
            r.error = "invalid or private channel URL"
            r.elapsed_ms = int((time.monotonic() - t0) * 1000)
            return r
        try:
            resp = _http_get(client, preview)
            r.http_status = resp.status_code
            resp.raise_for_status()
        except Exception as e:  # noqa: BLE001
            r.detected_type = "telegram"
            r.error = f"{type(e).__name__}: {e}"[:200]
            r.elapsed_ms = int((time.monotonic() - t0) * 1000)
            return r
        posts = parse_channel_html(
            html=resp.text,
            channel_preview_url=preview,
            source_name="",
            source_url=url,
            source_language=None,
            max_items=_items_cap_for(url),
        )
        r.detected_type = "telegram"
        r.feed_url = preview
        r.articles_attempted = len(posts)
        for idx, art in enumerate(posts, start=1):
            row = ArticleRow(
                source_idx=source_idx,
                source_url=url,
                article_idx=idx,
                article_url=art.url,
            )
            if _score_article(art, r, row):
                article_rows.append(row)
        r.elapsed_ms = int((time.monotonic() - t0) * 1000)
        return r

    try:
        resp = _http_get(client, url)
        r.http_status = resp.status_code
        resp.raise_for_status()
    except Exception as e:  # noqa: BLE001
        r.error = f"{type(e).__name__}: {e}"[:200]
        r.elapsed_ms = int((time.monotonic() - t0) * 1000)
        return r

    html = resp.text
    content_type = resp.headers.get("Content-Type", "").lower()

    # Check whether the page is itself an RSS/Atom feed. The BODY is the
    # authoritative signal: some servers (e.g. tass.ru/rss/v2.xml) serve a
    # valid feed with Content-Type: text/html, so AND-gating detection on an
    # xml content-type silently dropped the whole feed -> 0 links. Detect on
    # body-OR-ctype, then commit to the RSS branch ONLY if feedparser actually
    # yields entries — so a stray "<rss" inside an HTML page harmlessly falls
    # through to HTML mode instead of being trapped as an empty feed.
    head_lower = html[:1000].lower()
    looks_like_feed = (
        any(t in content_type for t in ("rss", "atom", "xml"))
        or "<rss" in head_lower or "<feed" in head_lower
    )
    if looks_like_feed:
        cap = _items_cap_for(url)
        entries = feedparser.parse(html).entries[:cap]
        if entries:
            r.detected_type = "rss"
            r.feed_url = url
            r.articles_attempted = len(entries)
            _fill_from_rss_entries(client, entries, r, source_idx, article_rows,
                                   t0=t0)
            r.elapsed_ms = int((time.monotonic() - t0) * 1000)
            return r

    feed_url = discover_feed(client, url, html)
    if feed_url:
        try:
            rss_resp = client.get(feed_url)
            rss_resp.raise_for_status()
            cap = _items_cap_for(url)
            entries = feedparser.parse(rss_resp.content).entries[:cap]
            if entries:
                r.detected_type = "rss"
                r.feed_url = feed_url
                r.articles_attempted = len(entries)
                _fill_from_rss_entries(client, entries, r, source_idx,
                                       article_rows, t0=t0)
                r.elapsed_ms = int((time.monotonic() - t0) * 1000)
                return r
        except httpx.HTTPError:
            pass  # fall through to HTML mode

    # HTML index mode
    r.detected_type = "html"
    links = _discover_article_links(url, html, _items_cap_for(url))
    if not links:
        # Anti-bot SHELL VARIANCE (jul-10 root cause of silent 0-yield runs):
        # some sources intermittently serve a JS-shell instead of the real
        # server-rendered page — same URL, same 200, no article links.
        # Measured on auto.ru/mag/theme/news: 1 of 6 fetches returns a 13KB
        # shell, the rest the 77KB real page; 13 sources flip-flopped between
        # the last two runs this way (auto.ru x2, iz.ru, lada.ru, kia.ru,
        # cadillac/chevrolet/renault media, minfin, banki.ru). The 10.07 run
        # lost BOTH auto.ru sources to it. One immediate refetch recovers
        # ~5/6 of these at the cost of a single extra GET on 0-link sources.
        # An IMMEDIATE refetch often hits the same CDN edge and gets the shell
        # again (verified live) — a short pause before each retry lets the
        # edge rotate. 2 attempts x 2s: worst case +4s per genuinely-empty
        # source, well inside the 90s source budget.
        for _attempt in (1, 2):
            time.sleep(2.0)
            try:
                resp_retry = _http_get(client, url)
                resp_retry.raise_for_status()
                links = _discover_article_links(
                    url, resp_retry.text, _items_cap_for(url))
            except Exception:  # noqa: BLE001
                continue
            if links:
                note = f"shell-retry#{_attempt} recovered {len(links)} links"
                r.error = (r.error + " | " + note)[:200] if r.error else note
                break
    r.articles_attempted = len(links)
    for idx, link in enumerate(links, start=1):
        if _source_budget_exceeded(r, t0, idx - 1, len(links)):
            break
        _fetch_and_score(client, link, r, source_idx, idx, article_rows)
    r.elapsed_ms = int((time.monotonic() - t0) * 1000)
    return r


def _source_budget_exceeded(r: SourceResult, t0: float, done: int, total: int) -> bool:
    """True once the per-source wall clock is spent — record the truncation
    in r.error so the report shows WHICH sources are slow (they used to hide
    behind a silently long run)."""
    if SOURCE_BUDGET_S <= 0:
        return False
    elapsed = time.monotonic() - t0
    if elapsed <= SOURCE_BUDGET_S:
        return False
    note = f"budget-exceeded {elapsed:.0f}s at {done}/{total} links"
    r.error = (r.error + " | " + note)[:200] if r.error else note
    return True


def _rss_pub_date(entry) -> datetime | None:  # type: ignore[no-untyped-def]
    """Pull authoritative pubDate from an RSS/Atom feedparser entry.

    Tries ``published_parsed`` (struct_time → most reliable, normalised by
    feedparser) then falls back to ``published``/``pubDate`` as RFC 2822
    strings. Returns None when no usable date is present.
    """
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed:
        try:
            return datetime(*parsed[:6], tzinfo=timezone.utc)
        except Exception:  # noqa: BLE001
            pass
    raw = (entry.get("published") or entry.get("pubDate")
           or entry.get("updated") or "").strip()
    if not raw:
        return None
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:  # noqa: BLE001
        return None


def _rss_entry_body(entry) -> str:  # type: ignore[no-untyped-def]
    """Best body text from an RSS entry (content:encoded > summary/description),
    HTML-stripped. The fallback body when a JS-rendered article PAGE extracts
    empty (jun-19 coverage fix — recovers brand newsrooms / press feeds)."""
    import re
    raw = ""
    cont = entry.get("content")
    if isinstance(cont, list) and cont:
        raw = cont[0].get("value", "") or ""
    if not raw:
        raw = entry.get("summary", "") or entry.get("description", "") or ""
    text = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", text).strip()


def _fill_from_rss_entries(  # type: ignore[no-untyped-def]
    client: httpx.Client,
    entries,
    result: SourceResult,
    source_idx: int,
    article_rows: list[ArticleRow],
    *,
    t0: float | None = None,
) -> None:
    for idx, entry in enumerate(entries, start=1):
        if t0 is not None and _source_budget_exceeded(
                result, t0, idx - 1, len(entries)):
            break
        link = (entry.get("link") or "").strip()
        if not link:
            continue
        # The RSS pubDate is the publisher's authoritative timestamp.
        # When we hand it through, extract_article() will trust it over
        # body-text date heuristics that can misfire on sites where the
        # article references past events (e.g. abreview.ru). Without this
        # the freshness gate silently drops fresh items as «stale».
        rss_pub = _rss_pub_date(entry)
        _fetch_and_score(
            client, link, result, source_idx, idx, article_rows,
            rss_pub_date=rss_pub,
            rss_title=(entry.get("title") or "").strip(),
            rss_body=_rss_entry_body(entry),
        )


def _run_llm_pass(article_rows: list[ArticleRow], *, use_legacy: bool = False) -> dict:
    """For every certain/possible row: run LLM editorial review + translate.

    By default uses the consolidated `editorial_review` call (replaces
    is_automotive + classify_section in one). Pass ``use_legacy=True`` to
    fall back to the old 2-call sequence.

    Stops early on budget exhaustion (raises BudgetExceeded) or on a
    persistent editorial_review failure (circuit breaker — see the abort
    block). Returns ``{"aborted": reason_or_empty}`` so main() can fail the
    run loudly instead of pushing a silently truncated feed (incident I2)."""
    settings = get_settings()
    client: LLMClient = make_llm_client(settings)
    # The editorial DECISION (publish? section?) is the precision bottleneck and
    # benefits from a stronger model; translation does not. Route ONLY
    # editorial_review through EDITORIAL_MODEL when set (e.g. claude-sonnet-4-6),
    # keeping translate/relevance on the cheaper default. Unset -> one model for
    # all. (jun-29: Sonnet +7pp vs Haiku on the editor-labeled set, almost all on
    # section accuracy; ~3x/call but cache caps it, ~$2.7/run editorial-only.)
    editorial_client: LLMClient = client
    _ed_model = os.environ.get("EDITORIAL_MODEL", "").strip()
    if _ed_model and _ed_model != getattr(client, "model", ""):
        editorial_client = make_llm_client(settings)
        editorial_client.model = _ed_model
    sections = load_sections()
    budget = BudgetTracker(cap_usd=LLM_BUDGET_USD)

    # Only truly fresh rows go through the LLM. Cached rows already have
    # their classification fields restored from SQLite.
    candidates = [
        r for r in article_rows
        if r.verdict in {"Точно новость", "Возможно новость"} and not r.from_cache
    ]
    cached_count = sum(
        1 for r in article_rows
        if r.from_cache and r.verdict in {"Точно новость", "Возможно новость"}
    )
    if not candidates:
        print(
            f"LLM pass: кандидатов для LLM нет "
            f"(из кэша: {cached_count} строк — LLM не вызван)."
        )
        return {"aborted": ""}

    print(
        f"\nLLM pass: {len(candidates)} свежих кандидатов "
        f"(из кэша без LLM: {cached_count}, "
        f"certain={sum(1 for r in candidates if r.verdict == 'Точно новость')}, "
        f"possible={sum(1 for r in candidates if r.verdict == 'Возможно новость')})"
    )
    mode = "legacy (relevance + classify)" if use_legacy else "editorial_review (consolidated)"
    print(f"  provider: {client.provider_name}  model: {client.model}  mode: {mode}  cap: ${LLM_BUDGET_USD}")
    if editorial_client is not client:
        print(f"  editorial_review model: {editorial_client.model} (translate/relevance stay {client.model})")

    country = "Russia"  # portal country for now; будет параметром позже
    section_names = {s.name for s in sections}

    # Plan P3-D (advisory): load the recently-classified brand+model map
    # once. Read-only; failure must NOT abort the LLM pass (the hint is
    # purely cosmetic for the editor's reason column).
    recent_bm: dict[str, tuple[str, str]] = {}
    try:
        if DEDUP_STORE is not None:
            recent_bm = DEDUP_STORE.recent_brand_models(DEDUP_PORTAL, days=30)
            if recent_bm:
                print(f"  P3-D: {len(recent_bm)} models seen in last 30d "
                      f"(advisory dup hints enabled)")
    except Exception as _e:  # noqa: BLE001
        print(f"  P3-D: recent-model lookup skipped ({type(_e).__name__})")
        recent_bm = {}

    # Hybrid Stage 2a (advisory): semantic event-key map from last 30d.
    # Stronger than the lexical brand_model — catches divergent-headline
    # / cross-language repeats. Same read-only, never-abort contract.
    recent_ev: dict[str, tuple[str, str, str]] = {}
    try:
        if DEDUP_STORE is not None:
            recent_ev = DEDUP_STORE.recent_event_keys(DEDUP_PORTAL, days=30)
            if recent_ev:
                print(f"  Stage2a: {len(recent_ev)} event-keys seen in "
                      f"last 30d (semantic dup hints enabled)")
    except Exception as _e:  # noqa: BLE001
        print(f"  Stage2a: event-key lookup skipped ({type(_e).__name__})")
        recent_ev = {}

    # Tier 0.5 (jul-20 dup-wave): fuzzy paraphrase vs OUR OWN pushes of the
    # last 30d. A story we fed on the 17th resurfaced from another outlet on
    # the 19th and NO layer could see it (URL differs, wording differs, and
    # pre-fix history rows carry no event keys). Same never-abort contract.
    own_titles: set[str] = set()
    try:
        if DEDUP_STORE is not None:
            own_titles = DEDUP_STORE.recent_pushed_titles(DEDUP_PORTAL, days=30)
            if own_titles:
                print(f"  Tier0.5: {len(own_titles)} own pushed titles "
                      f"(30d anti-repeat enabled)")
    except Exception as _e:  # noqa: BLE001
        print(f"  Tier0.5: own-titles lookup skipped ({type(_e).__name__})")
        own_titles = set()

    # Arbiter candidate pool (jul-23): archive + our own pushes in ONE set —
    # the event-key store drops empty-model keys at write time, so a prior
    # Honda–GAC push is reachable only through its pushed TITLE. Computed
    # once per pass, not per row.
    arb_pub_titles: set[str] = (
        (PUBLISHED_ALL_TITLES or set()) | own_titles) if DUP_ARBITER_ON else set()

    _stale_dup_hints = 0  # hints older than a week -> handed to the arbiter
    _weak_archive_hints = 0  # archive brand+model tier -> handed to the arbiter
    consec_errors = 0   # circuit breaker: N in a row → persistent failure
    aborted = ""        # non-empty = why the pass stopped early
    for i, r in enumerate(candidates, start=1):
        # ============================================================
        # Stage 1+2: editorial review (NEW path — consolidated call)
        # OR legacy is_automotive + classify_section (old path)
        # ============================================================
        if not use_legacy:
            # NEW PATH: one consolidated LLM call returning publish/skip
            # + section + region + confidence + reason.
            try:
                review, u = editorial_client.editorial_review(
                    title=r.title,
                    body=r.body_excerpt or r.title,
                    sections=sections,
                    portal_country=country,
                )
                budget.record(u)
                r.llm_cost_usd = round((r.llm_cost_usd or 0) + u.cost_usd, 5)
            except Exception as e:  # noqa: BLE001
                r.llm_note = f"editorial_review error: {str(e)[:200]}"
                consec_errors += 1
                # Persistent failures must ABORT, not silently burn the rest of
                # the candidates (the I2 incident: a usage-limit 400 truncated
                # the tail ~15% of every run, invisible outside per-row notes).
                # Two triggers:
                #   • fast-path: the known usage-limit wording;
                #   • generic: N consecutive failures of ANY wording — catches
                #     org-disabled 403 (the v16 broken run), credit-exhausted,
                #     404 from a typo'd EDITORIAL_MODEL, sustained 429 storms,
                #     and any future rewording of the limit message.
                # Untouched rows stay "Возможно новость" and re-run next time
                # (the cache is not authoritative for them).
                _limit_hit = looks_like_usage_limit(str(e))
                if _limit_hit or consec_errors >= CONSEC_LLM_ERRORS_ABORT:
                    aborted = (
                        "usage limit" if _limit_hit
                        else f"{consec_errors} consecutive editorial_review errors "
                             f"(last: {str(e)[:120]})"
                    )
                    print(
                        f"\n!!! LLM pass ABORTED at candidate {i}/{len(candidates)}"
                        f" — {aborted}. {len(candidates) - i} candidates left "
                        f"unclassified; they will re-run next time. "
                        f"(usage limit → raise it in Anthropic Console → Billing.)",
                        file=sys.stderr,
                    )
                    break
                continue
            consec_errors = 0  # a successful call resets the breaker

            r.llm_relevance = "Да" if review.should_publish else "Нет"
            # Hybrid dedup Stage 1: capture the semantic event-signature
            # (may be None on a model that omitted it — degrade silently).
            es = getattr(review, "event_signature", None)
            if es is not None:
                r.event_brand = (es.brand or "").strip().lower()[:40]
                r.event_model = (es.model or "").strip().lower()[:60]
                r.event_type = (es.event_type or "").strip().lower()[:24]
            # Always capture the editor-facing reason in a dedicated field;
            # this populates the new "Обоснование LLM" sheet column so the
            # editor can see exactly why the bot accepted or rejected.
            r.llm_reason = review.reason[:300] if review.reason else ""

            # Consistency guard (jun-18): the model occasionally returns
            # should_publish=True while its OWN reason concludes the item must be
            # rejected (e.g. "...без конкретной модели/рынка РФ — отклонить" got
            # mislabelled "Test-drive" and slipped into the live feed — row 56 of
            # the jun-18 push). Trust the explicit reject directive over the
            # boolean and route it to the rejected tab (auditable), not the feed.
            if review.should_publish and has_reject_directive(review.reason):
                r.llm_relevance = "Нет"
                r.verdict = "Отклонено LLM"
                r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                    f"противоречие: publish=Да, reason='отклонить' → отклонено ({review.reason[:80]})"
                print(
                    f"  [{i}/{len(candidates)}] CONTRADICTION publish=True/reason=reject "
                    f"→ REJECT  |  {r.title[:50]}"
                )
                continue

            if not review.should_publish:
                # RU transport-civic rescue (jun-2026 "Опубликованные 3"
                # recall audit). The LLM rejects Russian transport-CIVIC
                # news (traffic law, roads, taxi/carsharing, ОСАГО,
                # утильсбор, car surveys) as non-product noise — but the
                # editor publishes ~40% of Local specifics from exactly
                # this category. Force-accept the genuine category to
                # Local specifics; the editor still reviews Local and can
                # drop edge cases. Gated against military/banking noise
                # inside is_ru_transport_civic.
                if False and is_ru_transport_civic(r.title, r.body_excerpt):  # noqa: SIM223
                    # RESCUE DISABLED (jun-19). The editorial constitution now
                    # routes genuine RU-transport itself (regulation / carsharing
                    # / loan surveys -> Local); the rescue was force-adding
                    # OFF-topic Local junk the LLM correctly rejected (PMD-social,
                    # taxi-tyres, parking-trivia, driving-tips). Trust the LLM.
                    r.llm_relevance = "Да"
                    r.llm_section = "Local specifics"
                    r.llm_region = "Local"
                    r.llm_confidence = 0.55
                    r.llm_note = (
                        (r.llm_note + " | " if r.llm_note else "")
                        + "RU-транспорт-civic rescue (LLM отклонил: "
                        + f"{review.reason[:60]}) — проверьте"
                    )
                    r.verdict = "Точно новость"
                    print(
                        f"  [{i}/{len(candidates)}] RESCUE→Local  "
                        f"(LLM rejected)  |  {r.title[:50]}"
                    )
                    # fall through to the accept path below
                else:
                    # LLM rejected. Mark verdict + reason for editor.
                    r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                        f"LLM: {review.reason[:120]}"
                    r.verdict = "Отклонено LLM"
                    print(
                        f"  [{i}/{len(candidates)}] REJECT  conf={review.confidence:.2f}  "
                        f"{review.reason[:60]}  |  {r.title[:50]}"
                    )
                    continue

            # Accepted — final section/region via the SHARED resolver
            # (heuristic pre-classifier override + Other-news fallback;
            # single source of truth with retry_failed_llm).
            dec = resolve_section_region(
                review_section=review.section,
                review_region=review.region,
                review_confidence=review.confidence,
                title=r.title,
                body_excerpt=r.body_excerpt,
                domain=domain_of(r.article_url),
                section_names=section_names,
            )
            r.llm_section = dec.section
            r.llm_region = dec.region
            r.llm_confidence = dec.confidence
            if dec.heuristic_note:
                r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                    dec.heuristic_note
            elif review.reason:
                r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                    f"reason: {review.reason[:100]}"

            if r.llm_section == "Test-drive":
                r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                    "требует ручной проверки — Test-drive"
        else:
            # LEGACY PATH: 2 separate LLM calls (relevance + classify)
            try:
                rel, u = client.is_automotive(r.title, r.body_excerpt or r.title)
                budget.record(u)
                r.llm_relevance = "Да" if rel.is_automotive_or_economy else "Нет"
                r.llm_cost_usd = round((r.llm_cost_usd or 0) + u.cost_usd, 5)
            except Exception as e:  # noqa: BLE001
                r.llm_note = f"relevance error: {str(e)[:200]}"
                continue
            if not rel.is_automotive_or_economy:
                r.llm_note = (r.llm_note + " | " if r.llm_note else "") + "LLM: не авто/эконом"
                r.verdict = "Отклонено LLM"
                print(f"  [{i}/{len(candidates)}] {r.title[:60]!r} → relevance=Нет (отсечено)")
                continue

            pre = heuristic_section(
                title=r.title,
                body_excerpt=r.body_excerpt,
                domain=domain_of(r.article_url),
            )
            if pre is not None:
                r.llm_section = pre.section if pre.section in section_names else "Other news"
                r.llm_region = pre.region
                r.llm_confidence = round(pre.confidence, 2)
                r.llm_note = (
                    (r.llm_note + " | " if r.llm_note else "")
                    + f"эвристика: {pre.reason}"
                )
                if r.llm_section == "Test-drive":
                    r.llm_note += " | требует ручной проверки — Test-drive"
            else:
                try:
                    cls, u = client.classify_section(
                        title=r.title,
                        body=r.body_excerpt or r.title,
                        sections=sections,
                        few_shots=[],
                        portal_country=country,
                    )
                    budget.record(u)
                    r.llm_section = cls.section if cls.section in section_names else "Other news"
                    r.llm_region = cls.region
                    r.llm_confidence = round(cls.confidence, 2)
                    r.llm_cost_usd = round((r.llm_cost_usd or 0) + u.cost_usd, 5)
                    if r.llm_section == "Test-drive":
                        r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                            "требует ручной проверки — Test-drive"
                except Exception as e:  # noqa: BLE001
                    r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                        f"classify error: {str(e)[:200]}"
                    continue

        # 3. Translate title
        try:
            tp, u = client.translate_title(title=r.title, source_language_hint=None)
            budget.record(u)
            r.llm_title_en = tp.english
            r.llm_title_ru = tp.russian
            # LLM's language detection is more reliable than trafilatura's
            # HTML-lang guess, so prefer it when available.
            if tp.source_language:
                r.source_language = tp.source_language.lower()[:2]
            r.llm_cost_usd = round((r.llm_cost_usd or 0) + u.cost_usd, 5)
        except Exception as e:  # noqa: BLE001
            r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                f"translate error: {str(e)[:200]}"

        # 3.5 Advisory dup hint — SHARED 3-tier helper (archive paraphrase →
        # semantic event-key → lexical brand_model); at most one hint.
        # Moved AFTER translate (jul-23, Geely-A7 «писали давно» forensics):
        # the all-time archive is EN-heavy and an RU original transliterates
        # to ~50 fuzz vs its own EN twin, so the EN title must participate.
        # PREPENDED so the [:300] reason cap can never amputate it (the
        # push diverts on the "возможно дуб" substring).
        dup_hint = None
        if not use_legacy:
            dup_hint = dup_hint_for(
                title=r.title,
                event_brand=r.event_brand,
                event_model=r.event_model,
                event_type=r.event_type,
                launch_brand_model=r.launch_brand_model,
                canon_url=canonicalise(r.article_url),
                pub_titles=PUBLISHED_ALL_TITLES,
                recent_ev=recent_ev,
                recent_bm=recent_bm,
                own_titles=own_titles,
                alt_title=r.llm_title_en,
            )
            # jul-27 (editor: «много отклоняем того, что нужно»): a hint whose
            # match is older than a week diverts a genuinely-new story 25-33%
            # of the time (measured on 1058 editor answers). Don't divert on
            # it — drop the hint and let the LLM arbiter below read the
            # candidates and decide. Fresh hints (<=7d, 5-12% error) keep
            # diverting deterministically for $0.
            if dup_hint and event_hint_is_stale(dup_hint):
                _stale_dup_hints += 1
                dup_hint = None
            # jul-28: the all-time archive's brand+model tier knows nothing
            # about the EVENT — 24% of its diverts were a NEW happening for
            # a model we had covered (Urus recall after the Urus reveal,
            # X5 LWB India after X5 LWB China). Hand it to the arbiter too.
            elif dup_hint and archive_model_hint_is_weak(dup_hint):
                _weak_archive_hints += 1
                dup_hint = None
            if dup_hint:
                r.llm_reason = (
                    dup_hint + (" | " + r.llm_reason if r.llm_reason else "")
                )[:300]

        # 3.6 LLM dup arbiter (LLM_DUP_ARBITER=1; advisory). Deterministic
        # layers answered «не доказано» — collect their near-misses (all-time
        # archive + 30d event-key gray zone) and let a cheap call read and
        # decide. A match only prepends «возможно дубль» → review-tab divert;
        # never a hard reject, never aborts the pass.
        if (DUP_ARBITER_ON and not dup_hint and not use_legacy
                and hasattr(client, "same_published_event")):
            try:
                _arb_cands = arbiter_archive_candidates(
                    title=r.title, alt_title=r.llm_title_en,
                    event_brand=r.event_brand, event_model=r.event_model,
                    pub_titles=arb_pub_titles,
                ) + arbiter_stat_candidates(
                    title=r.title, alt_title=r.llm_title_en,
                    event_type=r.event_type, event_brand=r.event_brand,
                    pub_titles=arb_pub_titles,
                ) + arbiter_graykey_candidates(
                    event_brand=r.event_brand, event_model=r.event_model,
                    event_type=r.event_type, recent=recent_ev or {},
                    current_url=canonicalise(r.article_url),
                )
                _arb_cands = _arb_cands[:6]
                if _arb_cands:
                    _idx, u = client.same_published_event(
                        fresh=arbiter_fresh_display(
                            title=r.title, alt_title=r.llm_title_en,
                            event_brand=r.event_brand,
                            event_model=r.event_model,
                            event_type=r.event_type),
                        candidates=_arb_cands,
                    )
                    budget.record(u)
                    r.llm_cost_usd = round(
                        (r.llm_cost_usd or 0) + u.cost_usd, 5)
                    if _idx:
                        _m = _arb_cands[_idx - 1].splitlines()[0][:80]
                        r.llm_reason = (
                            f"(возможно дубль, ИИ-арбитр: похоже на "
                            f"«{_m}» — проверьте)"
                            + (" | " + r.llm_reason if r.llm_reason else "")
                        )[:300]
                        print(f"  [{i}/{len(candidates)}] ARB-DUP  "
                              f"{r.title[:45]!r} ≈ «{_m[:45]}»")
            except Exception as e:  # noqa: BLE001 — advisory, never fatal
                r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                    f"dup-arbiter error: {str(e)[:120]}"

        # 4. Primary-source arbitration (contested link-soup only). This row is
        # accepted for publication and the heuristic flagged ≥2 plausible
        # first-sources it couldn't choose between (redistribution-host repost,
        # e.g. autonews linking BOTH the brand site AND a carscoops related
        # piece). Ask the LLM which link is THIS story's true origin. Rejected
        # rows never reach here (they `continue` above), so the call fires only
        # for the handful of contested published rows. Never aborts the pass; on
        # any error the deterministic pick stands. The resolved primary_url is
        # cached with the row, so a repeat run reuses it without re-calling.
        if r.primary_candidates and hasattr(client, "pick_primary_source"):
            try:
                chosen, u = client.pick_primary_source(
                    title=r.title,
                    body_excerpt=r.body_excerpt or r.title,
                    candidates=r.primary_candidates,
                )
                budget.record(u)
                r.llm_cost_usd = round((r.llm_cost_usd or 0) + u.cost_usd, 5)
                if chosen and chosen != r.primary_url:
                    prev_dom = r.primary_domain or domain_of(r.primary_url)
                    r.primary_url = chosen
                    r.primary_domain = domain_of(chosen)
                    r.primary_confidence = "high"
                    r.primary_method = "llm-pick"
                    r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                        f"первоисточник (LLM): {r.primary_domain} (было {prev_dom})"
                    print(
                        f"  [{i}/{len(candidates)}] PRIMARY→{r.primary_domain} "
                        f"(было {prev_dom})  |  {r.title[:45]}"
                    )
            except Exception as e:  # noqa: BLE001
                r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
                    f"primary-pick error: {str(e)[:120]}"

        # Row passed both relevance + classification + translation. If it
        # came in as "Возможно новость" (yellow), promote it to "Точно
        # новость" (green) so the editor's colour view matches the LLM's
        # verdict. "Точно новость" rows stay as they are.
        if r.verdict == "Возможно новость":
            r.verdict = "Точно новость"

        print(
            f"  [{i}/{len(candidates)}]  {r.llm_section:22}  "
            f"{r.llm_region:6}  conf={r.llm_confidence}  "
            f"spent=${budget.spent_usd:.3f}  |  {r.title[:60]}"
        )

    snap = budget.snapshot()
    print(f"\nLLM pass done: {snap['calls']} calls, "
          f"${snap['spent_usd']} / ${snap['cap_usd']}  "
          f"({snap['input_tokens']} in / {snap['output_tokens']} out)")
    if _stale_dup_hints:
        print(f"  dup hints older than {EVENT_HINT_TRUSTED_DAYS}d not auto-diverted: "
              f"{_stale_dup_hints} (handed to the LLM arbiter instead)")
    if _weak_archive_hints:
        print(f"  archive brand+model hints not auto-diverted: "
              f"{_weak_archive_hints} (handed to the LLM arbiter instead)")
    return {"aborted": aborted}


def _apply_brand_newsroom_primary(article_rows: list[ArticleRow]) -> None:
    """Attribute weak-primary brand SALES/FINANCIAL rows to the brand's
    OFFICIAL newsroom (config/brand_newsrooms.yaml).

    The editor requires brand figures to carry the brand's own press as the
    primary («все финансовые отчёты компаний нужно брать с их сайтов» — Olga,
    03.07, on a Nio-deliveries story cited from cnevpost). Runs AFTER the
    corpus pass.

    Override policy is PER EVENT TYPE:
      • sales_stat / financial — override EVEN a high-confidence journalistic
        primary (cnevpost, carnewschina…): the editor wants the company's OWN
        site, an aggregator is only a fallback «когда с ВПН уже сайт не
        работает».
      • reveal / partnership / facelift / tech (jul-17: «ок, но нужен пресс»
        on an Xpeng partnership, «пресс есть» on a DS reveal; jul-21: «нужен
        пресс» ×3 — Maybach GLS facelift, VW autonomous shuttles, Bentley
        sound tech) — override ONLY a WEAK primary: confidence low
        (self-fallback, no source found) or a redistribution host standing as
        the source. Measured on cache history before shipping: blanket
        reveal/partnership would flip 277 rows incl. carscoops/motor1 deep
        links (P2-A says keep those); the weak-gate flips 152
        (reveal/partnership) + 112 (facelift/tech), all of the
        naavtotrasse/kolesa/auto.mail-as-primary class, while 75 deep
        journalistic facelift/tech primaries stay. A story ON a preferred
        journalistic site (cnevpost self@high) is deliberately NOT overridden
        — editor comments so far don't outweigh the P2-A doctrine (маятник).
    We never override when the primary is already on an official brand-owned
    domain (a specific press release beats the generic newsroom index)."""
    try:
        from news_agent.core.config_loader import load_brand_newsrooms
        from news_agent.core.primary_source import (
            _REDISTRIBUTION_HOSTS, _matches_brand, _normalise_domain)
        newsrooms = load_brand_newsrooms()
        brands = BRANDS or []
    except Exception as e:  # noqa: BLE001 — attribution is best-effort
        print(f"  brand-newsrooms load failed ({type(e).__name__}) — pass skipped")
        return
    if not newsrooms:
        return

    def _is_redis(dom: str) -> bool:
        n = _normalise_domain(dom or "")
        return n in _REDISTRIBUTION_HOSTS or any(
            n.endswith("." + h) for h in _REDISTRIBUTION_HOSTS)

    rooted = 0   # rows annotated with their brand's newsroom index
    for r in article_rows:
        if r.verdict not in ("Точно новость", "Возможно новость"):
            continue
        et = r.event_type
        if et not in ("sales_stat", "financial", "reveal", "partnership",
                      "facelift", "tech"):
            continue
        nr = newsrooms.get((r.event_brand or "").strip().lower())
        if not nr:
            continue
        nr_dom = domain_of(nr)
        # Already on an official brand domain (real press release or the
        # newsroom itself) → keep it, it's already the company's own site.
        if r.primary_domain == nr_dom or _matches_brand(r.primary_domain, brands):
            continue
        if et in ("reveal", "partnership", "facelift", "tech"):
            weak = (r.primary_confidence == "low") or _is_redis(r.primary_domain)
            if not weak:
                continue  # P2-A: keep the deep journalistic primary
        # jul-28 (editor on a Lynk & Co row: «это не ссылка на новость
        # компании, там такой нет»). EVERY entry in brand_newsrooms.yaml is
        # a newsroom INDEX, not a release: 21 of 34 are bare roots and the
        # other 13 are index paths (ir.nio.com/news-events/news-releases).
        # Neither answers «which release is this story», which is the exact
        # reason Tier 5 (domain-root attribution) was deleted in july. So
        # the index never overwrites a primary any more — it goes into the
        # note, where the editor still sees an official source exists.
        # Restoring it as a real primary needs Layer 3 (core/press_search.py,
        # built but unwired — needs BRAVE_SEARCH_API_KEY) to find the actual
        # release URL.
        r.llm_note = (r.llm_note + " | " if r.llm_note else "") + \
            f"офиц. ньюсрум бренда: {nr_dom}"
        rooted += 1
    if rooted:
        print(f"Brand-newsroom: {rooted} rows annotated with the official "
              f"newsroom index (never overwrites the primary link).")


def _run_corpus_primary_source_pass(article_rows: list[ArticleRow]) -> None:
    """Level 2 primary-source detection.

    For every row that (a) is going to the output sheet and (b) does not
    already have a high-confidence primary source from Level 1, look inside
    the corpus of THIS run for an earlier-published article with a very
    similar title. If found — it's most likely the actual primary source.
    """
    # Build an in-memory corpus of everything we have scored this run.
    corpus: list[CorpusEntry] = []
    for r in article_rows:
        if not r.article_url or not r.title:
            continue
        pub: datetime | None = None
        if r.published_at:
            try:
                pub = datetime.fromisoformat(r.published_at.replace("Z", "+00:00"))
            except ValueError:
                pub = None
        if pub is None:
            continue  # cannot use an undated entry to establish ordering
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)
        corpus.append(
            CorpusEntry(
                url=r.article_url,
                title=r.title,
                published_at=pub,
                domain=domain_of(r.article_url),
            )
        )

    if not corpus:
        print("Primary-source L2: corpus empty — skipped.")
        return

    press_hosts = PRIMARY_CUES.press_release_hosts if PRIMARY_CUES else []
    upgraded = 0
    for r in article_rows:
        # Only act on rows that will actually reach the editor.
        if r.verdict not in {"Точно новость", "Возможно новость"}:
            continue
        # Cached rows already have a primary source from the previous run.
        if r.from_cache:
            continue
        # Already high-confidence → leave as is.
        if r.primary_confidence == "high":
            continue
        # Need a known publication date on the target row.
        try:
            target_pub = datetime.fromisoformat(
                (r.published_at or "").replace("Z", "+00:00")
            )
            if target_pub.tzinfo is None:
                target_pub = target_pub.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        found = detect_earliest_in_corpus(
            article_url=r.article_url,
            article_title=r.title,
            article_published_at=target_pub,
            corpus=corpus,
            whitelist_domains=WHITELIST,
            press_release_hosts=press_hosts,
            mirror_hosts=PRIMARY_CUES.mirror_hosts if PRIMARY_CUES else [],
            source_hint_domain=r.source_hint_domain,
        )
        if found is None:
            continue
        url, dom, conf = found
        r.primary_url = url
        r.primary_domain = dom
        r.primary_confidence = conf
        r.primary_method = "corpus-earlier"
        upgraded += 1

    print(f"Primary-source L2: upgraded {upgraded} rows from corpus.")


# Same-site redirector paths that wrap a marked-source link. ixbt (jul-21
# editor: «ссылка на первоисточник есть в самой статье»): the «Источник» link
# is api.ixbt.com/to/<encrypted> — SAME-site, so Tier 0.5 discarded it and the
# row fell to self@low, while one HEAD request resolves the 302 to the real
# target (the observed case: x.com/Tesla/status/… — the brand's own post).
_REDIRECT_PATH_MARKERS = ("/to/", "/go/", "/away", "/redirect", "/out/")


def _resolve_same_site_redirect(client, hint_url: str, article_url: str) -> str:
    """Resolve a marked-source link wrapped in the site's OWN redirector.

    Only fires for a same-site hint whose path looks like a redirect (so a
    normal external hint costs nothing); one HEAD, no body. On any failure —
    or if the redirect leads back to the same site — returns the input
    unchanged, so behaviour degrades to exactly what it was before."""
    try:
        if not hint_url:
            return hint_url
        from news_agent.core.primary_source import _normalise_domain, _same_site
        ad = _normalise_domain(domain_of(article_url))
        if not _same_site(domain_of(hint_url), ad):
            return hint_url  # already external — nothing to resolve
        path = urlparse(hint_url).path or ""
        if not any(m in path for m in _REDIRECT_PATH_MARKERS):
            return hint_url
        resp = client.head(hint_url, follow_redirects=False, timeout=6.0)
        loc = resp.headers.get("location", "")
        if loc.startswith("http") and not _same_site(domain_of(loc), ad):
            return loc
    except Exception:  # noqa: BLE001 — resolution is best-effort
        pass
    return hint_url


def _fetch_and_score(
    client: httpx.Client,
    link: str,
    r: SourceResult,
    source_idx: int,
    article_idx: int,
    article_rows: list[ArticleRow],
    *,
    rss_pub_date: datetime | None = None,
    rss_title: str = "",
    rss_body: str = "",
) -> None:
    row = ArticleRow(
        source_idx=source_idx,
        source_url=r.url,
        article_idx=article_idx,
        article_url=link,
    )
    try:
        resp = _http_get(client, link)
        resp.raise_for_status()
        article = extract_article(
            html=resp.text,
            url=str(resp.url),
            source_name="",
            source_url="",
            source_language=None,
            fallback_title=rss_title,
            fallback_body=rss_body,
            preferred_published=rss_pub_date,
        )
    except Exception as e:  # noqa: BLE001
        r.error = (r.error + f" | {type(e).__name__}")[:200]
        row.article_reasons = f"fetch_failed: {type(e).__name__}"
        row.verdict = "Отклонить (ошибка загрузки)"
        article_rows.append(row)
        return
    if article is None:
        row.article_reasons = "extract_failed"
        row.verdict = "Отклонить (не удалось извлечь)"
        article_rows.append(row)
        return

    # ixbt-class marked source: unwrap the site's own redirector so Tier 0.5
    # sees the real external target instead of discarding a same-site link.
    _hint = getattr(article, "source_hint_url", "") or ""
    if _hint:
        _resolved = _resolve_same_site_redirect(client, _hint, article.url)
        if _resolved != _hint:
            article.source_hint_url = _resolved

    if _score_article(article, r, row):
        article_rows.append(row)


def _score_article(article, r: SourceResult, row: ArticleRow) -> bool:  # type: ignore[no-untyped-def]
    """Populate heuristic fields on ``row`` from ``article``; update source counters.

    Returns ``False`` if the row should NOT be written to the output sheet
    (currently: articles older than FRESHNESS_HOURS — they never go to LLM
    and clutter the report with no value).
    """
    # Defrag chokepoint: never let a #fragment (autoreview.ru #comments)
    # reach the sheet / dedup, regardless of which fetch path produced it.
    row.article_url = urldefrag(article.url)[0]
    row.title = article.title
    row.body_len = len(article.body)
    row.body_excerpt = article.body[:1000]  # ≤1000 chars → ~40% token saving on LLM input
    row.published_at = article.published_at.isoformat() if article.published_at else ""
    row.has_image = bool(article.image_url)
    row.image_url = article.image_url or ""
    row.source_language = (article.source_language or "").lower()[:2]

    # --- Freshness gate ------------------------------------------------------
    # When RUN_WINDOW is populated (scheduled-run mode), accept only articles
    # whose published_at falls inside [since, now]. Else fall back to a fixed
    # FRESHNESS_HOURS lookback (manual / ad-hoc runs).
    if article.published_at is not None:
        if RUN_WINDOW is not None:
            if not is_in_window(
                article.published_at,
                since=RUN_WINDOW.since,
                now=RUN_WINDOW.now,
            ):
                return False
        elif not is_fresh(article.published_at, hours=FRESHNESS_HOURS):
            return False
    else:
        # Undated article — was previously waved through ("dedup will catch
        # reruns"). Editor review showed this lets in archive pages from
        # 2022/2023/2024 (Hyundai newsroom, BYD press) that have no
        # published_at in HTML. Now: only allow undated articles from
        # whitelist (editor-trusted) domains. Everything else gets dropped.
        host = domain_of(article.url)
        if host not in WHITELIST:
            return False

    # --- URL year sanity check -----------------------------------------------
    # Some sites embed publication year in the URL path. If the URL says
    # /2022/ or /2023/ and the run window is "now", drop the article — it's
    # an archive page that escaped the published_at gate (or its date got
    # mis-extracted). Tolerance: prev/current year always allowed.
    url_year = year_in_url_path(article.url)
    if url_year is not None:
        ref_now = RUN_WINDOW.now if RUN_WINDOW is not None else datetime.now(timezone.utc)
        max_age_years = max(1, (FRESHNESS_HOURS // 24 // 365) + 1)
        if url_year < ref_now.year - max_age_years:
            return False

    # --- Per-source quality gate ---------------------------------------------
    # Editor flagged some Telegram channels and aggregators as «1-2 строки,
    # без цифр». These still get fetched (we want to see them in the report),
    # but face stricter filters: longer body, mandatory date, no auto-grade
    # to "certain_news" (always require LLM relevance). See
    # config/source_quality.yaml.
    is_low_quality_source = (
        SOURCE_QUALITY is not None
        and SOURCE_QUALITY.is_low_quality(r.url)
    )
    if is_low_quality_source:
        if len(article.body.strip()) < 500:
            return False
        if article.published_at is None:
            return False

    # --- Blacklist gate ------------------------------------------------------
    # Hard-reject topics the editorial team explicitly opted out of
    # (buses, construction equipment, agricultural machinery, battery
    # raw-material prices). Cheaper than a full LLM call.
    if BLACKLIST is not None:
        bl = blacklist_hit(article, BLACKLIST, brands=BRANDS)
        if bl.hit:
            row.verdict = "Точно не новость (чёрный список)"
            row.article_reasons = bl.reason
            return True

    # --- Multi-news gate -----------------------------------------------------
    # Editor: «несколько разных новостей по одной ссылке — нельзя ставить».
    # A title with a semicolon and substantial content on both sides is
    # almost always a digest / aggregator post.
    if is_multi_news_title(article.title):
        row.verdict = "Точно не новость (мульти-новость)"
        row.article_reasons = "multi-news-title (semicolon split)"
        return True

    # --- Dzen listicle gate --------------------------------------------------
    # "Parasitism, gigantism and three more obvious trends..." — these are
    # editorial Дзен-канал listicles, off-topic regardless of subject.
    if is_dzen_listicle(article.title):
        row.verdict = "Точно не новость (дзен-листикл)"
        row.article_reasons = "dzen-style listicle pattern"
        return True

    # --- Supplier-abstract-showcase gate -------------------------------------
    # Editor's round-2 «обо всем и ни о чем» pattern: a parts vendor
    # showcases abstract "technologies / solutions / matrix" at a motorshow.
    # Reject unless a passenger-brand is also named (then it's coverage).
    if is_supplier_abstract_showcase(article.title, article.body):
        row.verdict = "Точно не новость (поставщик-абстракция)"
        row.article_reasons = "supplier-abstract-showcase (vendor + abstract noun + motorshow)"
        return True

    # ---- Phase-1 launch-lifecycle tagging (heuristic, no LLM) -------------
    # MUST run before the cache short-circuit below — cached rows still get
    # populated stage columns (heuristic is free and deterministic). Wrapped
    # in try/except: detection failure must NOT break row writing.
    try:
        _stages = detect_launch_stages(article.title, article.body)
        _bm = extract_brand_model(article.title, BRANDS)
        if _stages and _bm:
            row.launch_stages = list(_stages)
            row.launch_brand_model = f"{_bm[0]} {_bm[1]}"
    except Exception as _e:  # noqa: BLE001
        print(
            f"   ! launch-stage detection failed for {article.url}: "
            f"{type(_e).__name__}: {str(_e)[:80]}"
        )

    # Final-URL deduplication. Using canonicalised URL handles trailing slashes,
    # UTM params and similar differences that hide the same page behind many
    # referring links (see the benchmarkminerals.com × 5 case from v1).
    canon = canonicalise(article.url)
    if canon in SEEN_FINAL_URLS:
        row.article_reasons = "duplicate-final-url"
        row.verdict = "Отклонить (дубль финального URL)"
        return True
    SEEN_FINAL_URLS.add(canon)

    # Already published by the editor? Deterministic dedup against the daily
    # "Опубликованные (все)" archive — exact source URL or exact recent
    # title. Checked BEFORE the cache so it re-evaluates against the fresh
    # archive every run (not persisted). Catches the same-source / same-
    # headline dups; cross-source paraphrases still rely on the LLM flag.
    pub_reason = already_published(
        article.url, article.title, PUBLISHED_URLS, PUBLISHED_TITLES)
    if pub_reason:
        row.article_reasons = f"published-archive-{pub_reason}"
        row.verdict = "Отклонить (уже опубликовано редактором)"
        return True

    # SQLite persistent cache — have we already processed this URL?
    # We only honour the cache when it carries a real LLM verdict OR a
    # hard-reject heuristic verdict (no LLM ever needed for those). A
    # row left over from a fetch-only run (LLM was disabled, llm_section
    # blank for would-be candidates) does NOT short-circuit — we let LLM
    # do its job this time around.
    uh = url_hash(canon)
    if uh in PREVIOUSLY_SEEN:
        cached = PREVIOUSLY_SEEN[uh]
        cached_verdict = cached.get("verdict", "")
        has_llm_classification = bool(cached.get("llm_section"))
        # Classifier-version gate (peer-review §4). A cached row carries
        # the version of the prompt+heuristics that produced it. If that
        # differs from the live version, the RULES changed since — so we
        # must NOT honour rule-based verdicts; the row re-runs fresh.
        # Classifier-version gate (peer-review §4): a cached row carries
        # the version of the prompt+heuristics that produced it. If that
        # differs from the live version the RULES changed, so rule-based
        # verdicts are not honoured and the row re-runs fresh. Identity
        # verdicts (dups/stale) stay valid regardless of version.
        version_ok = cached.get("cls_ver", "") == CLASSIFIER_VERSION
        cache_authoritative = cache_is_authoritative(
            cached_verdict, has_llm_classification, version_ok,
            prompt_ok=(cached.get("prompt_ver") == PROMPT_VERSION
                       if cached.get("prompt_ver") else None),
            heuristics_ok=(cached.get("heur_ver") == HEURISTICS_VERSION
                           if cached.get("heur_ver") else None),
            prompt_change=PROMPT_CHANGE,
        )
        if cache_authoritative:
            row.verdict = cached_verdict or row.verdict
            row.llm_relevance = cached.get("llm_relevance", "")
            row.llm_section = cached.get("llm_section", "")
            row.llm_region = cached.get("llm_region", "")
            row.llm_confidence = cached.get("llm_confidence", "")
            row.llm_title_en = cached.get("llm_title_en", "")
            row.llm_title_ru = cached.get("llm_title_ru", "")
            row.llm_reason = cached.get("llm_reason", "")
            # Always mark every cache restoration with "из кэша" so the
            # editor can see at a glance that the row wasn't re-evaluated
            # this run. Cached rows still carry the full verdict / section
            # / region / titles the previous run produced — they look just
            # like fresh rows, just with this lineage tag in column M.
            cache_note = "из кэша"
            row.llm_note = (
                cache_note if not row.llm_note else f"{cache_note} | {row.llm_note}"
            )
            row.llm_cost_usd = 0  # zero this run
            row.primary_url = cached.get("primary_url", "")
            row.primary_domain = cached.get("primary_domain", "")
            row.primary_confidence = cached.get("primary_confidence", "")
            row.primary_method = cached.get("primary_method", "") or "cached"
            # Hybrid dedup Stage 1: restore semantic event-key (absent on
            # pre-Stage-1 cache rows → stays "").
            row.event_brand = cached.get("event_brand", "")
            row.event_model = cached.get("event_model", "")
            row.event_type = cached.get("event_type", "")
            row.article_score = cached.get("article_score", row.article_score)
            row.article_reasons = cached.get("article_reasons", "from-cache")
            row.is_article = bool(cached.get("is_article", True))
            row.auto_topic = bool(cached.get("auto_topic", True))
            row.auto_hits = cached.get("auto_hits", "")
            row.from_cache = True
            # RU transport-civic rescue ALSO at cache-restore time.
            # Today's heuristic fixes (rescue, blacklist) must apply even
            # to articles cached BEFORE the fix — otherwise a recently
            # cached run short-circuits the whole change. Only flips
            # LLM-rejections (not heuristic hard-rejects, which are
            # correct); is_ru_transport_civic gates out military/banking.
            if (False  # RESCUE DISABLED jun-19 (see live path) — keep cached
                    # LLM rejections rejected; constitution handles RU-transport
                    and row.verdict == "Отклонено LLM"
                    and is_ru_transport_civic(row.title, row.body_excerpt)):
                row.verdict = "Точно новость"
                row.llm_relevance = "Да"
                row.llm_section = "Local specifics"
                row.llm_region = "Local"
                row.llm_confidence = 0.55
                row.llm_note = (
                    "RU-транспорт-civic rescue (из кэша) — проверьте"
                    + (f" | {row.llm_note}" if row.llm_note else "")
                )
            return True
        # Else: cache exists but is incomplete (fetch-only run). Fall through
        # so the normal heuristic + LLM pipeline picks this row up fresh.

    if article.title:
        r.articles_with_title += 1
        if len(r.sample_titles) < 3:
            r.sample_titles.append(article.title)
    if len(article.body) >= 200:
        r.articles_with_body += 1
    if article.published_at is not None:
        r.articles_with_date += 1
    if article.image_url:
        r.articles_with_image += 1
    if looks_like_news(article.title, article.body, article.published_at, article.url):
        r.news_like += 1

    verdict = looks_like_article(article, whitelist=WHITELIST)
    row.is_article = verdict.is_article
    row.article_score = verdict.score
    row.article_reasons = ", ".join(verdict.reasons)

    topic = is_auto_or_economy(article)
    row.auto_topic = topic.is_auto_or_economy
    row.auto_hits = ", ".join(topic.hit_samples)

    if verdict.is_article:
        r.passed_is_article += 1
        if topic.is_auto_or_economy:
            r.passed_auto_topic += 1
            if len(r.sample_passed) < 3:
                r.sample_passed.append(article.title)

    grade = grade_article(verdict, topic)

    # Low-quality sources: even strong heuristic signals don't get a free
    # pass. Force the row through LLM relevance check by demoting
    # certain_news → possible_news. We also need at least 2 auto_hits AND
    # auto-topic to keep — drop the row otherwise (heuristic precision is
    # unreliable on these sources).
    if is_low_quality_source:
        if grade == "certain_news":
            grade = "possible_news"
        if topic.auto_hits < 2:
            grade = "off_topic"

    row.verdict = {
        "certain_news": "Точно новость",
        "possible_news": "Возможно новость",
        "off_topic": "Точно не новость (не авто)",
        "not_article": "Точно не новость (не статья)",
    }[grade]

    # ---- Primary-source detection, level 1 (body links + cues) --------------
    # Only bother computing for rows that will actually reach the output
    # (certain/possible). For rejects we skip — saves time on 200+ items.
    if grade in ("certain_news", "possible_news") and PRIMARY_CUES is not None:
        p_url, p_dom, p_conf = detect_primary_source(
            article_url=article.url,
            body=article.body,
            title=article.title,
            outbound_links=article.outbound_links,
            brands=BRANDS,
            cues=PRIMARY_CUES,
            whitelist_domains=WHITELIST,
            source_hint_url=getattr(article, "source_hint_url", ""),
        )
        _hint = getattr(article, "source_hint_url", "")
        row.source_hint_domain = domain_of(_hint) if _hint else ""
        row.primary_url = p_url
        row.primary_domain = p_dom
        row.primary_confidence = p_conf
        row.primary_method = "body-link" if p_conf != "low" else "self"
        # Flag contested link-soup for the LLM primary picker (the redistribution
        # -host «не видит первоисточник» class, e.g. autonews linking BOTH the
        # brand site AND a carscoops related piece). [] on the common case, so
        # the LLM call fires rarely; the actual arbitration runs in
        # _run_llm_pass, where the LLM client lives.
        if PRIMARY_LLM_PICK:
            row.primary_candidates = arbitration_candidates(
                article_url=article.url,
                body=article.body,
                title=article.title,
                outbound_links=article.outbound_links,
                brands=BRANDS,
                cues=PRIMARY_CUES,
                whitelist_domains=WHITELIST,
            )

    return True


def _discover_article_links(index_url: str, html: str, limit: int) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    host = urlparse(index_url).netloc
    # www./m./amp.-tolerant same-site check. The exact-netloc comparison
    # silently ZERO-yielded any source whose page links use a www variant of
    # the listed domain: the source sheet says "carscoops.com" but every
    # article href on the page is "www.carscoops.com/…" → all 50+ links
    # dropped, source reported healthy-but-0-links. 23 editor pubs from
    # carscoops alone were missed in the 23.06–07.07 window (top S2 domain in
    # the miss-funnel). Same normalisation as _same_site in primary_source.
    host_norm = _re.sub(r"^(?:www|m|amp)\.", "", host.lower())
    seen: set[str] = set()
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        absolute = urljoin(index_url, href)
        # Strip the #fragment — index pages on some portals (autoreview.ru)
        # link straight to the comments anchor (".../slug#comments"), which
        # then gets written to the sheet verbatim and also splits dedup
        # (same article with vs without #comments). The fragment never
        # matters for article identity.
        absolute = urldefrag(absolute)[0]
        p = urlparse(absolute)
        if _re.sub(r"^(?:www|m|amp)\.", "", p.netloc.lower()) != host_norm:
            continue
        if _looks_like_article(absolute) and absolute not in seen:
            seen.add(absolute)
            out.append(absolute)
    # Rank before capping (jul-28). Taking the first `limit` links in DOM
    # order lets a site's NAVIGATION eat the whole per-source budget: on
    # europarl's press-room the 24 language switchers plus /agenda,
    # /press-kit, /faq filled all 35 slots and not one press release was
    # ever fetched (source ran at 0 articles for weeks). A story URL carries
    # a strong marker — a date in the path, a long hyphenated slug, or a
    # numeric/alphanumeric id — while section pages are short and bare. Sort
    # is STABLE, so within a tier the original page order is preserved and
    # every source that was already healthy keeps its exact link set.
    def _strength(u: str) -> int:
        p = urlparse(u).path.rstrip("/")
        segs = [s for s in p.split("/") if s]
        last = segs[-1] if segs else ""
        if _DATE_PATH_RE.search(p):
            return 0                     # dated permalink — strongest
        if last.count("-") >= 3:
            return 1                     # long hyphenated slug
        if any(ch.isdigit() for ch in last) and len(last) >= 4:
            return 2                     # id-style leaf (autostat, europarl)
        if len(segs) >= 3:
            return 3                     # deep path, no marker
        return 4                         # shallow section page
    out.sort(key=_strength)
    return out[:limit]


# ------------------------------------------------------------- CLI
def _parse_cli(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI args. All have sensible defaults so the scheduled
    invocation can be just ``python scripts/batch_fetch_test.py``.
    """
    p = argparse.ArgumentParser(
        description="NewsMaker batch fetch — scheduled / ad-hoc news ingest",
    )
    p.add_argument(
        "--since-overlap-minutes", type=int, default=120,
        help="How far before last_run_at to start the fetch window (default 120 "
             "= 2h). Re-scans the 2h before the last run so articles that only "
             "appeared on the source AFTER that run fetched it are still caught "
             "(a source-timing coverage lever). Re-fetched items are deduped.",
    )
    p.add_argument(
        "--max-lookback-hours", type=int, default=48,
        help="Ceiling on the fetch window. First run / stale state clamps to "
             "this. Default 48h (not 24): if a daily run is MISSED, a 24h "
             "ceiling permanently drops that day's publications, while 48h "
             "lets the next run catch the backlog. Re-seen articles restore "
             "from cache (≈$0); only genuinely-new ones cost LLM. On a normal "
             "daily cadence the window is ~24h, so this only binds after a gap.",
    )
    p.add_argument(
        "--hot", action="store_true",
        help="HOT lane: poll only the fast-rotating sources from "
             "config/hot_sources.yaml (index churn beats the full-run "
             "cadence). Uses its OWN tab family ('… (гор) vN') so the full "
             "chain's tab handoff is never poisoned; run it with "
             "RUN_STATE_PATH=data/state_hot.json so the two lanes keep "
             "independent windows. No NHTSA seed, no LLM-abort recovery "
             "hint (a hot re-run costs minutes, not hours).",
    )
    p.add_argument(
        "--no-window", action="store_true",
        help="Disable scheduled-run mode and use legacy FRESHNESS_HOURS instead. "
             "Useful for ad-hoc backfill runs that should NOT touch state.json.",
    )
    p.add_argument(
        "--state-path", type=Path, default=STATE_PATH,
        help=f"Path to the JSON state file (default {STATE_PATH}).",
    )
    p.add_argument(
        "--runs-log", type=Path, default=RUNS_LOG_PATH,
        help=f"Path to the per-run summary log (default {RUNS_LOG_PATH}).",
    )
    p.add_argument(
        "--no-playwright", action="store_true",
        help="Disable the Playwright JS-fallback entirely. Use when a "
             "JS-gated source hangs the run (a single bad site's "
             "navigation can stall the whole prog). httpx still fetches "
             "the vast majority of sources; only a few Cloudflare/SPA "
             "sites are skipped.",
    )
    p.add_argument(
        "--no-published-dedup", action="store_true",
        help="Disable the deterministic dedup against the editor's daily "
             "'Опубликованные (все)' archive (exact URL / recent exact "
             "title). On by default.",
    )
    p.add_argument(
        "--no-llm", action="store_true",
        help="Skip the LLM pass entirely — heuristic-only prog. Used when "
             "the API balance is exhausted or for offline QA. Articles still "
             "get the verdict 'Точно новость / Возможно новость' from "
             "heuristic, but classify/translate are skipped. After API is "
             "restored, run retry_failed_llm.py on the resulting tab.",
    )
    p.add_argument(
        "--legacy-llm", action="store_true",
        help="Use the old 2-call LLM sequence (is_automotive + "
             "classify_section) instead of the consolidated editorial_review. "
             "Default: editorial_review. Useful for A/B comparison or "
             "rollback if the new prompt produces worse results.",
    )
    return p.parse_args(argv)


def _append_run_log(
    path: Path,
    *,
    run_at: datetime,
    window: RunWindow | None,
    rows_total: int,
    rows_new: int,
    cost_usd: float,
    elapsed_s: float,
    status: str,
) -> None:
    """One JSON-line per run. Easy to tail / grep / aggregate later."""
    import json as _json
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {
        "run_at": run_at.isoformat(timespec="seconds"),
        "status": status,
        "window_since": window.since.isoformat(timespec="seconds") if window else None,
        "window_now": window.now.isoformat(timespec="seconds") if window else None,
        "window_fallback": window.using_fallback if window else None,
        "previous_run_at": (
            window.previous_run_at.isoformat(timespec="seconds")
            if window and window.previous_run_at else None
        ),
        "rows_total": rows_total,
        "rows_new": rows_new,
        "cost_usd": round(cost_usd, 4),
        "elapsed_s": round(elapsed_s, 1),
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(_json.dumps(record, ensure_ascii=False) + "\n")


def _health_check(
    results: list[SourceResult],
    article_rows: list[ArticleRow],
    *,
    llm_ran: bool,
    llm_aborted: str,
    dedup_enabled: bool,
    prev_state: dict,
) -> list[str]:
    """End-of-run invariant check — the anti-silent-failure gate.

    Every incident of the last month (archive truncation at 6000 rows,
    usage-limit tail burn, serial-date dedup decay 219→47→0, Brotli garbage
    HTML, JS-shell zero-yield) was detectable from data the run already had —
    it just was never asserted. This checks the cheap invariants and returns
    ALARMS: non-empty ⇒ the run is degraded, main() exits non-zero and the
    chain does NOT push a silently broken feed. Softer anomalies print as
    warnings without failing the run.
    """
    alarms: list[str] = []
    warns: list[str] = []

    # 1. LLM-pass integrity: after a completed pass no fresh candidate may
    #    remain unclassified (llm_relevance empty = never got a verdict).
    stuck = [
        r for r in article_rows
        if r.verdict in ("Точно новость", "Возможно новость")
        and not r.from_cache and not r.llm_relevance
    ]
    if llm_ran:
        if llm_aborted:
            alarms.append(
                f"LLM pass aborted ({llm_aborted}) — "
                f"{len(stuck)} candidates unclassified"
            )
        elif stuck:
            alarms.append(
                f"{len(stuck)} candidates unclassified after a 'completed' "
                f"LLM pass (errors swallowed per-row?)"
            )

    # 2. Published-archive dedup: floors + shrink-vs-last-run (the gate has
    #    silently died 3 times; a >10% shrink is the early-decay signature).
    if dedup_enabled:
        prev_urls = int(prev_state.get("archive_urls_count") or 0)
        if (len(PUBLISHED_URLS) < ARCHIVE_URLS_FLOOR
                or len(PUBLISHED_TITLES) < ARCHIVE_TITLES_FLOOR):
            alarms.append(
                f"archive dedup degraded: {len(PUBLISHED_URLS)} urls / "
                f"{len(PUBLISHED_TITLES)} recent titles (floors "
                f"{ARCHIVE_URLS_FLOOR}/{ARCHIVE_TITLES_FLOOR})"
            )
        elif prev_urls and len(PUBLISHED_URLS) < prev_urls * 0.9:
            warns.append(
                f"archive urls shrank {prev_urls} → {len(PUBLISHED_URLS)} "
                f"(>10% — archive read decaying?)"
            )
        # Archive FRESHNESS — count floors pass even when the export stops
        # updating; only the newest-row date can see a stall. The normal
        # export cadence is a ~1-2 day lag (verified jul-10: current through
        # 08.07 — an earlier "stale since 30.06" reading was a string-max
        # analysis bug, see published_archive.py), so the threshold is 3+
        # days: it fires on a genuine stall, not on the routine lag.
        arch_dt = published_archive.LAST_ARCHIVE_MAX_DT
        if arch_dt is not None:
            age_d = (datetime.now(timezone.utc) - arch_dt).days
            if age_d >= 3:
                warns.append(
                    f"published-archive STALE: newest row "
                    f"{arch_dt.strftime('%d.%m')} ({age_d} days old) — "
                    f"portal dups of the last {age_d} days are INVISIBLE to "
                    f"dedup. Нужна свежая выгрузка «Опубликованные (все)»."
                )

    # 3. Fetch mass-failure signal (informational unless widespread).
    err_sources = sum(1 for s in results if s.error)
    zero_links = sum(
        1 for s in results if not s.error and s.articles_attempted == 0)
    if results and err_sources > len(results) * 0.4:
        warns.append(
            f"{err_sources}/{len(results)} sources errored — mass fetch "
            f"failure (network/proxy/WAF)?"
        )

    print("\n=== RUN HEALTH ===")
    print(f"  sources : {len(results)} fetched | {err_sources} errored | "
          f"{zero_links} reachable-but-0-links")
    if llm_ran:
        print(f"  llm     : {'ABORTED: ' + llm_aborted if llm_aborted else 'completed'}"
              f" | stuck={len(stuck)}")
    if dedup_enabled:
        print(f"  dedup   : archive {len(PUBLISHED_URLS)} urls / "
              f"{len(PUBLISHED_TITLES)} recent titles")
    for w in warns:
        print(f"  WARN  : {w}")
    for a in alarms:
        print(f"  ALARM : {a}")
    if alarms:
        print(
            "!!! RUN DEGRADED — exiting non-zero so the chain does not push "
            "a silently broken feed. State window NOT advanced (next run "
            "re-covers this period; cached classifications make it cheap).",
            file=sys.stderr,
        )
    else:
        print("  status  : OK")
    return alarms


# ------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    global WHITELIST, SEEN_FINAL_URLS, BLACKLIST, BRANDS, DEDUP_STORE, PREVIOUSLY_SEEN
    global PUBLISHED_URLS, PUBLISHED_TITLES, PUBLISHED_ALL_TITLES
    global PRIMARY_CUES, PW_FETCHER, PW_ALLOWLIST, IMP_FETCHER, IMP_ALLOWLIST
    global PROXY_URL, PROXY_DIRECT
    global RUN_WINDOW
    args = _parse_cli(argv)
    state = RunState(args.state_path)
    # A fresh run supersedes any pending LLM-abort recovery hint (it re-covers
    # the same window anyway) — drop it so a stale hint can never hijack a
    # future retry invocation.
    try:
        (args.state_path.parent / "llm_abort_recovery.json").unlink(missing_ok=True)
    except OSError:
        pass
    if args.no_window:
        RUN_WINDOW = None
        print(
            f"Window mode: OFF (--no-window) — using legacy FRESHNESS_HOURS={FRESHNESS_HOURS}h. "
            f"State file will NOT be updated."
        )
    else:
        RUN_WINDOW = state.compute_window(
            overlap_minutes=args.since_overlap_minutes,
            max_lookback_hours=args.max_lookback_hours,
        )
        prev = (
            RUN_WINDOW.previous_run_at.isoformat(timespec="seconds")
            if RUN_WINDOW.previous_run_at else "none"
        )
        mode = "fallback (max_lookback)" if RUN_WINDOW.using_fallback else "incremental"
        print(
            f"Window mode: {mode}\n"
            f"  previous_run_at: {prev}\n"
            f"  since:           {RUN_WINDOW.since.isoformat(timespec='seconds')}\n"
            f"  now:             {RUN_WINDOW.now.isoformat(timespec='seconds')}\n"
            f"  overlap:         {args.since_overlap_minutes} min\n"
            f"  max_lookback:    {args.max_lookback_hours} h"
        )

    WHITELIST = load_whitelist_domains()
    BLACKLIST = load_blacklist()
    BRANDS = load_brand_domains()
    PRIMARY_CUES = load_primary_source_cues()
    global SOURCE_QUALITY
    SOURCE_QUALITY = load_source_quality()
    print(
        f"Source quality: {len(SOURCE_QUALITY.low_quality)} low-quality entries "
        f"(stricter heuristic thresholds will apply)"
    )
    SEEN_FINAL_URLS = set()
    print(f"Whitelist domains loaded: {len(WHITELIST)}")
    print(f"Brand list loaded: {len(BRANDS)} brands (used to whitelist blacklist hits)")
    print(
        f"Blacklist: {len(BLACKLIST.topic_phrases_ru)} RU phrases, "
        f"{len(BLACKLIST.topic_phrases_en)} EN phrases, {len(BLACKLIST.domains)} domains"
    )

    # --- SQLite dedup + classification cache (persistent across runs) --------
    DEDUP_STORE = DedupStore(SQLITE_PATH)
    PREVIOUSLY_SEEN = DEDUP_STORE.load_cache(DEDUP_PORTAL)
    _cur_ver = sum(
        1 for c in PREVIOUSLY_SEEN.values()
        if c.get("cls_ver") == CLASSIFIER_VERSION
    )
    _stale_ver = len(PREVIOUSLY_SEEN) - _cur_ver
    print(
        f"SQLite cache loaded: {len(PREVIOUSLY_SEEN)} classified url_hashes "
        f"for portal={DEDUP_PORTAL} ({SQLITE_PATH})."
    )
    print(
        f"  classifier_version={CLASSIFIER_VERSION} | "
        f"{_cur_ver} cache rows current, {_stale_ver} stale "
        f"(stale rule-verdicts re-classify if re-fetched; "
        f"identity-verdicts e.g. dups stay valid)."
    )
    # Split-stamp view: what a version change ACTUALLY costs this run.
    _p_ok = sum(1 for c in PREVIOUSLY_SEEN.values()
                if c.get("prompt_ver") == PROMPT_VERSION)
    _h_ok = sum(1 for c in PREVIOUSLY_SEEN.values()
                if c.get("heur_ver") == HEURISTICS_VERSION)
    _legacy = sum(1 for c in PREVIOUSLY_SEEN.values() if not c.get("prompt_ver"))
    _saved = 0
    if PROMPT_CHANGE != PROMPT_CHANGE_BOTH:
        _saved = sum(
            1 for c in PREVIOUSLY_SEEN.values()
            if c.get("prompt_ver") and c.get("prompt_ver") != PROMPT_VERSION
            and llm_cache_survives_prompt_change(c.get("verdict", ""), PROMPT_CHANGE)
        )
    print(
        f"  split stamps: prompt={PROMPT_VERSION} ({_p_ok} rows match), "
        f"heuristics={HEURISTICS_VERSION} ({_h_ok} match), "
        f"{_legacy} legacy rows | PROMPT_CHANGE={PROMPT_CHANGE} ({PROMPT_CHANGE_SRC})"
        + (f" → {_saved} stale-prompt LLM rows kept (~${_saved * 0.0065:.2f} saved)"
           if _saved else "")
    )

    svc = sheets_client()

    # Deterministic anti-dup: load the editor's daily-refreshed published
    # archive so candidates already published (same source URL / recent
    # exact title) are dropped before they reach the editor's feed.
    if not args.no_published_dedup:
        # The archive lives ONLY in the editor's spreadsheet (1fQic…), NOT in
        # this prog's working sheet (SHEET_ID = the 14PTb copy). Reading
        # SHEET_ID here returned empty (no such tab) → the dedup was silently
        # dead. Read from EDITOR_SPREADSHEET_ID.
        _editor_id = os.environ.get(
            "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs")
        PUBLISHED_URLS, PUBLISHED_TITLES, PUBLISHED_ALL_TITLES = published_archive.load_published_index(
            svc, _editor_id)
        print(f"Published-archive dedup: {len(PUBLISHED_URLS)} source URLs + "
              f"{len(PUBLISHED_TITLES)} recent titles loaded "
              f"(drop candidates already published by the editor).")
        # Floor alarm — this exact gate has silently died THREE times (empty
        # tab read, serial-date decay 219→47→0, the A1:R6000 truncation). The
        # archive is ~6000 rows, so thousands of URLs / dozens of recent
        # titles are the legitimate minimum; below that the gate is degraded
        # and dups will flow to the editor. Loud now, and re-checked with
        # last-run comparison in the end-of-run health report.
        if len(PUBLISHED_URLS) < ARCHIVE_URLS_FLOOR or len(PUBLISHED_TITLES) < ARCHIVE_TITLES_FLOOR:
            print(
                f"!!! ARCHIVE DEDUP DEGRADED: {len(PUBLISHED_URLS)} urls / "
                f"{len(PUBLISHED_TITLES)} recent titles (floor "
                f"{ARCHIVE_URLS_FLOOR}/{ARCHIVE_TITLES_FLOOR}) — already-published "
                f"stories WILL reach the feed this run.", file=sys.stderr,
            )

    # NHTSA recalls go FIRST (right after the telegram seeds), NOT last: the
    # editor requires the NHTSA source for US recalls, and a mid-run API
    # usage-limit 400 truncates the TAIL of the candidate list — appended last,
    # the recalls were the consistent victims (they classify fine when reached;
    # see the editorial-review abort below). Early → always processed.
    nhtsa_seed = [NHTSA_RECALLS_SENTINEL] if ENABLE_NHTSA_RECALLS else []
    if ENABLE_NHTSA_RECALLS:
        print("NHTSA recalls source enabled "
              f"(lookback {NHTSA_RECALL_LOOKBACK_DAYS}d, cap {NHTSA_RECALL_MAX_ITEMS}).")
    urls = TELEGRAM_SEED_URLS + nhtsa_seed + read_active_sources(svc, NUM_SOURCES)
    if args.hot:
        # HOT lane: fast-rotating sources only (config/hot_sources.yaml),
        # own tab family. Everything else (cache, dedup, breaker, health)
        # is shared with the full run by construction.
        global REPORT_TAB_BASE, ARTICLES_TAB_BASE
        REPORT_TAB_BASE = REPORT_TAB_BASE + " (гор)"
        ARTICLES_TAB_BASE = ARTICLES_TAB_BASE + " (гор)"
        urls = load_hot_sources()
        print(f"HOT lane: {len(urls)} fast-rotating sources "
              f"(state: {args.state_path}).")
    if not urls:
        print("No active sources found in the sheet.", file=sys.stderr)
        return 2

    # ---- Resumable fetch: adopt a prior crashed run's progress if it matches --
    # The fetch loop below accumulates rows in memory; a crash (power outage)
    # otherwise loses them and re-parses every source on restart. When a
    # checkpoint from a crashed run matches THIS run's fingerprint, restore its
    # fetched rows + tabs + window and skip the sources it already did.
    _ckpt_path = fetch_checkpoint.checkpoint_path(args.state_path)
    _ckpt_fp: str | None = None
    _ckpt_loaded = None
    _ckpt_done: set[int] = set()  # source indices restored from a checkpoint
    results: list[SourceResult] = []
    article_rows: list[ArticleRow] = []
    if FETCH_RESUME and RUN_WINDOW is not None:
        _ckpt_fp = fetch_checkpoint.fingerprint(
            classifier_version=CLASSIFIER_VERSION,
            previous_run_at=RUN_WINDOW.previous_run_at,
            urls=urls, max_lookback_hours=args.max_lookback_hours,
            overlap_minutes=args.since_overlap_minutes,
            max_articles=MAX_ARTICLES, hot=args.hot)
        try:
            _ckpt_loaded = fetch_checkpoint.load(
                _ckpt_path, _ckpt_fp, max_age_hours=FETCH_RESUME_MAX_AGE_H)
        except Exception as _e:  # noqa: BLE001 — resume must never break a run
            print(f"fetch-resume: checkpoint load skipped ({type(_e).__name__}: {_e})")
            _ckpt_loaded = None

    _adopted = False
    if _ckpt_loaded is not None:
        # Reconstruct the crashed run's state. Guarded: a checkpoint written by
        # an older code version whose ArticleRow/SourceResult shape has since
        # changed (without a classifier_version bump, so the fingerprint still
        # matched) would make Cls(**d) raise — which must NOT crash the run.
        # On any failure, discard the checkpoint and fall through to a clean
        # full fetch (the module's degrade-to-no-checkpoint contract).
        try:
            h = _ckpt_loaded.header
            # Adopt the crashed run's window so freshness for the REMAINING
            # sources matches the rows already fetched (faithful continuation,
            # not a fresh window with a later `now`).
            _rw = RunWindow(
                since=datetime.fromisoformat(h.since_iso),
                now=datetime.fromisoformat(h.now_iso),
                using_fallback=h.using_fallback,
                previous_run_at=(datetime.fromisoformat(h.previous_run_at_iso)
                                 if h.previous_run_at_iso else None),
            )
            _restored_rows = [ArticleRow(**d) for d in _ckpt_loaded.rows]
            _restored_results = [SourceResult(**d) for d in _ckpt_loaded.results]
        except Exception as _e:  # noqa: BLE001 — schema drift must not break a run
            print(f"fetch-resume: checkpoint incompatible ({type(_e).__name__}: "
                  f"{_e}) — discarding, starting a clean full fetch.")
            fetch_checkpoint.clear(_ckpt_path)
            _ckpt_loaded = None
        else:
            report_tab, articles_tab = h.report_tab, h.articles_tab
            run_ts = h.run_ts
            RUN_WINDOW = _rw
            article_rows = _restored_rows
            results = _restored_results
            # Skip by SET MEMBERSHIP, not a threshold. append_source is
            # best-effort (swallowed on failure), so done_idx can have a HOLE
            # (source K fetched but its append failed). Skipping i<=max(done_idx)
            # would jump the hole and silently drop K's rows — they only ever
            # lived in the crashed run's memory. Membership re-fetches ONLY the
            # gaps and keeps every source that WAS persisted.
            _ckpt_done = set(_ckpt_loaded.done_idx)
            # Re-seed the within-run cross-source final-URL dedup set from the
            # restored rows (it starts empty each process), so a URL fetched
            # before the crash isn't emitted again by a post-resume source.
            for _r in article_rows:
                if _r.article_url:
                    SEEN_FINAL_URLS.add(canonicalise(_r.article_url))
            _gaps = sorted(
                set(range(1, (max(_ckpt_done) if _ckpt_done else 0) + 1)) - _ckpt_done)
            print(
                f"\n*** RESUMING crashed fetch ***  {len(_ckpt_done)}/{len(urls)} "
                f"sources restored ({len(article_rows)} rows) from "
                f"{_ckpt_path.name}.\n  Reusing tab '{articles_tab}', window "
                f"{h.since_iso} → {h.now_iso}."
                + (f"\n  Re-fetching {len(_gaps)} un-persisted gap source(s): "
                   f"{_gaps[:20]}" if _gaps else "")
            )
            _adopted = True

    if not _adopted:
        report_tab, articles_tab = allocate_new_tabs(svc)
        run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        print(f"New tabs allocated for this run:")
        print(f"  summary  : {report_tab}")
        print(f"  articles : {articles_tab}")
        if FETCH_RESUME and RUN_WINDOW is not None and _ckpt_fp is not None:
            try:
                fetch_checkpoint.begin(
                    _ckpt_path, fingerprint=_ckpt_fp, run_ts=run_ts,
                    report_tab=report_tab, articles_tab=articles_tab,
                    since=RUN_WINDOW.since, now=RUN_WINDOW.now,
                    using_fallback=RUN_WINDOW.using_fallback,
                    previous_run_at=RUN_WINDOW.previous_run_at,
                    total_sources=len(urls))
            except Exception as _e:  # noqa: BLE001 — never break a run over the checkpoint
                print(f"fetch-resume: checkpoint init skipped ({type(_e).__name__}: {_e})")

    print(f"Run: {run_ts}")
    print(f"Sources to test: {len(urls)}")
    for i, u in enumerate(urls, start=1):
        print(f"  {i:2d}. {u}")

    total_t0 = time.monotonic()

    # Playwright context — only spun up if the allowlist has entries AND the
    # library is importable. The fetch path gracefully degrades to httpx if
    # Playwright crashes mid-run (see `_http_get`).
    quirks = load_http_quirks()
    # --no-playwright: empty the allowlist so the JS fallback is a no-op
    # and Playwright is never spun up (a single hung JS site can stall
    # the whole 337-source run — see jun-2026 media.jaguar.com hang).
    if getattr(args, "no_playwright", False):
        PW_ALLOWLIST = PlaywrightAllowlist([])
        print("Playwright DISABLED (--no-playwright): httpx-only fetch.")
        quirks.playwright_domains = []
    else:
        PW_ALLOWLIST = PlaywrightAllowlist(quirks.playwright_domains)
    pw_cm = None
    if PLAYWRIGHT_AVAILABLE and quirks.playwright_domains:
        try:
            pw_cm = PlaywrightFetcher(timeout_ms=int(HTTP_TIMEOUT * 1000))
            PW_FETCHER = pw_cm.__enter__()
            stealth_note = " + stealth" if STEALTH_AVAILABLE else ""
            print(
                f"Playwright fallback enabled for {len(quirks.playwright_domains)} "
                f"domains (Cloudflare-gated + JS-SPA sites){stealth_note}."
            )
        except Exception as e:  # noqa: BLE001
            print(f"Playwright init failed: {type(e).__name__}: {e} — falling back to httpx only.")
            PW_FETCHER = None
            pw_cm = None
    else:
        if not PLAYWRIGHT_AVAILABLE:
            print("Playwright not installed — skipping JS fallback.")
        elif not quirks.playwright_domains:
            print("No playwright_domains in http_quirks.yaml — JS fallback is a no-op.")

    # curl_cffi (JA3 TLS impersonation) — stateless, zero-cost init.
    IMP_ALLOWLIST = ImpersonateAllowlist(quirks.impersonate_domains)
    if CURL_CFFI_AVAILABLE and quirks.impersonate_domains:
        try:
            IMP_FETCHER = ImpersonateFetcher(timeout=HTTP_TIMEOUT)
            print(
                f"curl_cffi impersonation enabled for "
                f"{len(quirks.impersonate_domains)} domains "
                f"(Chrome JA3 TLS)."
            )
        except Exception as e:  # noqa: BLE001
            print(f"curl_cffi init failed: {type(e).__name__}: {e} — skipping TLS impersonation.")
            IMP_FETCHER = None
    elif not CURL_CFFI_AVAILABLE:
        print("curl_cffi not installed — skipping TLS impersonation.")

    # Split-tunnel proxy (config/http_quirks.yaml). Mirrors the httpx client's
    # routing on the curl_cffi path. Empty proxy_url → no proxy (unchanged).
    PROXY_URL = quirks.proxy_url
    PROXY_DIRECT = quirks.proxy_direct
    if PROXY_URL:
        print(f"split-tunnel proxy ON: foreign -> {PROXY_URL}, direct for "
              f"{sorted(PROXY_DIRECT)} (e.g. .ru stay on the real local IP).")

    try:
        with make_client() as client:
            for i, u in enumerate(urls, start=1):
                if i in _ckpt_done:
                    continue  # persisted by the crashed run, restored from checkpoint
                if len(article_rows) >= MAX_ARTICLES:
                    print(
                        f"\nReached MAX_ARTICLES={MAX_ARTICLES}, stopping at "
                        f"source {i - 1}/{len(urls)}"
                    )
                    break
                _rows_before = len(article_rows)
                print(f"\n[{i}/{len(urls)}] {u}  (articles so far: {len(article_rows)})")
                try:
                    r = process_source(client, u, i, article_rows)
                except Exception as e:  # noqa: BLE001
                    traceback.print_exc()
                    r = SourceResult(url=u, error=f"crash: {e}"[:200])
                results.append(r)
                print(
                    f"   → type={r.detected_type:4}  http={r.http_status!s:3}  "
                    f"tried={r.articles_attempted:2}  news_like={r.news_like:2}  "
                    f"is_article={r.passed_is_article:2}  "
                    f"auto={r.passed_auto_topic:2}  elapsed={r.elapsed_ms} ms"
                )
                # Persist this source's rows so a crash resumes here, not from 0.
                # Append-only + fsync; any failure is swallowed (a checkpoint
                # must never break the run it is protecting).
                if FETCH_RESUME and _ckpt_fp is not None:
                    try:
                        fetch_checkpoint.append_source(
                            _ckpt_path, src_idx=i, source_result=asdict(r),
                            rows=[asdict(x) for x in article_rows[_rows_before:]])
                    except Exception:  # noqa: BLE001
                        pass
    finally:
        if pw_cm is not None:
            try:
                pw_cm.__exit__(None, None, None)
            except Exception as e:  # noqa: BLE001
                print(f"Playwright shutdown warning: {type(e).__name__}: {e}")
            PW_FETCHER = None

    total_ms = int((time.monotonic() - total_t0) * 1000)
    print(f"\nTotal: {total_ms} ms ({total_ms / 1000:.1f} s)")
    news_total = sum(r.news_like for r in results)
    print(f"News-like articles across all sources: {news_total}")

    # ------------------------------------------ LLM pass (certain + possible)
    llm_ran = False
    llm_aborted = ""
    if ENABLE_LLM and not args.no_llm:
        llm_ran = True
        try:
            _llm_stats = _run_llm_pass(article_rows, use_legacy=args.legacy_llm) or {}
            llm_aborted = _llm_stats.get("aborted", "")
        except BudgetExceeded as e:
            # The cap tripping mid-pass leaves the tail unclassified — same
            # truncated-feed hazard as I2. Health check below fails the run.
            print(f"\n!!! Бюджет LLM превышен: {e}", file=sys.stderr)
            llm_aborted = f"budget exceeded: {e}"
    elif args.no_llm:
        print(
            "\nLLM pass SKIPPED (--no-llm). 'Точно новость' / 'Возможно "
            "новость' rows will appear in the sheet WITHOUT EN/RU "
            "translation and section assignment. Use retry_failed_llm.py "
            "after API is restored to fill these in."
        )

    # ------------------------------------ Primary-source level 2 (corpus-based)
    _run_corpus_primary_source_pass(article_rows)
    # Official-press attribution for brand sales/financial figures (after the
    # corpus pass: official newsroom beats a corpus-earlier aggregator guess).
    _apply_brand_newsroom_primary(article_rows)

    # Persist the URL hashes + full classification into the SQLite cache
    # BEFORE the network writes. The cache store is local (no network, can't
    # SSL-fail) and is the expensive work's only checkpoint — storing it
    # first means a transient blip on the Sheets write no longer throws away
    # the whole classification: a re-run restores it from cache for ~$0.
    if DEDUP_STORE is not None:
        import json as _json
        entries = []
        for row in article_rows:
            if not row.article_url:
                continue
            # Don't persist rows that ended in a fetch/extract error — we
            # want the next run to retry them. Also don't persist the
            # published-archive verdict — it must be re-checked against the
            # FRESH daily archive each run, not frozen in the cache.
            if row.verdict in {
                "Отклонить (ошибка загрузки)",
                "Отклонить (не удалось извлечь)",
                "Отклонить (уже опубликовано редактором)",
                # jul-28: "дубль финального URL" is a WITHIN-RUN artefact —
                # the SAME url arriving from a second source (iz.ru root +
                # its rubric pages, auto.ru index + mag). The first copy is
                # the real one and may well be ACCEPTED and pushed; but both
                # copies share a url_hash, so persisting the duplicate
                # OVERWRITES the real row's verdict, event keys and LLM
                # fields. Measured: 810 such rows in one week, e.g. the
                # Maybach GLS story that WAS pushed and published while its
                # cache row read "дубль". Consequences: the next run's dedup
                # is blind to a story we already published (dup risk), the
                # paid classification is lost (re-classify cost), and any
                # analytics built on the cache under-counts the feed.
                "Отклонить (дубль финального URL)",
            }:
                continue
            # Don't persist accept-graded rows the LLM never classified
            # (aborted/errored pass, --no-llm run): an unclassified row is NOT
            # a final state. I2 froze 1000+ such rows stamped with the CURRENT
            # cls_ver — harmless to correctness (cache_is_authoritative refuses
            # them) but poisonous to forensics. Let the next run redo them.
            if (row.verdict in {"Точно новость", "Возможно новость"}
                    and not row.from_cache and not row.llm_relevance):
                continue
            canon_u = canonicalise(row.article_url)
            uh = url_hash(canon_u)
            # Strip the "из кэша" marker before persisting so a row never
            # ends up with the marker on its own saved snapshot.
            note_out = (row.llm_note or "").replace("из кэша", "").strip(" |").strip()
            cached_row = {
                # Classifier version that produced this classification.
                # A later run honours the row's rule-based verdict only if
                # this matches the live version (else it re-classifies).
                "cls_ver": CLASSIFIER_VERSION,
                "prompt_ver": PROMPT_VERSION,
                "heur_ver": HEURISTICS_VERSION,
                "verdict": row.verdict,
                "is_article": row.is_article,
                "article_score": row.article_score,
                "article_reasons": row.article_reasons[:300],
                "auto_topic": row.auto_topic,
                "auto_hits": row.auto_hits[:200],
                "llm_relevance": row.llm_relevance,
                "llm_section": row.llm_section,
                "llm_region": row.llm_region,
                "llm_confidence": row.llm_confidence,
                "llm_title_en": row.llm_title_en[:300],
                "llm_title_ru": row.llm_title_ru[:300],
                "llm_note": note_out,
                "llm_reason": (row.llm_reason or "")[:300],
                "primary_url": row.primary_url,
                "primary_domain": row.primary_domain,
                "primary_confidence": row.primary_confidence,
                "primary_method": row.primary_method,
                # Plan P3-D: persist brand+model so a later run can emit a
                # "we wrote about this model recently" advisory hint.
                "launch_brand_model": row.launch_brand_model,
                # Hybrid dedup Stage 1: persist the semantic event-key
                # so a later run / portal check can collapse the same
                # story without re-calling the LLM.
                "event_brand": row.event_brand,
                "event_model": row.event_model,
                "event_type": row.event_type,
            }
            entries.append((
                uh,
                canon_u,
                row.title[:500],
                row.published_at or None,
                domain_of(canon_u),
                DEDUP_PORTAL,
                _json.dumps(cached_row, ensure_ascii=False),
            ))
        DEDUP_STORE.mark_many_with_cache(entries)
        print(
            f"SQLite cache: +{len(entries)} rows stored with full classification "
            f"(next run will restore these without LLM)."
        )
        # The declaration has now done its job: the rows it protected are
        # re-stamped with the live prompt_ver. Consume it only after a
        # HEALTHY pass — an aborted run must keep it for the retry.
        if not llm_aborted:
            _consume_prompt_change()

    # Network writes — retried with backoff so a transient SSL/5xx blip on
    # the Sheets API doesn't discard the run (the classification is already
    # checkpointed in SQLite above, so even a hard failure here is cheap to
    # recover by re-running).
    _net_retry(write_report, svc, run_ts, results, report_tab,
               what="write_report")
    _net_retry(write_articles, svc, run_ts, article_rows, articles_tab,
               what="write_articles")
    print(f"Report written to tabs: {report_tab!r}, {articles_tab!r}")
    print(f"Detailed article rows: {len(article_rows)}")

    # Fetch rows are now durable in Sheets — drop the resume checkpoint so a
    # later crash (formatting / health / state-advance) can never double-write
    # them on the next run. Past this point the SQLite cache is the recovery
    # path, not the fetch checkpoint.
    if FETCH_RESUME and _ckpt_fp is not None:
        try:
            fetch_checkpoint.clear(_ckpt_path)
        except Exception:  # noqa: BLE001
            pass

    # Apply conditional formatting (colours + frozen header) to the new articles tab
    try:
        from apply_sheet_formatting import apply_formatting  # type: ignore[import-not-found]

        _net_retry(apply_formatting, svc, articles_tab, what="formatting")
        print(f"Conditional formatting applied to {articles_tab!r}")
    except Exception as e:  # noqa: BLE001
        print(f"(formatting step skipped: {e})", file=sys.stderr)

    # ---------------- Persist run state + per-run summary log ---------------
    # We only update state.json in scheduled-run mode (RUN_WINDOW set).
    # Ad-hoc / backfill runs (--no-window) leave the timestamp untouched
    # so they don't pollute the schedule.
    total_cost = sum(
        float(r.llm_cost_usd) for r in article_rows
        if isinstance(r.llm_cost_usd, (int, float))
    )
    rows_new = sum(1 for r in article_rows if not r.from_cache)
    elapsed_total_s = (time.monotonic() - total_t0)
    run_at_dt = datetime.fromisoformat(run_ts.replace("Z", "+00:00"))

    # -------------------- end-of-run health check (anti-silent-failure) -----
    alarms = _health_check(
        results, article_rows,
        llm_ran=llm_ran,
        llm_aborted=llm_aborted,
        dedup_enabled=not args.no_published_dedup,
        prev_state=state.load(),
    )

    # State window advances ONLY on a healthy run: a degraded run must be
    # re-covered by the next run (its unclassified rows re-fetch + re-classify;
    # the cache makes the healthy part cost ~$0).
    if RUN_WINDOW is not None and not alarms:
        try:
            state.update_success(
                run_at=run_at_dt,
                window_start=RUN_WINDOW.since,
                articles=len(article_rows),
                cost_usd=total_cost,
                extra={
                    # Baselines for the next run's health comparison + the
                    # explicit stage-handoff pointer (which tab this run wrote).
                    "archive_urls_count": len(PUBLISHED_URLS),
                    "archive_titles_count": len(PUBLISHED_TITLES),
                    "articles_tab": articles_tab,
                },
            )
            print(
                f"State saved: last_run_at={run_at_dt.isoformat(timespec='seconds')}, "
                f"articles={len(article_rows)}, cost=${total_cost:.4f}"
            )
        except Exception as e:  # noqa: BLE001
            # Don't fail the whole run if state write fails — the next run
            # will fall back to max_lookback, no news lost.
            print(
                f"WARNING: failed to write state file {args.state_path}: "
                f"{type(e).__name__}: {e}",
                file=sys.stderr,
            )
    elif RUN_WINDOW is not None:
        print("State NOT advanced (degraded run) — next run re-covers this window.")

    try:
        _append_run_log(
            args.runs_log,
            run_at=run_at_dt,
            window=RUN_WINDOW,
            rows_total=len(article_rows),
            rows_new=rows_new,
            cost_usd=total_cost,
            elapsed_s=elapsed_total_s,
            status="ok" if not alarms else ("degraded: " + "; ".join(alarms))[:200],
        )
        print(f"Run log appended: {args.runs_log}")
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: failed to append run log: {e}", file=sys.stderr)

    # LLM-abort auto-recovery hint. When the ONLY alarm is an aborted LLM pass
    # the fetch itself completed — classification is the sole missing piece, so
    # run_prog.ps1 can recover with retry_failed_llm instead of a ~1.5h full
    # re-fetch (jul-10: a transient API 403 cost a whole re-run). state.json
    # deliberately still points at the LAST healthy tab, so the aborted run's
    # tab travels via this hint file; retry consumes it and advances the state
    # itself on full success. Any other alarm class (archive floors, stuck
    # rows) is NOT retryable — no hint, chain stays aborted.
    if (alarms and RUN_WINDOW is not None and not args.hot
            and all(a.startswith("LLM pass aborted") for a in alarms)):
        hint_path = args.state_path.parent / "llm_abort_recovery.json"
        try:
            hint_path.write_text(json.dumps({
                "articles_tab": articles_tab,
                "run_at": run_at_dt.isoformat(timespec="seconds"),
                "window_start": RUN_WINDOW.since.isoformat(timespec="seconds"),
                "articles": len(article_rows),
                "cost_usd": round(total_cost, 4),
            }, ensure_ascii=False, indent=1), encoding="utf-8")
            print(f"Recovery hint written ({hint_path.name}): retry_failed_llm "
                  f"can finish tab '{articles_tab}' without a re-fetch.")
        except Exception as e:  # noqa: BLE001 — hint is best-effort
            print(f"WARNING: recovery hint write failed: {e}", file=sys.stderr)

    # Exit 3 = degraded: run_prog.ps1's Stage() aborts the chain on non-zero,
    # so a truncated/undeduped feed is never pushed silently.
    return 0 if not alarms else 3


if __name__ == "__main__":
    sys.exit(main())
