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

import io
import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

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
    _looks_like_article,
)
from news_agent.adapters.fetchers.telegram import (  # noqa: E402
    is_telegram_url,
    parse_channel_html,
    to_channel_preview_url,
)
from news_agent.adapters.fetchers.base import make_http_client  # noqa: E402
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
    load_blacklist,
    load_brand_domains,
    load_http_quirks,
    load_primary_source_cues,
    load_whitelist_domains,
)
from news_agent.core.freshness import is_fresh  # noqa: E402
from news_agent.core.primary_source import (  # noqa: E402
    CorpusEntry,
    detect_earliest_in_corpus,
    detect_primary_source,
)
from news_agent.core.urls import url_hash  # noqa: E402
from news_agent.core.heuristic_relevance import (  # noqa: E402
    blacklist_hit,
    grade_article,
    is_auto_or_economy,
    looks_like_article,
)
from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.adapters.llm.base import LLMClient  # noqa: E402
from news_agent.adapters.storage import DedupStore  # noqa: E402
from news_agent.core.budget import BudgetExceeded, BudgetTracker  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.core.urls import canonicalise, domain_of  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

# ----------------------------------------------------------------- config
NUM_SOURCES = 400         # effectively all active sources (there are ~357 flagged "1")
MAX_ARTICLES = 10_000     # soft cap
ITEMS_PER_SOURCE = 15  # was 5 — active news sites (motortrend, iz.ru,
                       # carnewschina) publish 10-20 articles/day. Cap of 5
                       # was clipping real coverage. Downside: fetch time
                       # goes up ~2x and LLM cost scales linearly.
HTTP_TIMEOUT = 20.0  # was 10.0 — several slow-but-alive sources (gov.ru, OEM
                     # press rooms) need more patience. Retries are still
                     # DISABLED on timeouts, so this just widens the window once.
ENABLE_LLM = True         # flip to False to run pure heuristics again
LLM_BUDGET_USD = 5.0      # hard cap — abort LLM calls if exceeded
FRESHNESS_HOURS = int(os.environ.get("FRESHNESS_HOURS", "24"))  # drop articles older than this
SQLITE_PATH = Path(os.environ.get("SQLITE_PATH", "./data/news_agent.sqlite"))

# Real Telegram channels that the editor uses (from 'Новости опубликованные').
# We prepend them to the source list so the test run actually exercises the
# Telegram adapter end-to-end, not just the HTML/RSS paths.
TELEGRAM_SEED_URLS = [
    "https://t.me/chinamashina_news",
    "https://t.me/sergtselikov",
    "https://t.me/autopotoknews",
]
USER_AGENT = os.environ.get("USER_AGENT", "NewsMakerBot/0.1 (+test)")
REPORT_TAB_BASE = "ТЕСТ прогон"
ARTICLES_TAB_BASE = "ТЕСТ статьи"

# Populated in main(). Global so HTML + Telegram branches share the same
# final-URL deduplication set.
WHITELIST: set[str] = set()
SEEN_FINAL_URLS: set[str] = set()
BLACKLIST = None  # type: ignore[assignment]  # set to a Blacklist instance in main()
BRANDS: list = []  # type: ignore[type-arg]  # BrandDomainEntry list from config
DEDUP_STORE = None  # type: ignore[assignment]  # DedupStore instance in main()
# url_hash → cached classification fields from earlier runs (SQLite).
# A row whose hash is here doesn't go through the LLM again — instead its
# cached verdict / section / titles / primary source get copied back.
PREVIOUSLY_SEEN: dict[str, dict] = {}
DEDUP_PORTAL = "RU"  # current batch treats every source as RU portal
PRIMARY_CUES = None  # type: ignore[assignment]  # PrimarySourceCues from config
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
    primary_method: str = ""      # "body-link" / "corpus-earlier" / "self"
    # Reconstructed from SQLite cache on a repeat run — skip the LLM pass.
    from_cache: bool = False


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
    for row in rows[1:]:  # skip header
        if not row:
            continue
        active = (row[0] if len(row) > 0 else "").strip()
        url = (row[1] if len(row) > 1 else "").strip()
        # Accept anything that is NOT explicitly "0" (flags "1", "2", empty
        # are all considered active). Editor has three flags in the sheet:
        #   1 — daily-monitored automotive sources
        #   2 — OEM pressrooms and IR pages (secondary)
        #   (empty) — unmarked, but URL is valid
        if active != "0" and url.startswith(("http://", "https://")):
            out.append(url)
        if len(out) >= limit:
            break
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
# Block 1 (light green):  "Что за новость"        — columns A–I
# Block 2 (light blue):   "Первоисточник"         — columns J–L
# Block 3 (light yellow): "Для редактора"          — columns M–Q
# Block 4 (light grey):   "Отладка"  (hidden)     — columns R–AB
ARTICLES_HEADER = [
    # --- Block 1: «Что за новость» --------------------------------------
    "Прогон (UTC)",                    # A
    "Заголовок (EN / RU)",             # B  combined EN+RU (+ source lang tag)
    "Лид",                             # C  first ~300 chars of body
    "URL статьи",                      # D
    "Раздел",                          # E  LLM section (+ "(неактивный)" for Test-drive)
    "Регион",                          # F  Local / Global
    "Страна",                          # G  Russia / Uzbekistan / Kazakhstan
    "Дата публикации",                 # H
    "Картинка (URL)",                  # I  full image URL, not just a flag
    # --- Block 2: «Первоисточник» ---------------------------------------
    "Первоисточник (домен)",           # J
    "Первоисточник URL",               # K
    "Уверенность источника",           # L  high / medium / low
    # --- Block 3: «Для редактора» ---------------------------------------
    "Пометка бота",                    # M
    "Confidence раздела",              # N  0.00-1.00
    "Итог бота",                       # O  ← colour formatting column
    "Ручная проверка (Новость / Не новость)",  # P
    "Комментарий",                     # Q
    # --- Block 4: «Отладка» (hidden by default) --------------------------
    "URL источника",                   # R
    "№ ист.",                          # S
    "№ ст.",                           # T
    "Тело (симв)",                     # U
    "is_article",                      # V
    "is_article score",                # W
    "Причины is_article",              # X
    "Hits темы",                       # Y
    "LLM relevance",                   # Z
    "Стоимость LLM, $",                # AA
    "Способ поиска источника",         # AB
]

# Portal → country label (visible in the sheet) + numeric code (kept for
# future CMS integration — per the task spec RU=7, UZ=608, KZ=14).
PORTAL_COUNTRY: dict[str, tuple[str, int]] = {
    "RU": ("Russia", 7),
    "UZ": ("Uzbekistan", 608),
    "KZ": ("Kazakhstan", 14),
}

# Source language → (EN-title tag, RU-title tag), matching the format the
# editorial team uses in "Новости опубликованные".
#   EN line gets the ISO code in Latin uppercase: (EN), (RU), (DE)...
#   RU line gets a 3-letter Russian abbreviation: (АНГЛ), (РУС), (ДЕ)...
# Mapping verified against 2,817 rows of "Новости опубликованные":
# EN tag uses the Latin ISO-639-1 code, RU tag uses a Russian 3-4 letter
# abbreviation — these are the exact strings the editor uses, not guesses.
_LANG_TAG_MAP: dict[str, tuple[str, str]] = {
    "en": ("EN", "АНГЛ"),
    "ru": ("RU", "РУС"),
    "de": ("DE", "НЕМ"),      # editor uses НЕМ, not ДЕ
    "fr": ("FR", "ФР"),
    "it": ("IT", "ИТАЛ"),     # editor uses ИТАЛ, not ИТ
    "es": ("ES", "ИСП"),
    "zh": ("ZH", "КИТ"),
    "ja": ("JA", "ЯП"),
    "ko": ("KO", "КОР"),
    "pl": ("PL", "ПОЛ"),
    "pt": ("PT", "ПОР"),
    "nl": ("NL", "НИД"),
    "cs": ("CS", "ЧЕШ"),
    "tr": ("TR", "ТУР"),
    "uk": ("UK", "УКР"),
}


def _lang_tags_for(lang: str) -> tuple[str, str]:
    """Return (EN-tag, RU-tag) for a given ISO-639-1 language code.

    Falls back to the uppercase code itself if we don't have a specific
    Russian abbreviation for that language.
    """
    if not lang:
        return ("", "")
    key = lang.strip().lower()[:2]
    if key in _LANG_TAG_MAP:
        return _LANG_TAG_MAP[key]
    up = key.upper()
    return (up, up)


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
            ]
        )
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{tab}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": out},
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
        body={"values": rows},
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


def _http_get(client, url: str):
    """Route ``url`` through the right backend based on allowlist membership.

    Priority:
      1. Playwright — only when JS rendering is required (SPA / CF challenge)
      2. curl_cffi  — fast TLS-impersonation for Cloudflare/Akamai 403
      3. httpx      — the default for everything else

    Keeps the caller's existing code untouched — returns a duck-typed object
    with the same three attributes ``process_source`` needs.
    """
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
            status, html = IMP_FETCHER.fetch(url)
            return _PwResponse(url, status, html)
        except Exception as e:  # noqa: BLE001
            print(f"   ! curl_cffi failed on {url}: {type(e).__name__}: {str(e)[:80]}")

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
def process_source(
    client: httpx.Client, url: str, source_idx: int, article_rows: list[ArticleRow]
) -> SourceResult:
    r = SourceResult(url=url)
    t0 = time.monotonic()

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
            max_items=ITEMS_PER_SOURCE,
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

    # Check whether the page is itself an RSS/Atom feed
    head_lower = html[:1000].lower()
    if any(t in content_type for t in ("rss", "atom", "xml")) and (
        "<rss" in head_lower or "<feed" in head_lower
    ):
        r.detected_type = "rss"
        r.feed_url = url
        entries = feedparser.parse(html).entries[:ITEMS_PER_SOURCE]
        r.articles_attempted = len(entries)
        _fill_from_rss_entries(client, entries, r, source_idx, article_rows)
        r.elapsed_ms = int((time.monotonic() - t0) * 1000)
        return r

    feed_url = discover_feed(client, url, html)
    if feed_url:
        try:
            rss_resp = client.get(feed_url)
            rss_resp.raise_for_status()
            entries = feedparser.parse(rss_resp.content).entries[:ITEMS_PER_SOURCE]
            if entries:
                r.detected_type = "rss"
                r.feed_url = feed_url
                r.articles_attempted = len(entries)
                _fill_from_rss_entries(client, entries, r, source_idx, article_rows)
                r.elapsed_ms = int((time.monotonic() - t0) * 1000)
                return r
        except httpx.HTTPError:
            pass  # fall through to HTML mode

    # HTML index mode
    r.detected_type = "html"
    links = _discover_article_links(url, html, ITEMS_PER_SOURCE)
    r.articles_attempted = len(links)
    for idx, link in enumerate(links, start=1):
        _fetch_and_score(client, link, r, source_idx, idx, article_rows)
    r.elapsed_ms = int((time.monotonic() - t0) * 1000)
    return r


def _fill_from_rss_entries(  # type: ignore[no-untyped-def]
    client: httpx.Client,
    entries,
    result: SourceResult,
    source_idx: int,
    article_rows: list[ArticleRow],
) -> None:
    for idx, entry in enumerate(entries, start=1):
        link = (entry.get("link") or "").strip()
        if not link:
            continue
        _fetch_and_score(client, link, result, source_idx, idx, article_rows)


def _run_llm_pass(article_rows: list[ArticleRow]) -> None:
    """For every certain/possible row: run LLM relevance (possible only), then
    classify_section + translate_title. Populates llm_* fields in-place.

    Stops early on budget exhaustion (raises BudgetExceeded)."""
    settings = get_settings()
    client: LLMClient = make_llm_client(settings)
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
        return

    print(
        f"\nLLM pass: {len(candidates)} свежих кандидатов "
        f"(из кэша без LLM: {cached_count}, "
        f"certain={sum(1 for r in candidates if r.verdict == 'Точно новость')}, "
        f"possible={sum(1 for r in candidates if r.verdict == 'Возможно новость')})"
    )
    print(f"  provider: {client.provider_name}  model: {client.model}  cap: ${LLM_BUDGET_USD}")

    country = "Russia"  # portal country for now; будет параметром позже
    section_names = {s.name for s in sections}

    for i, r in enumerate(candidates, start=1):
        # 1. Cheap relevance check for 'possible' rows
        if r.verdict == "Возможно новость":
            try:
                rel, u = client.is_automotive(r.title, r.body_excerpt or r.title)
                budget.record(u)
                r.llm_relevance = "Да" if rel.is_automotive_or_economy else "Нет"
                r.llm_cost_usd = round((r.llm_cost_usd or 0) + u.cost_usd, 5)
            except Exception as e:  # noqa: BLE001
                r.llm_note = f"relevance error: {e!s:100}"
                continue
            if not rel.is_automotive_or_economy:
                # drop — not auto/economy; no classify/translate needed
                r.llm_note = (r.llm_note + " | " if r.llm_note else "") + "LLM: не авто/эконом"
                # OVERWRITE verdict so the editor sees the LLM decision —
                # "Возможно новость" → "Отклонено LLM"
                r.verdict = "Отклонено LLM"
                print(f"  [{i}/{len(candidates)}] {r.title[:60]!r} → relevance=Нет (отсечено)")
                continue
        else:
            # certain — implicitly relevant
            r.llm_relevance = "Да"

        # 2. Classify section
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
                f"classify error: {e!s:100}"
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
                f"translate error: {e!s:100}"

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


def _fetch_and_score(
    client: httpx.Client,
    link: str,
    r: SourceResult,
    source_idx: int,
    article_idx: int,
    article_rows: list[ArticleRow],
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

    if _score_article(article, r, row):
        article_rows.append(row)


def _score_article(article, r: SourceResult, row: ArticleRow) -> bool:  # type: ignore[no-untyped-def]
    """Populate heuristic fields on ``row`` from ``article``; update source counters.

    Returns ``False`` if the row should NOT be written to the output sheet
    (currently: articles older than FRESHNESS_HOURS — they never go to LLM
    and clutter the report with no value).
    """
    row.article_url = article.url
    row.title = article.title
    row.body_len = len(article.body)
    row.body_excerpt = article.body[:1000]  # ≤1000 chars → ~40% token saving on LLM input
    row.published_at = article.published_at.isoformat() if article.published_at else ""
    row.has_image = bool(article.image_url)
    row.image_url = article.image_url or ""
    row.source_language = (article.source_language or "").lower()[:2]

    # --- Freshness gate ------------------------------------------------------
    # If the article has a known publication timestamp and it's older than
    # FRESHNESS_HOURS, short-circuit: no heuristic score, no LLM call.
    # Articles without any timestamp pass through (many t.me posts do expose
    # one; HTML fetchers usually find og:article:published_time).
    if article.published_at is not None and not is_fresh(
        article.published_at, hours=FRESHNESS_HOURS
    ):
        # Skip entirely — do not write to Sheets, don't run LLM.
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

    # Final-URL deduplication. Using canonicalised URL handles trailing slashes,
    # UTM params and similar differences that hide the same page behind many
    # referring links (see the benchmarkminerals.com × 5 case from v1).
    canon = canonicalise(article.url)
    if canon in SEEN_FINAL_URLS:
        row.article_reasons = "duplicate-final-url"
        row.verdict = "Отклонить (дубль финального URL)"
        return True
    SEEN_FINAL_URLS.add(canon)

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
        # Verdicts where LLM is never invoked (heuristic-only). These are
        # safe to restore even without llm_section.
        _heuristic_only_verdicts = {
            "Точно не новость (не статья)",
            "Точно не новость (не авто)",
            "Точно не новость (старая)",
            "Точно не новость (чёрный список)",
            "Отклонить (дубль)",
            "Отклонить (дубль финального URL)",
            "Отклонить (обработан ранее)",
        }
        cache_is_authoritative = (
            has_llm_classification or cached_verdict in _heuristic_only_verdicts
        )
        if cache_is_authoritative:
            row.verdict = cached_verdict or row.verdict
            row.llm_relevance = cached.get("llm_relevance", "")
            row.llm_section = cached.get("llm_section", "")
            row.llm_region = cached.get("llm_region", "")
            row.llm_confidence = cached.get("llm_confidence", "")
            row.llm_title_en = cached.get("llm_title_en", "")
            row.llm_title_ru = cached.get("llm_title_ru", "")
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
            row.article_score = cached.get("article_score", row.article_score)
            row.article_reasons = cached.get("article_reasons", "from-cache")
            row.is_article = bool(cached.get("is_article", True))
            row.auto_topic = bool(cached.get("auto_topic", True))
            row.auto_hits = cached.get("auto_hits", "")
            row.from_cache = True
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
        )
        row.primary_url = p_url
        row.primary_domain = p_dom
        row.primary_confidence = p_conf
        row.primary_method = "body-link" if p_conf != "low" else "self"
    return True


def _discover_article_links(index_url: str, html: str, limit: int) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    host = urlparse(index_url).netloc
    seen: set[str] = set()
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        absolute = urljoin(index_url, href)
        p = urlparse(absolute)
        if p.netloc != host:
            continue
        if _looks_like_article(absolute) and absolute not in seen:
            seen.add(absolute)
            out.append(absolute)
            if len(out) >= limit:
                break
    return out


# ------------------------------------------------------------- main
def main() -> int:
    global WHITELIST, SEEN_FINAL_URLS, BLACKLIST, BRANDS, DEDUP_STORE, PREVIOUSLY_SEEN
    global PRIMARY_CUES, PW_FETCHER, PW_ALLOWLIST, IMP_FETCHER, IMP_ALLOWLIST
    WHITELIST = load_whitelist_domains()
    BLACKLIST = load_blacklist()
    BRANDS = load_brand_domains()
    PRIMARY_CUES = load_primary_source_cues()
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
    print(
        f"SQLite cache loaded: {len(PREVIOUSLY_SEEN)} classified url_hashes "
        f"for portal={DEDUP_PORTAL} ({SQLITE_PATH}). "
        f"These will be reconstructed without LLM calls."
    )

    svc = sheets_client()
    urls = TELEGRAM_SEED_URLS + read_active_sources(svc, NUM_SOURCES)
    if not urls:
        print("No active sources found in the sheet.", file=sys.stderr)
        return 2

    report_tab, articles_tab = allocate_new_tabs(svc)
    print(f"New tabs allocated for this run:")
    print(f"  summary  : {report_tab}")
    print(f"  articles : {articles_tab}")

    run_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"Run: {run_ts}")
    print(f"Sources to test: {len(urls)}")
    for i, u in enumerate(urls, start=1):
        print(f"  {i:2d}. {u}")

    results: list[SourceResult] = []
    article_rows: list[ArticleRow] = []
    total_t0 = time.monotonic()

    # Playwright context — only spun up if the allowlist has entries AND the
    # library is importable. The fetch path gracefully degrades to httpx if
    # Playwright crashes mid-run (see `_http_get`).
    quirks = load_http_quirks()
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

    try:
        with make_client() as client:
            for i, u in enumerate(urls, start=1):
                if len(article_rows) >= MAX_ARTICLES:
                    print(
                        f"\nReached MAX_ARTICLES={MAX_ARTICLES}, stopping at "
                        f"source {i - 1}/{len(urls)}"
                    )
                    break
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
    if ENABLE_LLM:
        try:
            _run_llm_pass(article_rows)
        except BudgetExceeded as e:
            print(f"\n!!! Бюджет LLM превышен: {e}", file=sys.stderr)

    # ------------------------------------ Primary-source level 2 (corpus-based)
    _run_corpus_primary_source_pass(article_rows)

    write_report(svc, run_ts, results, report_tab)
    write_articles(svc, run_ts, article_rows, articles_tab)
    print(f"Report written to tabs: {report_tab!r}, {articles_tab!r}")
    print(f"Detailed article rows: {len(article_rows)}")

    # Persist the URL hashes + full classification into the SQLite cache.
    # Next run will see the hash and restore these fields without any LLM
    # call → cost drops to cents on daily delta-runs.
    if DEDUP_STORE is not None:
        import json as _json
        entries = []
        for row in article_rows:
            if not row.article_url:
                continue
            # Don't persist rows that ended in a fetch/extract error — we
            # want the next run to retry them.
            if row.verdict in {
                "Отклонить (ошибка загрузки)",
                "Отклонить (не удалось извлечь)",
            }:
                continue
            canon_u = canonicalise(row.article_url)
            uh = url_hash(canon_u)
            # Strip the "из кэша" marker before persisting so a row never
            # ends up with the marker on its own saved snapshot.
            note_out = (row.llm_note or "").replace("из кэша", "").strip(" |").strip()
            cached_row = {
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
                "primary_url": row.primary_url,
                "primary_domain": row.primary_domain,
                "primary_confidence": row.primary_confidence,
                "primary_method": row.primary_method,
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

    # Apply conditional formatting (colours + frozen header) to the new articles tab
    try:
        from apply_sheet_formatting import apply_formatting  # type: ignore[import-not-found]

        apply_formatting(svc, articles_tab)
        print(f"Conditional formatting applied to {articles_tab!r}")
    except Exception as e:  # noqa: BLE001
        print(f"(formatting step skipped: {e})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
