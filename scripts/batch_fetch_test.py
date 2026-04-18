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
load_dotenv(ROOT / ".env")

from news_agent.adapters.fetchers.html import (  # noqa: E402
    extract_article,
    _looks_like_article,
)
from news_agent.adapters.fetchers.telegram import (  # noqa: E402
    is_telegram_url,
    parse_channel_html,
    to_channel_preview_url,
)
from news_agent.core.config_loader import load_whitelist_domains  # noqa: E402
from news_agent.core.heuristic_relevance import (  # noqa: E402
    grade_article,
    is_auto_or_economy,
    looks_like_article,
)
from news_agent.core.urls import canonicalise, domain_of  # noqa: E402

# ----------------------------------------------------------------- config
NUM_SOURCES = 100         # number of source URLs to process end-to-end
MAX_ARTICLES = 10_000     # soft cap; effectively disabled for 100-source runs
ITEMS_PER_SOURCE = 5
HTTP_TIMEOUT = 10.0

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
    is_article: bool = False
    article_score: float = 0.0
    article_reasons: str = ""
    auto_topic: bool = False
    auto_hits: str = ""
    verdict: str = ""  # "Отправить в LLM" | "Отклонить"


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
        if active == "1" and url.startswith(("http://", "https://")):
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


ARTICLES_HEADER = [
    "Прогон (UTC)",
    "№ ист.",
    "URL источника",
    "№ ст.",
    "URL статьи",
    "Заголовок",
    "Дата публикации",
    "Тело (симв)",
    "Картинка",
    "is_article",
    "Score",
    "Причины is_article",
    "Авто/эконом",
    "Hits темы",
    "Итог бота",
    "Ручная проверка (впишите: Новость / Не новость)",
    "Комментарий",
]


def write_articles(svc, run_ts: str, rows: list[ArticleRow], tab: str) -> None:  # type: ignore[no-untyped-def]
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=f"'{tab}'"
    ).execute()
    out = [ARTICLES_HEADER]
    for r in rows:
        out.append(
            [
                run_ts,
                r.source_idx,
                r.source_url,
                r.article_idx,
                r.article_url,
                r.title[:300],
                r.published_at,
                r.body_len,
                "да" if r.has_image else "",
                "Новость" if r.is_article else "Не новость",
                r.article_score,
                r.article_reasons[:400],
                "Авто/эконом" if r.auto_topic else "Нет",
                r.auto_hits[:200],
                r.verdict,
                "",  # manual check column — fill in by the editor
                "",  # free-form comment
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
def make_client() -> httpx.Client:
    return httpx.Client(
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.9",
        },
        follow_redirects=True,
        timeout=HTTP_TIMEOUT,
    )


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
            resp = client.get(preview)
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
            _score_article(art, r, row)
            article_rows.append(row)
        r.elapsed_ms = int((time.monotonic() - t0) * 1000)
        return r

    try:
        resp = client.get(url)
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
        resp = client.get(link)
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

    _score_article(article, r, row)
    article_rows.append(row)


def _score_article(article, r: SourceResult, row: ArticleRow) -> None:  # type: ignore[no-untyped-def]
    """Populate heuristic fields on ``row`` from ``article``; update source counters."""
    row.article_url = article.url
    row.title = article.title
    row.body_len = len(article.body)
    row.published_at = article.published_at.isoformat() if article.published_at else ""
    row.has_image = bool(article.image_url)

    # Final-URL deduplication. Using canonicalised URL handles trailing slashes,
    # UTM params and similar differences that hide the same page behind many
    # referring links (see the benchmarkminerals.com × 5 case from v1).
    canon = canonicalise(article.url)
    if canon in SEEN_FINAL_URLS:
        row.article_reasons = "duplicate-final-url"
        row.verdict = "Отклонить (дубль финального URL)"
        return
    SEEN_FINAL_URLS.add(canon)

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
    global WHITELIST, SEEN_FINAL_URLS
    WHITELIST = load_whitelist_domains()
    SEEN_FINAL_URLS = set()
    print(f"Whitelist domains loaded: {len(WHITELIST)}")

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
    with make_client() as client:
        for i, u in enumerate(urls, start=1):
            if len(article_rows) >= MAX_ARTICLES:
                print(f"\nReached MAX_ARTICLES={MAX_ARTICLES}, stopping at source {i - 1}/{len(urls)}")
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
    total_ms = int((time.monotonic() - total_t0) * 1000)
    print(f"\nTotal: {total_ms} ms ({total_ms / 1000:.1f} s)")
    news_total = sum(r.news_like for r in results)
    print(f"News-like articles across all sources: {news_total}")

    write_report(svc, run_ts, results, report_tab)
    write_articles(svc, run_ts, article_rows, articles_tab)
    print(f"Report written to tabs: {report_tab!r}, {articles_tab!r}")
    print(f"Detailed article rows: {len(article_rows)}")

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
