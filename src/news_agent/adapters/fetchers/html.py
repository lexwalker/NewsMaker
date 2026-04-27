"""HTML fetcher: index-page scraping + per-article extraction."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
from bs4.element import Tag

from news_agent.adapters.fetchers.base import RateLimiter, RobotsCache
from news_agent.core.models import RawArticle, Source
from news_agent.core.urls import canonicalise
from news_agent.logging_setup import get_logger

log = get_logger("fetch.html")


class HTMLFetcher:
    """Fetch article pages and extract body, image, outbound links.

    The index-page scraping strategy is intentionally simple: we take all
    `<a href>` that look like article permalinks, then fetch each. Sites with
    unusual layouts can be handled via `config/sources_overrides.yaml`
    (switching to RSS or adding `requires_js: true`).
    """

    def __init__(self, client: httpx.Client, rate: RateLimiter, robots: RobotsCache) -> None:
        self._client = client
        self._rate = rate
        self._robots = robots

    # ----------------------------------------------------------------- public
    def fetch(self, source: Source, max_items: int) -> list[RawArticle]:
        links = self._discover_article_links(source.url, max_items * 3)
        out: list[RawArticle] = []
        for link in links:
            if len(out) >= max_items:
                break
            art = self.fetch_single(
                link,
                source_name=source.name,
                source_url=source.url,
                source_language=source.language,
            )
            if art is not None:
                out.append(art)
        return out

    def fetch_single(
        self,
        url: str,
        *,
        source_name: str,
        source_url: str,
        source_language: str | None = None,
        fallback_title: str = "",
        fallback_published: datetime | None = None,
    ) -> RawArticle | None:
        if not self._robots.allowed(url):
            log.debug("html.robots_blocked", url=url)
            return None
        self._rate.wait(url)
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            log.debug("html.fetch_failed", url=url, error=str(e))
            return None
        html = resp.text

        return extract_article(
            html=html,
            url=str(resp.url),
            source_name=source_name,
            source_url=source_url,
            source_language=source_language,
            fallback_title=fallback_title,
            fallback_published=fallback_published,
        )

    # -------------------------------------------------------------- internal
    def _discover_article_links(self, index_url: str, limit: int) -> list[str]:
        if not self._robots.allowed(index_url):
            log.warning("html.robots_blocked", url=index_url)
            return []
        self._rate.wait(index_url)
        try:
            resp = self._client.get(index_url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            log.warning("html.index_fetch_failed", url=index_url, error=str(e))
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        host = urlparse(str(resp.url)).netloc
        seen: set[str] = set()
        out: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "").strip()
            if not href or href.startswith(("#", "mailto:", "javascript:")):
                continue
            absolute = urljoin(str(resp.url), href)
            p = urlparse(absolute)
            if p.netloc != host:
                continue
            if _looks_like_article(absolute):
                can = canonicalise(absolute)
                if can in seen:
                    continue
                seen.add(can)
                out.append(absolute)
                if len(out) >= limit:
                    break
        return out


# ---------------------------------------------------------------------- pure
def extract_article(
    *,
    html: str,
    url: str,
    source_name: str,
    source_url: str,
    source_language: str | None,
    fallback_title: str = "",
    fallback_published: datetime | None = None,
) -> RawArticle | None:
    """Pure function — tested directly against HTML fixtures."""
    soup = BeautifulSoup(html, "lxml")

    title = _pick_title(soup) or fallback_title
    if not title:
        return None

    body = _pick_body(html) or _fallback_body(soup)
    published = (
        _pick_published(soup)
        or _pick_published_from_url(url)
        or _pick_published_from_text(title, body)
        or _pick_published_trafilatura(html, url)
        or fallback_published
    )

    image_url, images = _pick_images(soup, url)
    outbound = _pick_outbound_links(soup, url)

    return RawArticle(
        url=url,
        title=title.strip(),
        body=body.strip(),
        html=html,
        published_at=published,
        image_url=image_url,
        images=images,
        outbound_links=outbound,
        source_name=source_name,
        source_url=source_url,
        source_language=source_language,
    )


# ----------------------------------------------------- extraction primitives
def _pick_title(soup: BeautifulSoup) -> str:
    for sel in [
        ('meta', {"property": "og:title"}),
        ('meta', {"name": "twitter:title"}),
    ]:
        tag = soup.find(sel[0], attrs=sel[1])
        if isinstance(tag, Tag) and tag.get("content"):
            return str(tag.get("content"))
    h1 = soup.find("h1")
    if isinstance(h1, Tag):
        return h1.get_text(strip=True)
    title_tag = soup.find("title")
    if isinstance(title_tag, Tag):
        return title_tag.get_text(strip=True)
    return ""


def _pick_body(html: str) -> str:
    try:
        import trafilatura

        text = trafilatura.extract(html, include_comments=False, include_tables=False) or ""
        return text
    except Exception:
        return ""


def _fallback_body(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()
    article = soup.find("article") or soup.find("main") or soup.body
    return article.get_text(" ", strip=True) if isinstance(article, Tag) else ""


def _pick_published(soup: BeautifulSoup) -> datetime | None:
    # 1. <meta property="article:published_time">
    for sel in [
        ("meta", {"property": "article:published_time"}),
        ("meta", {"name": "article:published_time"}),
        ("meta", {"itemprop": "datePublished"}),
        ("meta", {"property": "og:article:published_time"}),
        # Modified-time fallback: for news pages it's usually close enough
        # to the publication time, and saves ~15% of "no date" cases.
        ("meta", {"property": "article:modified_time"}),
        ("meta", {"name": "last-modified"}),
        ("meta", {"itemprop": "dateModified"}),
        # Site-specific tags some Russian portals use
        ("meta", {"name": "pubdate"}),
        ("meta", {"name": "publishdate"}),
        ("meta", {"name": "publish_date"}),
        ("meta", {"name": "date"}),
    ]:
        tag = soup.find(sel[0], attrs=sel[1])
        if isinstance(tag, Tag) and tag.get("content"):
            dt = _parse_dt(str(tag.get("content")))
            if dt:
                return dt
    # 2. JSON-LD NewsArticle
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.get_text() or "null")
        except (ValueError, TypeError):
            continue
        dt = _extract_jsonld_date(data)
        if dt:
            return dt
    # 3. <time datetime="...">
    for t in soup.find_all("time"):
        if isinstance(t, Tag):
            dt_attr = t.get("datetime") or t.get("pubdate")
            if dt_attr:
                dt = _parse_dt(str(dt_attr))
                if dt:
                    return dt
    return None


# ----- URL-pattern date extraction ----------------------------------------
# Many news sites embed the publish date in the URL path, e.g.:
#   /2026/04/27/article-slug
#   /news/2026-04-27-something
#   /20260427/article  (russian.news.cn)
_URL_DATE_PATTERNS = (
    re.compile(r"/(\d{4})/(\d{1,2})/(\d{1,2})(?:/|$)"),
    re.compile(r"[/_-](\d{4})-(\d{1,2})-(\d{1,2})(?:[/_.-]|$)"),
    re.compile(r"/(\d{4})(\d{2})(\d{2})/"),  # /20260427/
)


def _pick_published_from_url(url: str) -> datetime | None:
    parsed = urlparse(url)
    path = parsed.path
    for rx in _URL_DATE_PATTERNS:
        m = rx.search(path)
        if not m:
            continue
        try:
            y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        except (ValueError, IndexError):
            continue
        # Sanity: 2015-2030, valid month/day
        if not (2015 <= y <= 2030 and 1 <= mth <= 12 and 1 <= d <= 31):
            continue
        try:
            return datetime(y, mth, d, tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# ----- Body / title text date extraction ----------------------------------
_RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}
_EN_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
_RU_DATE_RE = re.compile(
    r"\b(\d{1,2})\s+(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)\s+(\d{4})\b",
    re.IGNORECASE,
)
_EN_DATE_RE_1 = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+(\d{1,2}),?\s+(\d{4})\b",
    re.IGNORECASE,
)
_EN_DATE_RE_2 = re.compile(
    r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December|"
    r"Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?\s+(\d{4})\b",
    re.IGNORECASE,
)
_NUMERIC_DATE_RE = re.compile(r"\b(\d{1,2})[./](\d{1,2})[./](\d{4})\b")
_ISO_DATE_RE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")


def _pick_published_from_text(title: str, body: str) -> datetime | None:
    """Scan title + first 1500 chars of body for a date phrase.

    Looks for Russian ("27 апреля 2026"), English ("April 27, 2026" or
    "27 April 2026"), numeric ("27.04.2026", "27/04/2026") and ISO
    ("2026-04-27") formats. Returns the FIRST match — most articles put
    the publish date near the headline / dateline.
    """
    text = (title or "") + "\n" + (body or "")[:1500]

    m = _RU_DATE_RE.search(text)
    if m:
        try:
            d, mth_word, y = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            mth = _RU_MONTHS.get(mth_word)
            if mth and 2015 <= y <= 2030:
                return datetime(y, mth, d, tzinfo=timezone.utc)
        except (ValueError, KeyError):
            pass

    for rx in (_EN_DATE_RE_1, _EN_DATE_RE_2):
        m = rx.search(text)
        if m:
            try:
                groups = m.groups()
                # _EN_DATE_RE_1: month, day, year
                # _EN_DATE_RE_2: day, month, year
                if rx is _EN_DATE_RE_1:
                    mth_word, d, y = groups[0].lower(), int(groups[1]), int(groups[2])
                else:
                    d, mth_word, y = int(groups[0]), groups[1].lower(), int(groups[2])
                mth = _EN_MONTHS.get(mth_word.replace(".", ""))
                if mth and 2015 <= y <= 2030 and 1 <= d <= 31:
                    return datetime(y, mth, d, tzinfo=timezone.utc)
            except (ValueError, KeyError):
                pass

    m = _NUMERIC_DATE_RE.search(text)
    if m:
        try:
            d, mth, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 2015 <= y <= 2030 and 1 <= mth <= 12 and 1 <= d <= 31:
                return datetime(y, mth, d, tzinfo=timezone.utc)
        except ValueError:
            pass

    m = _ISO_DATE_RE.search(text)
    if m:
        try:
            y, mth, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 2015 <= y <= 2030 and 1 <= mth <= 12 and 1 <= d <= 31:
                return datetime(y, mth, d, tzinfo=timezone.utc)
        except ValueError:
            pass

    return None


def _pick_published_trafilatura(html: str, url: str) -> datetime | None:
    """Last-resort fallback to trafilatura's metadata heuristics — it
    knows tricks beyond what we hand-coded (Schema.org variants, embedded
    dateline conventions, OpenGraph fallbacks, language-aware patterns).
    """
    try:
        from trafilatura.metadata import extract_metadata  # type: ignore[import-not-found]
        meta = extract_metadata(html, default_url=url)
        if meta and getattr(meta, "date", None):
            return _parse_dt(str(meta.date))
    except Exception:  # noqa: BLE001
        return None
    return None


def _extract_jsonld_date(data: Any) -> datetime | None:
    if isinstance(data, list):
        for item in data:
            dt = _extract_jsonld_date(item)
            if dt:
                return dt
    elif isinstance(data, dict):
        for key in ("datePublished", "dateCreated", "uploadDate"):
            val = data.get(key)
            if isinstance(val, str):
                dt = _parse_dt(val)
                if dt:
                    return dt
        for v in data.values():
            if isinstance(v, (list, dict)):
                dt = _extract_jsonld_date(v)
                if dt:
                    return dt
    return None


def _parse_dt(raw: str) -> datetime | None:
    raw = raw.strip()
    if not raw:
        return None
    # ISO 8601 with 'Z' or +offset
    try:
        s = raw
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass
    # Common alternative formats encountered on news sites
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%a, %d %b %Y %H:%M:%S %z",   # RFC 2822 (Last-Modified header)
        "%a, %d %b %Y %H:%M:%S GMT",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _pick_images(soup: BeautifulSoup, page_url: str) -> tuple[str | None, list[str]]:
    primary: str | None = None
    for sel in [
        ("meta", {"property": "og:image"}),
        ("meta", {"name": "twitter:image"}),
        ("meta", {"property": "og:image:url"}),
    ]:
        tag = soup.find(sel[0], attrs=sel[1])
        if isinstance(tag, Tag) and tag.get("content"):
            primary = urljoin(page_url, str(tag.get("content")))
            break

    inline: list[str] = []
    article = soup.find("article") or soup.find("main") or soup
    if isinstance(article, Tag):
        for img in article.find_all("img", src=True):
            src = urljoin(page_url, img.get("src", ""))
            w = img.get("width")
            try:
                w_int = int(str(w)) if w else 0
            except ValueError:
                w_int = 0
            if (w_int == 0 or w_int >= 400) and src not in inline:
                inline.append(src)

    if primary is None and inline:
        primary = inline[0]
    return primary, inline


def _pick_outbound_links(soup: BeautifulSoup, page_url: str) -> list[str]:
    host = urlparse(page_url).netloc
    out: list[str] = []
    seen: set[str] = set()
    article = soup.find("article") or soup.find("main") or soup
    if not isinstance(article, Tag):
        return out
    for a in article.find_all("a", href=True):
        absolute = urljoin(page_url, str(a.get("href")))
        p = urlparse(absolute)
        if p.scheme not in ("http", "https"):
            continue
        if p.netloc == host or not p.netloc:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        out.append(absolute)
    return out


_ARTICLE_HINTS = ("/news/", "/article/", "/post/", "/20", "/story/", "-news-", ".html")


def _looks_like_article(url: str) -> bool:
    p = urlparse(url)
    path = p.path.lower()
    if not path or path == "/":
        return False
    if any(seg in path for seg in _ARTICLE_HINTS):
        return True
    # Long slug with hyphens → plausibly an article
    last = path.rstrip("/").split("/")[-1]
    return last.count("-") >= 3
