"""HTML fetcher: index-page scraping + per-article extraction."""

from __future__ import annotations

import json
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
    published = _pick_published(soup) or fallback_published

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
    t = soup.find("time")
    if isinstance(t, Tag) and t.get("datetime"):
        return _parse_dt(str(t.get("datetime")))
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
    # ISO 8601 with 'Z'
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
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
