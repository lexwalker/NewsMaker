"""RSS / Atom fetcher."""

from __future__ import annotations

import time as _time
from calendar import timegm
from datetime import datetime, timezone
from typing import Any

import feedparser
import httpx

from news_agent.adapters.fetchers.base import RateLimiter, RobotsCache
from news_agent.adapters.fetchers.html import HTMLFetcher
from news_agent.core.models import RawArticle, Source
from news_agent.logging_setup import get_logger

log = get_logger("fetch.rss")


class RSSFetcher:
    """Parse the feed, then enrich each entry by fetching its article page
    with HTMLFetcher (so we get body text, images and outbound links)."""

    def __init__(
        self,
        client: httpx.Client,
        rate: RateLimiter,
        robots: RobotsCache,
        html_fetcher: HTMLFetcher,
    ) -> None:
        self._client = client
        self._rate = rate
        self._robots = robots
        self._html = html_fetcher

    def fetch(self, source: Source, max_items: int) -> list[RawArticle]:
        if not self._robots.allowed(source.url):
            log.warning("rss.robots_blocked", url=source.url)
            return []
        self._rate.wait(source.url)
        try:
            resp = self._client.get(source.url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            log.warning("rss.fetch_failed", url=source.url, error=str(e))
            return []

        parsed = feedparser.parse(resp.content)
        entries: list[Any] = parsed.entries[:max_items]

        out: list[RawArticle] = []
        for entry in entries:
            link = (entry.get("link") or "").strip()
            title = (entry.get("title") or "").strip()
            if not link or not title:
                continue
            published = _entry_time(entry)
            art = self._html.fetch_single(
                link,
                source_name=source.name,
                source_url=source.url,
                source_language=source.language,
                fallback_title=title,
                fallback_published=published,
            )
            if art is not None:
                out.append(art)
        return out


def _entry_time(entry: Any) -> datetime | None:
    for key in ("published_parsed", "updated_parsed", "created_parsed"):
        t = entry.get(key)
        if t:
            return datetime.fromtimestamp(timegm(t), tz=timezone.utc)
    raw = entry.get("published") or entry.get("updated") or ""
    if raw:
        try:
            return datetime.fromtimestamp(
                _time.mktime(_time.strptime(raw[:25], "%a, %d %b %Y %H:%M:%S")), tz=timezone.utc
            )
        except (ValueError, OverflowError):
            return None
    return None
