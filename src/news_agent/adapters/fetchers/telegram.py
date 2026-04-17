"""Telegram public channel fetcher.

Uses the public web preview at ``https://t.me/s/<channel>``. No API keys,
no account, no third-party service. Works for any public channel; private
or invite-only channels are reported as empty.

Output shape is identical to :class:`RawArticle` so the rest of the pipeline
does not care where the content came from.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from bs4.element import Tag

from news_agent.adapters.fetchers.base import RateLimiter, RobotsCache
from news_agent.core.models import RawArticle, Source
from news_agent.logging_setup import get_logger

log = get_logger("fetch.telegram")

_TG_HOSTS = {"t.me", "telegram.me"}
_BG_IMAGE_RE = re.compile(r"background-image:\s*url\(['\"]?([^'\")]+)['\"]?\)")


def is_telegram_url(url: str) -> bool:
    return urlparse(url).netloc.lower() in _TG_HOSTS


def to_channel_preview_url(url: str) -> str | None:
    """Turn any ``t.me/<channel>[/...]`` into ``t.me/s/<channel>``.

    Returns None for invite links (``t.me/+abc…``) and empty paths.
    """
    p = urlparse(url)
    if p.netloc.lower() not in _TG_HOSTS:
        return None
    parts = [s for s in p.path.split("/") if s]
    if not parts:
        return None
    channel = parts[1] if parts[0] == "s" and len(parts) > 1 else parts[0]
    if not channel or channel.startswith("+"):
        return None
    return f"https://t.me/s/{channel}"


class TelegramFetcher:
    """Adapter for public Telegram channels via their web preview."""

    def __init__(
        self, client: httpx.Client, rate: RateLimiter, robots: RobotsCache
    ) -> None:
        self._client = client
        self._rate = rate
        self._robots = robots

    def fetch(self, source: Source, max_items: int) -> list[RawArticle]:
        preview_url = to_channel_preview_url(source.url)
        if not preview_url:
            log.warning("telegram.invalid_url", url=source.url)
            return []
        if not self._robots.allowed(preview_url):
            log.warning("telegram.robots_blocked", url=preview_url)
            return []
        self._rate.wait(preview_url)
        try:
            resp = self._client.get(preview_url)
            resp.raise_for_status()
        except httpx.HTTPError as e:
            log.warning("telegram.fetch_failed", url=preview_url, error=str(e))
            return []

        return parse_channel_html(
            html=resp.text,
            channel_preview_url=preview_url,
            source_name=source.name,
            source_url=source.url,
            source_language=source.language,
            max_items=max_items,
        )


# ---------------------------------------------------------------- pure parser
def parse_channel_html(
    *,
    html: str,
    channel_preview_url: str,
    source_name: str,
    source_url: str,
    source_language: str | None,
    max_items: int,
) -> list[RawArticle]:
    """Pure function — no I/O. Unit-tested directly against saved HTML."""
    soup = BeautifulSoup(html, "lxml")
    wraps = soup.find_all("div", class_="tgme_widget_message_wrap")
    out: list[RawArticle] = []

    # Telegram orders posts chronologically (oldest → newest); we want newest first.
    for wrap in reversed(wraps):
        if len(out) >= max_items:
            break
        if not isinstance(wrap, Tag):
            continue
        article = _parse_post(wrap, source_name, source_url, source_language)
        if article is not None:
            out.append(article)
    return out


def _parse_post(
    wrap: Tag,
    source_name: str,
    source_url: str,
    source_language: str | None,
) -> RawArticle | None:
    post = wrap.find("div", class_="tgme_widget_message")
    if not isinstance(post, Tag):
        return None

    data_post = post.get("data-post", "")  # "channel/msgid"
    if not data_post or "/" not in data_post:
        return None
    post_url = f"https://t.me/{data_post}"

    # Text
    text_el = wrap.find("div", class_="tgme_widget_message_text")
    body = ""
    outbound: list[str] = []
    if isinstance(text_el, Tag):
        # Preserve line breaks for title extraction
        for br in text_el.find_all("br"):
            br.replace_with("\n")
        body = text_el.get_text("\n", strip=True)
        for a in text_el.find_all("a", href=True):
            href = str(a.get("href", "")).strip()
            if href.startswith(("http://", "https://")) and "t.me/" not in href:
                outbound.append(href)

    if not body:
        return None  # media-only post without caption — skip

    # Title: first line, clipped.
    title = body.split("\n", 1)[0].strip()[:220]
    if len(title) < 3:
        return None

    # Published time
    published: datetime | None = None
    time_el = post.find("time", datetime=True)
    if isinstance(time_el, Tag):
        raw = str(time_el.get("datetime", "")).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            published = datetime.fromisoformat(raw)
            if published.tzinfo is None:
                published = published.replace(tzinfo=timezone.utc)
        except ValueError:
            published = None

    # Image: either a photo attachment or a link preview image
    image_url: str | None = None
    for sel in [
        ("a", {"class": "tgme_widget_message_photo_wrap"}),
        ("a", {"class": "link_preview_image"}),
        ("i", {"class": "link_preview_right_image"}),
    ]:
        node = wrap.find(sel[0], attrs=sel[1])
        if isinstance(node, Tag):
            style = str(node.get("style", ""))
            m = _BG_IMAGE_RE.search(style)
            if m:
                image_url = m.group(1)
                break

    return RawArticle(
        url=post_url,
        title=title,
        body=body,
        html=str(wrap),
        published_at=published,
        image_url=image_url,
        outbound_links=_dedup(outbound),
        source_name=source_name,
        source_url=source_url,
        source_language=source_language,
    )


def _dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
