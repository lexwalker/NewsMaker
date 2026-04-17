"""Shared fetcher abstractions: rate limiting, robots.txt, HTTP client."""

from __future__ import annotations

import threading
import time
import urllib.robotparser
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urljoin, urlparse

import httpx

from news_agent.core.models import RawArticle, Source
from news_agent.core.urls import domain_of
from news_agent.logging_setup import get_logger

log = get_logger("fetch")


class Fetcher(Protocol):
    def fetch(self, source: Source, max_items: int) -> list[RawArticle]:
        ...


@dataclass
class RateLimiter:
    """Per-domain token bucket. Thread-safe, process-local."""

    default_rps: float = 1.0

    def __post_init__(self) -> None:
        self._last: dict[str, float] = {}
        self._lock = threading.Lock()
        self._overrides: dict[str, float] = {}

    def set_rate(self, domain: str, rps: float) -> None:
        self._overrides[domain] = rps

    def wait(self, url: str) -> None:
        d = domain_of(url)
        rps = self._overrides.get(d, self.default_rps)
        if rps <= 0:
            return
        min_gap = 1.0 / rps
        with self._lock:
            last = self._last.get(d, 0.0)
            now = time.monotonic()
            gap = now - last
            if gap < min_gap:
                time.sleep(min_gap - gap)
            self._last[d] = time.monotonic()


class RobotsCache:
    def __init__(self, user_agent: str, client: httpx.Client) -> None:
        self.ua = user_agent
        self._client = client
        self._cache: dict[str, urllib.robotparser.RobotFileParser] = {}

    def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        rp = self._cache.get(base)
        if rp is None:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(urljoin(base, "/robots.txt"))
            try:
                r = self._client.get(urljoin(base, "/robots.txt"), timeout=10.0)
                if r.status_code == 200:
                    rp.parse(r.text.splitlines())
                else:
                    rp.parse([])  # treat as open
            except httpx.HTTPError:
                rp.parse([])  # be lenient on robots.txt fetch failures
            self._cache[base] = rp
        return rp.can_fetch(self.ua, url)


def make_http_client(user_agent: str, timeout: float = 20.0) -> httpx.Client:
    return httpx.Client(
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en,ru;q=0.9",
        },
        follow_redirects=True,
        timeout=timeout,
    )
