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


# Browser-like UA — many sites (jdpower, nhtsa, tuvsud, lamborghini) refuse
# a vanilla python/httpx UA but serve a Chrome one just fine. We keep a
# trailing "+NewsMakerBot" tag so admins can still identify our traffic.
DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36 "
    "(+NewsMakerBot)"
)


class RetryingHttpClient:
    """httpx.Client wrapper that transparently retries on:
        • HTTPX transient network errors (ReadError, RemoteProtocolError)
        • HTTP 5xx responses
        • HTTP 202 — some servers emit Accepted while the page is still
          being rendered; one short retry usually returns a real 200 body.

    Per-domain quirks:
        • ``ssl_insecure_domains`` — hosts whose TLS chain we accept without
          verification. Only for public news/PR sites we just READ HTML from.
        • ``url_rewrites`` — old sheet URL → live URL mapping, applied
          transparently at request time.

    Keeps the original httpx.Client context-manager API (`with` / `get`).
    """

    def __init__(
        self,
        user_agent: str,
        timeout: float = 20.0,
        max_attempts: int = 2,     # 1 retry is plenty for transient failures
        backoff_base: float = 1.2,
        ssl_insecure_domains: set[str] | None = None,
        url_rewrites: dict[str, str] | None = None,
    ) -> None:
        self._headers = {
            "User-Agent": user_agent,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "ru,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Chromium";v="126", "Not.A/Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        }
        self._timeout = timeout
        self._client = httpx.Client(
            headers=self._headers,
            follow_redirects=True,
            timeout=timeout,
            http2=False,  # some servers mis-negotiate h2 with our header set
        )
        # Insecure twin — created lazily, only when a whitelisted host needs it.
        self._insecure_client: httpx.Client | None = None
        self._ssl_insecure_domains = {d.lower() for d in (ssl_insecure_domains or set())}
        self._url_rewrites = dict(url_rewrites or {})
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base

    def _get_insecure_client(self) -> httpx.Client:
        if self._insecure_client is None:
            self._insecure_client = httpx.Client(
                headers=self._headers,
                follow_redirects=True,
                timeout=self._timeout,
                http2=False,
                verify=False,
            )
        return self._insecure_client

    def _needs_insecure(self, url: str) -> bool:
        if not self._ssl_insecure_domains:
            return False
        host = urlparse(url).netloc.lower()
        if host in self._ssl_insecure_domains:
            return True
        # Allow "example.com" entry to match "sub.example.com".
        for d in self._ssl_insecure_domains:
            if host.endswith("." + d):
                return True
        return False

    def _rewrite(self, url: str) -> str:
        return self._url_rewrites.get(url, url)

    # --- context manager passthrough ---
    def __enter__(self) -> "RetryingHttpClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()
        if self._insecure_client is not None:
            self._insecure_client.close()

    @property
    def headers(self) -> httpx.Headers:
        return self._client.headers

    # --- retrying GET / HEAD ---
    def get(self, url: str, **kwargs: object) -> httpx.Response:  # type: ignore[override]
        return self._request("GET", url, **kwargs)

    def head(self, url: str, **kwargs: object) -> httpx.Response:
        return self._request("HEAD", url, **kwargs)

    def _request(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        """Request with a narrow retry policy.

        Retryable (transient):
          • ``RemoteProtocolError`` / ``ReadError`` — server cut the stream
          • 5xx server errors — server overloaded / temporarily broken
          • 202 Accepted — async-render servers (Nissan, Lamborghini press)

        NOT retryable (permanent):
          • ``ConnectTimeout`` / ``ReadTimeout`` — server not answering;
            retrying wastes ~30 sec per source for no gain.
          • ``ConnectError`` — DNS / network down at the other end.
          • 4xx — our fault (auth, not found, forbidden).
        """
        url = self._rewrite(url)
        client = self._get_insecure_client() if self._needs_insecure(url) else self._client

        last_exc: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = client.request(method, url, **kwargs)  # type: ignore[arg-type]
            except (httpx.RemoteProtocolError, httpx.ReadError) as e:
                last_exc = e
                if attempt == self.max_attempts:
                    raise
                sleep = self.backoff_base ** attempt
                log.debug(
                    "http.retry.transient",
                    url=url, attempt=attempt, error=str(e)[:80], sleep=sleep,
                )
                time.sleep(sleep)
                continue
            except (
                httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError,
            ):
                # The server isn't there. Don't burn time on retries.
                raise

            if resp.status_code == 202 and attempt < self.max_attempts:
                sleep = self.backoff_base ** attempt
                log.debug(
                    "http.retry.202",
                    url=url, attempt=attempt, sleep=sleep,
                )
                time.sleep(sleep)
                continue
            if 500 <= resp.status_code < 600 and attempt < self.max_attempts:
                sleep = self.backoff_base ** attempt
                log.debug(
                    "http.retry.5xx",
                    url=url, attempt=attempt, status=resp.status_code, sleep=sleep,
                )
                time.sleep(sleep)
                continue
            return resp

        if last_exc:
            raise last_exc
        raise RuntimeError(f"request failed without exception: {method} {url}")


def make_http_client(
    user_agent: str | None = None,
    timeout: float = 20.0,
    ssl_insecure_domains: set[str] | None = None,
    url_rewrites: dict[str, str] | None = None,
) -> "RetryingHttpClient":
    """Return a browser-like HTTP client with automatic retries.

    Back-compat: callers who still pass their own UA get it applied; passing
    None (or an obvious bot UA from older .env) uses DEFAULT_BROWSER_UA.

    ``ssl_insecure_domains`` / ``url_rewrites`` — optional per-domain quirks
    loaded from ``config/http_quirks.yaml`` (see ``load_http_quirks``).
    """
    ua = user_agent or DEFAULT_BROWSER_UA
    # If someone explicitly set a short bot-looking UA, upgrade it to the
    # browser UA — we've measured that the bot one is blocked on ~50 sites.
    if ua and len(ua) < 40 and "NewsMakerBot" in ua:
        ua = DEFAULT_BROWSER_UA
    return RetryingHttpClient(
        user_agent=ua,
        timeout=timeout,
        ssl_insecure_domains=ssl_insecure_domains,
        url_rewrites=url_rewrites,
    )
