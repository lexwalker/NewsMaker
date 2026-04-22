"""Playwright-based fallback fetcher for sites that block httpx.

Rescues two classes of sources that the browser-UA httpx client cannot handle:

  1. **Cloudflare / WAF 403** — the server rejects our TLS fingerprint even
     with a perfect Chrome UA + Sec-Ch-Ua headers. A real Chromium does
     negotiate successfully (JA3 fingerprint matches).

  2. **JS-rendered SPAs** — Volvo, Hyundai, Mercedes and many OEM press
     rooms ship an empty `<body>` and hydrate articles from a JSON API
     client-side. httpx gets the shell; Playwright waits for the DOM to
     populate and then returns fully-rendered HTML.

Design goals:
  * **Fully isolated from the httpx pipeline.** If Playwright is not
    installed or fails for any reason, callers fall back to the old client.
  * **Lazy browser startup.** One Chromium process launched on first use,
    shared across all URLs in a run, closed at the end.
  * **Allowlisted.** Playwright is ~300ms/URL even on a warm browser, so
    we only route domains that we know need it (see
    ``config/http_quirks.yaml`` → ``playwright_domains``).

Usage (adapter side):

    from news_agent.adapters.fetchers.playwright_fetcher import (
        PlaywrightFetcher, PLAYWRIGHT_AVAILABLE,
    )
    if PLAYWRIGHT_AVAILABLE and pw_allowlist.matches(url):
        html = pw.fetch(url)
"""

from __future__ import annotations

from urllib.parse import urlparse

from news_agent.logging_setup import get_logger

log = get_logger("fetch.pw")

# Detect playwright availability at import time so callers can gate without
# a try/except every request.
try:  # pragma: no cover - import probe
    from playwright.sync_api import sync_playwright  # type: ignore[import-not-found]
    PLAYWRIGHT_AVAILABLE = True
except Exception:  # noqa: BLE001
    sync_playwright = None  # type: ignore[assignment]
    PLAYWRIGHT_AVAILABLE = False


# Browser-like UA — matches what the real Chromium ships; servers fingerprint
# the combination of TLS hello + headers.
DEFAULT_PW_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)


class PlaywrightFetcher:
    """Lazy-initialised Chromium wrapper.

    Not thread-safe — use one instance per thread. Meant to be held open
    as a context manager for the duration of a batch run.
    """

    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_PW_UA,
        timeout_ms: int = 20_000,
        wait_until: str = "domcontentloaded",
    ) -> None:
        self.user_agent = user_agent
        self.timeout_ms = timeout_ms
        self.wait_until = wait_until  # "load" | "domcontentloaded" | "networkidle"
        self._pw = None
        self._browser = None
        self._context = None

    # ---- lifecycle --------------------------------------------------
    def __enter__(self) -> "PlaywrightFetcher":
        if not PLAYWRIGHT_AVAILABLE:
            raise RuntimeError(
                "Playwright is not installed. Run `pip install playwright` "
                "and `playwright install chromium`."
            )
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            user_agent=self.user_agent,
            locale="ru-RU",
            viewport={"width": 1366, "height": 768},
        )
        # Block heavy resources we don't care about — HTML + redirects is
        # all we need to extract articles. Saves 5-10× per-page load time.
        self._context.route(
            "**/*",
            lambda route, req: route.abort()
            if req.resource_type in {"image", "media", "font", "stylesheet"}
            else route.continue_(),
        )
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        for obj in (self._context, self._browser, self._pw):
            if obj is None:
                continue
            try:
                if obj is self._pw:
                    obj.stop()
                else:
                    obj.close()
            except Exception as e:  # noqa: BLE001
                log.debug("pw.close.error", error=str(e)[:80])
        self._pw = None
        self._browser = None
        self._context = None

    # ---- request ----------------------------------------------------
    def fetch(self, url: str) -> tuple[int, str]:
        """Return ``(status_code, rendered_html)``.

        Raises on navigation failure so the caller can fall back to httpx.
        ``status_code`` is ``0`` when the main-frame response was missing
        (rare — means the page redirected via JS before network settled).
        """
        if self._context is None:
            raise RuntimeError("PlaywrightFetcher used outside 'with' block")
        page = self._context.new_page()
        try:
            resp = page.goto(url, timeout=self.timeout_ms, wait_until=self.wait_until)
            status = resp.status if resp is not None else 0
            # Let JS settle for a beat — enough to populate SPA shells.
            try:
                page.wait_for_load_state("networkidle", timeout=4000)
            except Exception:  # noqa: BLE001
                # Sites with long-polling never hit networkidle; that's fine,
                # the DOM is usually populated by now.
                pass
            html = page.content()
            return status, html
        finally:
            try:
                page.close()
            except Exception:  # noqa: BLE001
                pass


class PlaywrightAllowlist:
    """Match URLs against a per-domain Playwright allowlist."""

    def __init__(self, domains: set[str]) -> None:
        self._domains = {d.strip().lower() for d in domains if d}

    def matches(self, url: str) -> bool:
        if not self._domains:
            return False
        host = urlparse(url).netloc.lower()
        if host in self._domains:
            return True
        for d in self._domains:
            if host.endswith("." + d):
                return True
        return False


__all__ = [
    "DEFAULT_PW_UA",
    "PLAYWRIGHT_AVAILABLE",
    "PlaywrightAllowlist",
    "PlaywrightFetcher",
]
