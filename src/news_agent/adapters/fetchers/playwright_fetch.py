"""JS-rendering fetcher. Used only for sources with ``requires_js: true``.

Playwright is an optional dependency — installed via the ``[js]`` extra.
We import it lazily so the base install does not require the ~300MB browser.
"""

from __future__ import annotations

from news_agent.adapters.fetchers.html import extract_article
from news_agent.core.models import RawArticle, Source
from news_agent.logging_setup import get_logger

log = get_logger("fetch.playwright")


class PlaywrightFetcher:
    def __init__(self, user_agent: str, timeout_ms: int = 30_000) -> None:
        self.user_agent = user_agent
        self.timeout_ms = timeout_ms

    def fetch_html(self, url: str) -> str | None:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            log.error("playwright.not_installed", hint="pip install -e '.[js]' && playwright install chromium")
            return None
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(headless=True)
                ctx = browser.new_context(user_agent=self.user_agent)
                page = ctx.new_page()
                page.goto(url, timeout=self.timeout_ms, wait_until="domcontentloaded")
                html = page.content()
                browser.close()
                return html
        except Exception as e:  # noqa: BLE001
            log.warning("playwright.fetch_failed", url=url, error=str(e))
            return None

    def fetch(self, source: Source, max_items: int) -> list[RawArticle]:
        html = self.fetch_html(source.url)
        if not html:
            return []
        # Playwright path currently handles single-article URLs or simple
        # index pages where the article URL equals the source URL. Index-page
        # scraping with JS is out of MVP scope — use RSS for those.
        art = extract_article(
            html=html,
            url=source.url,
            source_name=source.name,
            source_url=source.url,
            source_language=source.language,
        )
        return [art] if art else []
