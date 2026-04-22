"""curl_cffi fetcher — impersonates Chrome's JA3/JA4 TLS fingerprint.

This is the right tool for sites that reject httpx even with perfect
browser headers. The block happens at the TLS layer — the server reads
the clientHello hash and decides we're not a real browser.

curl_cffi is a Python binding over BoringSSL configured to match Chrome's
cipher-suite order, curves, and extensions exactly. It's ~10× faster
than Playwright (no browser startup) and ~1.5× faster than httpx.

When to use this vs Playwright:
  • curl_cffi  → Cloudflare / Akamai / F5 block, server returns static
    HTML once you're past the TLS gate (NHTSA, Volvo, Tuv Sud, Stellantis).
  • Playwright → JS-rendered SPA. curl_cffi will return the empty shell
    because there's no JS engine.

Graceful degradation: if curl_cffi isn't importable (install failed,
platform lacks prebuilt wheel) the fetcher disables itself; callers
fall through to httpx → Playwright.
"""

from __future__ import annotations

from urllib.parse import urlparse

from news_agent.logging_setup import get_logger

log = get_logger("fetch.impersonate")

try:  # pragma: no cover - import probe
    from curl_cffi import requests as _cffi_requests  # type: ignore[import-not-found]
    CURL_CFFI_AVAILABLE = True
except Exception:  # noqa: BLE001
    _cffi_requests = None  # type: ignore[assignment]
    CURL_CFFI_AVAILABLE = False


# Chrome version string passed to curl_cffi. We use a recent stable to
# match what real users send. Bump ~every 6 months — older impersonations
# start to stand out again when the real Chrome rolls forward.
DEFAULT_IMPERSONATE = "chrome124"


class ImpersonateFetcher:
    """Thin wrapper around curl_cffi.requests.

    Stateless: no context manager needed. Keeping the class form so future
    sessions with connection pooling can be added without changing callers.
    """

    def __init__(
        self,
        *,
        impersonate: str = DEFAULT_IMPERSONATE,
        timeout: float = 20.0,
    ) -> None:
        if not CURL_CFFI_AVAILABLE:
            raise RuntimeError(
                "curl_cffi is not installed. Run `pip install curl_cffi`."
            )
        self.impersonate = impersonate
        self.timeout = timeout

    def fetch(self, url: str) -> tuple[int, str]:
        """Return ``(status_code, rendered_html)``.

        Raises on connection failure so the caller can fall back.
        """
        r = _cffi_requests.get(  # type: ignore[union-attr]
            url,
            impersonate=self.impersonate,
            timeout=self.timeout,
            allow_redirects=True,
        )
        # ``r.text`` does charset detection; fast and usually correct.
        return r.status_code, r.text


class ImpersonateAllowlist:
    """Match URLs against a per-domain curl_cffi allowlist."""

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
    "CURL_CFFI_AVAILABLE",
    "DEFAULT_IMPERSONATE",
    "ImpersonateAllowlist",
    "ImpersonateFetcher",
]
