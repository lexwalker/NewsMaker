"""Check whether our new browser UA + retries rescue previously-blocked
sources (HTTP 403 / timeouts from v10)."""

from __future__ import annotations

import io
import sys
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, "src")

from news_agent.adapters.fetchers.base import make_http_client  # noqa: E402

# URLs that failed in v10.
TEST_URLS = [
    # 403'd in v10:
    "https://www.jdpower.com/",
    "https://www.nhtsa.gov/press-releases",
    "https://www.tuvsud.com/de-de/publikationen/tuev-report/maengelzwerge-und-fehlerriesen",
    "https://www.uber.com/en-GB/newsroom/news/",
    "https://edition.cnn.com/",
    # timeouts in v10:
    "https://www.rst.gov.ru/portal/gost",
    "https://www.vehiclerecalls.gov.au/",
    "https://minpromtorg.gov.ru/press-centre/news",
    # ok sanity:
    "https://carnewschina.com/",
    "https://www.motortrend.com/",
]


def main() -> int:
    with make_http_client(timeout=12.0) as client:
        for url in TEST_URLS:
            t0 = time.monotonic()
            status: str | int
            note = ""
            try:
                r = client.get(url)
                status = r.status_code
                note = f"{len(r.content)} bytes"
            except Exception as e:  # noqa: BLE001
                status = "ERR"
                note = f"{type(e).__name__}: {e}"[:80]
            elapsed = (time.monotonic() - t0) * 1000
            ok = "✅" if status == 200 else "❌"
            print(f"  {ok} {str(status):>4}  {elapsed:6.0f}ms  {url[:70]:70}  {note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
