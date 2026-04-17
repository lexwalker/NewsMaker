"""Quick live sanity check for the Telegram fetcher."""

from __future__ import annotations

import io
import sys

import httpx

sys.path.insert(0, "src")
from news_agent.adapters.fetchers.telegram import (  # noqa: E402
    parse_channel_html,
    to_channel_preview_url,
)

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

CHANNELS = [
    "https://t.me/chinamashina_news",
    "https://t.me/sergtselikov",
    "https://t.me/DtRoad",
    "https://t.me/AvitoLive",
    "https://t.me/autopotoknews",
]

def main() -> int:
    with httpx.Client(timeout=15.0, follow_redirects=True, headers={"User-Agent": "NewsMakerBot/0.1"}) as client:
        for url in CHANNELS:
            preview = to_channel_preview_url(url) or ""
            print(f"\n=== {url}  →  {preview}")
            try:
                r = client.get(preview)
                r.raise_for_status()
            except Exception as e:  # noqa: BLE001
                print(f"  fetch failed: {e}")
                continue
            posts = parse_channel_html(
                html=r.text,
                channel_preview_url=preview,
                source_name="",
                source_url=url,
                source_language="ru",
                max_items=3,
            )
            print(f"  parsed {len(posts)} posts")
            for p in posts:
                print(f"    [{p.published_at.isoformat() if p.published_at else '—'}] {p.title[:90]}")
                if p.outbound_links:
                    print(f"      → primary? {p.outbound_links[0][:100]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
