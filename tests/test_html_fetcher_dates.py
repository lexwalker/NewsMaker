

def test_span_itemprop_datepublished_content() -> None:
    # jul-27 (autostat): the date lives on a <span itemprop="datePublished"
    # content="…"> — the meta-only loop missed it and trafilatura mis-dated
    # a fresh article to 2022, so the freshness gate silently dropped it.
    from bs4 import BeautifulSoup
    from news_agent.adapters.fetchers.html import _pick_published
    soup = BeautifulSoup(
        '<html><body><span class="date" itemprop="datePublished" '
        'content="2026-07-26">вчера, 09:00</span></body></html>', "lxml")
    dt = _pick_published(soup)
    assert dt is not None and dt.year == 2026 and dt.month == 7 and dt.day == 26
