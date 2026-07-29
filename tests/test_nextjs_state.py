"""__NEXT_DATA__ listing extraction (jul-29: lada.ru sat at zero links
for weeks while 18 press releases were in the HTML all along)."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.adapters.fetchers.nextjs_state import (  # noqa: E402
    extract_next_data_articles,
)


def _page(blob: dict) -> str:
    return ('<html><body><script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(blob, ensure_ascii=False) + "</script></body></html>")


FRESH = [{"id": 122421, "name": "АВТОВАЗ ПРОДОЛЖИТ МОДЕРНИЗАЦИЮ", "created": "24.07.2026"},
         {"id": 122420, "name": "LADA AZIMUT ПРОШЕЛ КРАШ-ТЕСТ", "created": "23.07.2026"},
         {"id": 122418, "name": "LADA NIVA LEGEND 1.8: СТАРТ", "created": "20.07.2026"}]
STALE = [{"id": 99001, "name": "НА ЛЕД ЗА НОВЫМИ РЕКОРДАМИ LADA", "created": "24.01.2024"},
         {"id": 99002, "name": "СТАРАЯ НОВОСТЬ ПРО ХОККЕЙ LADA", "created": "20.01.2024"},
         {"id": 99003, "name": "ЕЩЕ ОДНА СТАРАЯ ЗАПИСЬ LADA", "created": "10.01.2024"}]


def test_picks_the_fresh_list_not_the_build_time_copy() -> None:
    # The real lada.ru page ships BOTH: fresh under props.pageProps and a
    # January-2024 cache under props.initialState. Freshness must decide.
    html = _page({"props": {"pageProps": {"initialState": {"pressReleases": {"news": FRESH}}},
                            "initialState": {"news": {"releasesPage": STALE}}}})
    got = extract_next_data_articles(html, "https://www.lada.ru/press-releases",
                                     article_path="/press-releases")
    assert len(got) == 3
    assert all(it["published_at"].year == 2026 for it in got)
    assert got[0]["url"] == "https://www.lada.ru/press-releases/122421"


def test_builds_urls_from_ids_and_keeps_titles() -> None:
    html = _page({"props": {"pageProps": {"list": FRESH}}})
    got = extract_next_data_articles(html, "https://www.lada.ru/press-releases",
                                     article_path="/press-releases")
    assert [it["title"] for it in got][:1] == ["АВТОВАЗ ПРОДОЛЖИТ МОДЕРНИЗАЦИЮ"]
    assert got[1]["url"].endswith("/press-releases/122420")


def test_absolute_urls_in_payload_are_used_as_is() -> None:
    items = [{"url": f"https://x.example/news/{i}", "title": f"Заголовок новости номер {i}",
              "date": "2026-07-2%d" % i} for i in (1, 2, 3)]
    got = extract_next_data_articles(_page({"props": {"pageProps": {"items": items}}}),
                                     "https://x.example/news")
    assert len(got) == 3 and got[0]["url"] == "https://x.example/news/1"


def test_no_next_data_or_broken_json_returns_empty() -> None:
    assert extract_next_data_articles("<html><body>nope</body></html>", "https://x.example") == []
    broken = '<script id="__NEXT_DATA__">{not json</script>'
    assert extract_next_data_articles(broken, "https://x.example") == []


def test_short_or_titleless_lists_are_ignored() -> None:
    # Two items, or items without a real headline, are navigation — not news.
    html = _page({"props": {"pageProps": {"x": [{"id": 1, "name": "Слишком коротко"}]}}})
    assert extract_next_data_articles(html, "https://x.example", article_path="/n") == []
