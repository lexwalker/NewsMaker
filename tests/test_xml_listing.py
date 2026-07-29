"""Site-own XML listings (jul-29: globalsuzuki ships <newslist>, which
feedparser reads as zero entries and the HTML path sees as a shell)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from news_agent.adapters.fetchers.xml_listing import extract_xml_listing  # noqa: E402

SUZUKI = """<?xml version="1.0" encoding="utf-8"?>
<newslist>
  <year id="ad2026">
    <article>
      <link>/globalnews/2026/0729.html</link>
      <date>July 29</date>
      <message>Suzuki Facelifts the Three-Row Seven-Seater SUV XL7</message>
    </article>
    <article>
      <link>/globalnews/2026/0725.html</link>
      <date>July 25</date>
      <message>Suzuki Introduces the new Compact SUV Brezza in India</message>
    </article>
  </year>
  <year id="ad2025">
    <article>
      <link>/globalnews/2025/1201.html</link>
      <date>December 1</date>
      <message>Suzuki Announces Production Milestone in Hungary</message>
    </article>
  </year>
</newslist>"""


def test_extracts_articles_with_urls_and_dates() -> None:
    got = extract_xml_listing(SUZUKI, "https://www.globalsuzuki.com/globalnews/release.xml")
    assert len(got) == 3
    assert got[0]["url"] == "https://www.globalsuzuki.com/globalnews/2026/0729.html"
    assert got[0]["title"].startswith("Suzuki Facelifts")
    assert got[0]["published_at"].year == 2026 and got[0]["published_at"].month == 7


def test_year_comes_from_the_enclosing_block() -> None:
    # "December 1" carries no year — it must inherit ad2025, not today's.
    got = extract_xml_listing(SUZUKI, "https://www.globalsuzuki.com/globalnews/release.xml")
    old = [g for g in got if "Hungary" in g["title"]][0]
    assert old["published_at"].year == 2025 and old["published_at"].month == 12


def test_iso_and_dotted_dates_parse() -> None:
    xml = ("""<?xml version="1.0"?><list>"""
           """<item><url>/a.html</url><title>Заголовок новости один</title>"""
           """<pubdate>2026-07-29</pubdate></item>"""
           """<item><url>/b.html</url><title>Заголовок новости два</title>"""
           """<pubdate>28.07.2026</pubdate></item></list>""")
    got = extract_xml_listing(xml, "https://x.example/feed.xml")
    assert len(got) == 2
    assert got[0]["published_at"].day == 29 and got[1]["published_at"].day == 28


def test_broken_xml_and_titleless_entries_are_safe() -> None:
    assert extract_xml_listing("<not xml", "https://x.example") == []
    xml = '<?xml version="1.0"?><l><i><link>/a</link><message>кор</message></i></l>'
    assert extract_xml_listing(xml, "https://x.example") == []
