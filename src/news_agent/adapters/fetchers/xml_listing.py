"""Article lists published as a site's OWN XML (not RSS/Atom).

Some newsrooms ship a machine-readable listing that is neither HTML nor a
standard feed, so feedparser returns zero entries and the HTML path sees a
JS shell. globalsuzuki.com is the live example (jul-29):

    <newslist>
      <year id="ad2026">
        <article>
          <link>/globalnews/2026/0729.html</link>
          <date>July 29</date>
          <midashi>Suzuki to exhibit at …</midashi>
        </article>

Deliberately shape-agnostic: we look for repeated elements that carry a
link plus a text label, whatever the tags are called, so a sibling site
with the same idea and different names still works. Never raises.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

_LINK_TAGS = ("link", "url", "href", "loc")
_TITLE_TAGS = ("midashi", "title", "headline", "name", "subject",
               # globalsuzuki calls its headline <message> (verified live)
               "message", "text", "caption")
_DATE_TAGS = ("date", "pubdate", "published", "datetime")

# "July 29" / "July 29, 2026" / "2026-07-29" / "29.07.2026"
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}


def _parse_date(raw: str, year_hint: int | None) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        y, mo, d = (int(x) for x in m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc)
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})", s)
    if m:
        d, mo, y = (int(x) for x in m.groups())
        return datetime(y, mo, d, tzinfo=timezone.utc)
    m = re.match(r"^([A-Za-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?", s)
    if m:
        mon = _MONTHS.get(m.group(1).lower())
        if mon:
            year = int(m.group(3)) if m.group(3) else year_hint
            if year:
                try:
                    return datetime(year, mon, int(m.group(2)), tzinfo=timezone.utc)
                except ValueError:
                    return None
    return None


def _child_text(el, names: tuple[str, ...]) -> str:
    for child in el:
        tag = child.tag.split("}")[-1].lower()
        if tag in names and (child.text or "").strip():
            return child.text.strip()
    return ""


def _year_hint(el) -> int | None:
    """Nearest enclosing element carrying a 4-digit year (id="ad2026")."""
    for attr in el.attrib.values():
        m = re.search(r"(20\d\d)", str(attr))
        if m:
            return int(m.group(1))
    return None


def extract_xml_listing(xml_text: str, base_url: str) -> list[dict]:
    """Return [{url, title, published_at}] from a custom XML listing."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    out: list[dict] = []
    seen: set[str] = set()

    def walk(node, year: int | None):
        year = _year_hint(node) or year
        link = _child_text(node, _LINK_TAGS)
        title = _child_text(node, _TITLE_TAGS)
        if link and title and len(title) >= 12:
            url = urljoin(base_url, link)
            if url not in seen:
                seen.add(url)
                out.append({
                    "url": url,
                    "title": title,
                    "published_at": _parse_date(_child_text(node, _DATE_TAGS), year),
                })
        for child in node:
            walk(child, year)

    walk(root, None)
    return out
