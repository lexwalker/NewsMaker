"""Blacklist-phrase probe — decide per phrase whether removing it is safe,
by MEASURING (not guessing): run the articles a phrase killed through the
production LLM (editorial_review) and see what it would do without the
blacklist.

Decision per killed article:
  LLM would REJECT  → the LLM catches it anyway → the phrase is redundant
                      (safe to remove; junk still filtered downstream)
  LLM would PUBLISH → only the blacklist was stopping it. Good if the
                      article is genuinely wanted (phrase HARMFUL → remove);
                      bad if it's junk the LLM mis-accepts (phrase is
                      LOAD-BEARING → keep).

So a phrase is safe to remove when the LLM REJECTS most of what it killed
(redundant) OR the killed items are genuinely wanted (harmful). It must be
KEPT when the LLM would PUBLISH junk it currently blocks.

Usage: python scripts/blacklist_phrase_probe.py каршеринг мотоцикл автобус
"""

from __future__ import annotations

import io
import json
import os
import sqlite3
import sys
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

SQLITE_PATH = DATA / "news_agent.sqlite"


def killed_by(phrase: str) -> list[tuple[str, str]]:
    con = sqlite3.connect(str(SQLITE_PATH))
    out = []
    for title, lede, cj in con.execute(
        "SELECT title, lede_text, cached_row_json FROM seen_articles "
        "WHERE cached_row_json IS NOT NULL"
    ).fetchall():
        try:
            d = json.loads(cj)
        except Exception:
            continue
        if d.get("verdict") != "Точно не новость (чёрный список)":
            continue
        ar = (d.get("article_reasons") or "").lower()
        if phrase in ar:
            out.append((title, lede or ""))
    con.close()
    return out


def main() -> int:
    phrases = sys.argv[1:] or ["каршеринг", "мотоцикл", "автобус"]
    client = make_llm_client(get_settings())
    sections = load_sections()

    for phrase in phrases:
        arts = killed_by(phrase)
        print(f"\n=== '{phrase}': {len(arts)} killed by blacklist ===")
        pub = 0
        for title, lede in arts:
            body = lede or title
            try:
                review, _u = client.editorial_review(
                    title=title, body=body, sections=sections,
                    portal_country="Russia")
                p = bool(review.should_publish)
                sec = review.section or "—"
            except Exception as e:  # noqa: BLE001
                p, sec = None, f"err:{type(e).__name__}"
            pub += 1 if p else 0
            mark = "ПУБЛ" if p else "откл" if p is False else "err "
            print(f"   [{mark}] {sec[:16]:16} {title[:58]}")
        n = len(arts)
        if n:
            print(f"   → LLM published {pub}/{n}. "
                  + ("LLM lets these through → phrase is the only guard "
                     "(KEEP if they're junk, REMOVE if wanted)."
                     if pub > n / 2 else
                     "LLM rejects most → phrase redundant, REMOVE is safe."))
    print("\nRead the titles: REMOVE phrases whose killed items are wanted "
          "or LLM-rejected; KEEP phrases guarding junk the LLM would publish.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
