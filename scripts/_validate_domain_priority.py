"""Empirical check: for each editor-labeled 'wrong_primary' case,
compute domain_tier of (a) what bot picked vs (b) editor's reference.
If tier(editor_ref) < tier(bot_pick) → our new tiering would fix it.
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_agent.core.brand_canonical import canonicalize_brand  # noqa: E402
from news_agent.core.source_priority import domain_tier  # noqa: E402


def dom(u: str) -> str:
    try:
        return urlparse(u).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def main() -> int:
    rows = []
    for ln in (ROOT / "data" / "eval_set_v2.jsonl").read_text("utf-8").splitlines():
        if ln.strip():
            try:
                rows.append(json.loads(ln))
            except Exception:
                pass

    # Cases where editor said "wrong primary" AND cited a referenced URL
    cases = [r for r in rows
             if r.get("label_wrong_primary") and r.get("referenced_urls")]
    print(f"editor-flagged wrong_primary with reference URL: {len(cases)}")

    win = lose = tie = no_ref_in_domain = 0
    examples_win = []
    examples_lose = []

    for r in cases:
        bot_url = r.get("url", "")
        ed_url = r["referenced_urls"][0]
        bot_dom = dom(bot_url)
        ed_dom = dom(ed_url)
        title = r["title"]

        brand = canonicalize_brand(title) or canonicalize_brand(
            r.get("body", ""))

        t_bot = domain_tier(bot_dom, brand_canonical=brand)
        t_ed = domain_tier(ed_dom, brand_canonical=brand)

        if t_ed < t_bot:
            win += 1
            if len(examples_win) < 5:
                examples_win.append({
                    "title": title[:60], "brand": brand,
                    "bot": (bot_dom, t_bot),
                    "ed": (ed_dom, t_ed),
                })
        elif t_ed == t_bot:
            tie += 1
        else:
            lose += 1
            if len(examples_lose) < 5:
                examples_lose.append({
                    "title": title[:60], "brand": brand,
                    "bot": (bot_dom, t_bot),
                    "ed": (ed_dom, t_ed),
                })

    total = win + lose + tie
    print(f"\nResults:")
    print(f"  editor's ref scores BETTER (would-fix): {win}/{total} "
          f"({win*100/total:.0f}%)")
    print(f"  same tier (no change):                  {tie}/{total}")
    print(f"  bot's pick scores better (regression):  {lose}/{total}")

    print(f"\n=== WOULD-FIX examples (top 5) ===")
    for e in examples_win:
        print(f"  brand={e['brand']:18}  {e['title']}")
        print(f"    bot: {e['bot'][0]:30}  tier={e['bot'][1]}")
        print(f"    ed:  {e['ed'][0]:30}  tier={e['ed'][1]}  ← preferred")

    if examples_lose:
        print(f"\n=== REGRESSIONS (rare — investigate) ===")
        for e in examples_lose:
            print(f"  brand={e['brand']:18}  {e['title']}")
            print(f"    bot: {e['bot'][0]:30}  tier={e['bot'][1]}")
            print(f"    ed:  {e['ed'][0]:30}  tier={e['ed'][1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
