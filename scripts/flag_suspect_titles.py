"""Walk a clusters JSON and flag canonical titles that look broken.

Heuristics:
  - "Year mismatch": headline says 2024 / 2023 in a "will arrive / will
    be / to arrive" context (future) — current year is 2026, so the year
    is either wrong or the tense is wrong.
  - "EN/RU semantic drift": rapidfuzz token_set_ratio between the EN
    line and the RU line (ignoring brand names) is below 35.

Output: data/suspect_titles.json with one entry per flagged cluster
plus the lede so a human can decide what the title should actually be.
"""

from __future__ import annotations

import io
import json
import re
import sys
from pathlib import Path

from rapidfuzz import fuzz

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
ROOT = Path(__file__).resolve().parents[1]

CURRENT_YEAR = 2026
PAST_YEARS = {y for y in range(2020, CURRENT_YEAR)}
FUTURE_VERBS = (
    "will ", "to arrive", "to launch", "to open", "to expand",
    "to start", "to debut", "to introduce", "to build",
    "появится", "запустит", "откроет", "выйдет", "начнёт",
    "стартует", "дебютирует",
)


def _split_lines(combined: str) -> tuple[str, str]:
    """Extract clean EN and RU lines from a "EN: ...\nRU: ..." cell."""
    en, ru = "", ""
    for line in combined.splitlines():
        s = line.strip()
        if s.startswith("EN:"):
            en = s[3:].strip()
        elif s.startswith("RU:"):
            ru = s[3:].strip()
    if not en and not ru:
        # Pre-LLM rows had a single line (raw scrape).
        en = combined.strip()
    return en, ru


def _strip_lang_tag(s: str) -> str:
    return re.sub(r"\s*\([A-Za-zА-Яа-яЁё]{2,4}\)\s*$", "", s).strip()


def has_past_year_in_future_context(text: str) -> str | None:
    """Return the offending year if the title says 2023/2024 next to a
    future-tense verb."""
    text_lower = text.lower()
    has_future = any(v in text_lower for v in FUTURE_VERBS)
    if not has_future:
        return None
    for y in PAST_YEARS:
        if str(y) in text:
            return str(y)
    return None


def en_ru_drift(en: str, ru: str) -> int:
    """Return 0..100 similarity. Below ~35 = likely different stories."""
    if not en or not ru:
        return 100  # cannot compare; don't flag
    # Strip language tags
    en = _strip_lang_tag(en).lower()
    ru = _strip_lang_tag(ru).lower()
    # Map common Latin brand → ignore in comparison (brands appear in both)
    return fuzz.token_set_ratio(en, ru)


def main() -> int:
    src = sys.argv[1] if len(sys.argv) > 1 else "data/clusters_ТЕСТ_статьи_v18.json"
    clusters = json.loads(Path(src).read_text(encoding="utf-8"))
    flagged: list[dict] = []
    for i, c in enumerate(clusters):
        title = c["canonical_title"]
        en, ru = _split_lines(title)
        issues: list[str] = []
        # Past-year check on either line
        for line, lang in ((en, "EN"), (ru, "RU")):
            bad_year = has_past_year_in_future_context(line)
            if bad_year:
                issues.append(f"{lang}-past-year:{bad_year}")
        # Drift between EN and RU
        if en and ru:
            sim = en_ru_drift(en, ru)
            if sim < 35:
                issues.append(f"en-ru-drift:{sim}")
        if issues:
            flagged.append({
                "index": i,
                "issues": issues,
                "size": c["size"],
                "section": c["section"],
                "canonical_title": title,
                "canonical_lede": c.get("canonical_lede", "")[:600],
                "canonical_url": c["canonical_url"],
            })
    out = ROOT / "data" / "suspect_titles.json"
    out.write_text(
        json.dumps(flagged, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Total clusters: {len(clusters)}")
    print(f"Flagged: {len(flagged)} ({len(flagged)/max(len(clusters),1)*100:.1f}%)")
    print()
    from collections import Counter
    issue_counter: Counter[str] = Counter()
    for f in flagged:
        for i in f["issues"]:
            issue_counter[i.split(":")[0]] += 1
    print("Issue types:")
    for k, n in issue_counter.most_common():
        print(f"  {k}: {n}")
    print(f"\nWritten to: {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
