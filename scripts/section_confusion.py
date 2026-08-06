"""Section confusion matrix: OUR section vs the editor's, on matched stories.

weekly_kpi reports one number (section_right ≈ 66%) — a third of accepted
rows land in a section the editor then changes, which reads as «ненужное»
even when the story itself was wanted. This shows WHERE it breaks: the
matrix of (our section → editor's section) over the same matched pairs the
KPI counts, so constitution examples can target the worst pairs instead of
guessing.

Reuses weekly_kpi's loaders and matcher VERBATIM — this repo has already
been burned once by two matchers drifting apart (KPI vs miss_funnel).

Free: one Sheets read + SQLite. No LLM calls.

Usage: python scripts/section_confusion.py [--days 14]
"""

from __future__ import annotations

import argparse
import io
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from news_agent.core.weekly_kpi import build_index, match, url_key  # noqa: E402
import weekly_kpi as kpi  # noqa: E402  (scripts/ on path; import-safe)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    args = ap.parse_args()

    until = datetime.now(timezone.utc)
    since = until - timedelta(days=args.days)
    svc = kpi._svc()
    archive_entries, sec_by_key, _dated = kpi.load_archive(svc)
    _coll, accepted, _rej, _dates = kpi.load_cache(since, until)
    arch_idx = build_index(archive_entries)

    matrix: Counter = Counter()          # (ours, editors) -> n
    ours_total: Counter = Counter()
    matched = 0
    for a in accepted:
        m, method, matched_sec = match(a, arch_idx)
        if not m:
            continue
        ed_sec = matched_sec
        if method == "url" and not ed_sec:
            ed_sec = sec_by_key.get(url_key(a.url), "")
        if not (a.section and ed_sec):
            continue
        matched += 1
        ours = a.section.strip()
        theirs = ed_sec.strip()
        ours_total[ours] += 1
        matrix[(ours, theirs)] += 1

    print(f"=== МАТРИЦА РАЗДЕЛОВ, {args.days}д: {matched} сопоставленных "
          f"пар (наш раздел -> раздел редактора) ===\n")
    agree = sum(n for (o, t), n in matrix.items() if o.lower() == t.lower())
    print(f"совпадение: {agree}/{matched} "
          f"({agree / matched:.0%})\n" if matched else "нет пар\n")
    print("Худшие пары (наш -> редакторский, по убыванию):")
    wrong = [((o, t), n) for (o, t), n in matrix.items() if o.lower() != t.lower()]
    for (o, t), n in sorted(wrong, key=lambda kv: -kv[1])[:12]:
        share = n / ours_total[o] if ours_total[o] else 0
        print(f"  {o:22} -> {t:22} {n:3}  ({share:.0%} нашего «{o}»)")
    print("\nПо нашим разделам (сколько уходит не туда):")
    for o, tot in ours_total.most_common():
        bad = sum(n for (oo, t), n in matrix.items()
                  if oo == o and oo.lower() != t.lower())
        print(f"  {o:22} пар {tot:3}, редактор переложил {bad:3} ({bad / tot:.0%})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
