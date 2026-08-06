"""Does judging ten articles per call give the same verdicts as one per call?

The constitution is ~8.5k tokens and is read once PER CALL. Caching already cut
each read to a tenth of list price, but 418 reads a run still add up to $1.06 —
the largest removable line in a $3.30 run. Batching attacks the count rather
than the price: ten articles per call is one read instead of ten.

The risk is attention: a model judging ten stories at once may be sloppier than
one judging a single story. This replays rows whose one-per-call verdict is
already recorded on the sheet, in batches, through the SAME production code the
runner uses (editorial_review_batch), and compares.

Run aug-06 on 60 rows of «ТЕСТ статьи v78», batch 10, Sonnet:
    54/60 verdicts identical, 1 of 30 accepted stories flipped to rejected.
    That is 1/30 — a 95% upper bound of 14.9%, so it does NOT establish that
    the loss is under the 5% bar it was measured against. It only fails to
    show the bar is broken. Distinguishing 3% from 5% needs hundreds of rows.

COSTS MONEY: one Sonnet call per batch, plus the constitution once per call.
The aug-06 run cost $0.052. Ask before running it.

  python scripts/eval_batch_review.py                    # 60 rows, batch 10
  python scripts/eval_batch_review.py --rows 200 --batch 10
  python scripts/eval_batch_review.py --tab "ТЕСТ статьи (гор) v83"

Deliberately reads the CONSECUTIVE rows of a tab by default, not a shuffled
sample: in production a batch is ten adjacent candidates, which means one or
two sources, and a shuffled sample would test the batch on more varied material
than it ever sees. --shuffle restores the old behaviour for comparison.
"""

from __future__ import annotations

import argparse
import random
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from news_agent.core.console import force_utf8_stdio  # noqa: E402

force_utf8_stdio()

import weekly_kpi as wk  # noqa: E402
from batch_fetch_test import SHEET_ID  # noqa: E402
from news_agent.adapters.llm import make_llm_client  # noqa: E402
from news_agent.core.articles_schema import COL  # noqa: E402
from news_agent.core.config_loader import load_sections  # noqa: E402
from news_agent.settings import get_settings  # noqa: E402

ACCEPTED = {"Точно новость", "Возможно новость"}
# Verdicts decided BEFORE the LLM ever saw the row — nothing to compare against.
PRE_LLM = {
    "Точно не новость (не авто)", "Точно не новость (не статья)",
    "Точно не новость (чёрный список)", "Отклонить (дубль финального URL)",
    "Отклонить (уже опубликовано редактором)", "Точно не новость (мульти-новость)",
    "Точно не новость (дзен-листикл)", "Отклонить (ошибка загрузки)",
    "Отклонить (не удалось извлечь)",
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tab", default="ТЕСТ статьи v78")
    ap.add_argument("--rows", type=int, default=60)
    ap.add_argument("--batch", type=int, default=10)
    ap.add_argument("--shuffle", action="store_true",
                    help="sample at random instead of taking adjacent rows "
                         "(does NOT reflect how production batches are cut)")
    ap.add_argument("--seed", type=int, default=20260806)
    args = ap.parse_args()

    svc = wk._svc()
    v = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{args.tab}'!A1:AH4000").execute().get("values", [])
    g = lambda r, i: (r[i] if i < len(r) else "").strip()  # noqa: E731

    pool = []
    for r in v[1:]:
        t, verdict, lede = g(r, COL.TITLE), g(r, COL.VERDICT), g(r, COL.LEDE)
        if not t or verdict in PRE_LLM or not verdict or len(lede) < 80:
            continue
        title = re.sub(r"^(en|ru):\s*", "", t.splitlines()[0].strip(), flags=re.I)
        pool.append((title, lede, verdict in ACCEPTED))

    if args.shuffle:
        random.seed(args.seed)
        cases = random.sample(pool, min(args.rows, len(pool)))
    else:
        cases = pool[:args.rows]
    if not cases:
        print(f"в «{args.tab}» нет строк с записанным вердиктом LLM")
        return 1

    n_acc = sum(1 for c in cases if c[2])
    n_calls = (len(cases) + args.batch - 1) // args.batch
    print(f"{args.tab}: {len(cases)} строк "
          f"(принято {n_acc}, отклонено {len(cases) - n_acc}"
          f"{', случайная выборка' if args.shuffle else ', подряд'})")
    print(f"пачками по {args.batch} → {n_calls} вызовов вместо {len(cases)}\n")

    client = make_llm_client(get_settings())
    sections = load_sections()
    spent = 0.0
    agree: Counter[str] = Counter()
    flips = []
    for start in range(0, len(cases), args.batch):
        chunk = cases[start:start + args.batch]
        try:
            # The production call, not a copy of it — that is the point.
            reviews, usage = client.editorial_review_batch(
                items=[(t, lede) for t, lede, _ in chunk],
                sections=sections, portal_country="Russia")
            spent += usage.cost_usd
        except Exception as e:  # noqa: BLE001
            print(f"  ошибка вызова: {type(e).__name__}: {str(e)[:90]}")
            agree["вызов упал"] += len(chunk)
            continue
        got = sum(1 for r in reviews if r is not None)
        for (title, _, was_accepted), rev in zip(chunk, reviews):
            if rev is None:
                agree["не вернул вердикт"] += 1
                continue
            if bool(rev.should_publish) == was_accepted:
                agree["совпало"] += 1
            else:
                agree["РАЗОШЛОСЬ"] += 1
                flips.append((was_accepted, title[:60], (rev.reason or "")[:64]))
            if rev.event_signature is None:
                agree["без подписи события"] += 1
        print(f"  пачка {start // args.batch + 1}: вернул {got}/{len(chunk)}"
              f"  ${spent:.3f}")

    n = agree["совпало"] + agree["РАЗОШЛОСЬ"] + agree["не вернул вердикт"] \
        + agree["вызов упал"]
    print(f"\n=== из {n} строк ===")
    for k, val in agree.most_common():
        print(f"  {val:>4}  ({val / max(1, n):>4.0%})  {k}")

    lost = [f for f in flips if f[0]]
    print(f"\n  ПОТЕРЯНО принятых новостей: {len(lost)} из {n_acc}"
          f"  ({len(lost) / max(1, n_acc):.0%})")
    try:
        from scipy.stats import beta
        print(f"  верхняя граница 95% (Клоппер–Пирсон): "
              f"{beta.ppf(0.95, len(lost) + 1, n_acc - len(lost)):.1%}"
              f"  ← вот что этот замер на самом деле показывает")
    except ImportError:
        pass
    print(f"  потрачено: ${spent:.3f}")
    if lost:
        print("\nчто пакетная обработка отклонила из принятого:")
        for _, t, why in lost[:10]:
            print(f"   {t}\n      {why}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
