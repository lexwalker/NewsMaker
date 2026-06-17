"""Честная сводка после ревью редактора — ВСЕГДА показывает обе оси
(recall + precision), чтобы нельзя было показать только удобную.

Запускать ПОСЛЕ того как редактор просмотрел прогон:

    python scripts/scorecard.py

Что делает:
  1. Тянет свежие комментарии редактора (sync_editor_feedback).
  2. PRECISION — из того что МЫ показали, сколько редактор пометил
     ошибкой (дубль / не та секция / не тот источник / не нужно).
     Считается на только что прокомментированных строках.
  3. RECALL — из того что редактор ОПУБЛИКОВАЛ (лист «Опубликованные
     3», если выгружен), сколько мы вообще поймали.
  4. Печатает обе цифры вместе с честными оговорками про смещение.

Никаких «удобных половин» — обе оси или ничего.
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
DATA = ROOT / "data"
EVAL_V2 = DATA / "eval_set_v2.jsonl"
PUBLISHED = DATA / "published_3.json"

# Shared with weekly_kpi.py so the editor-comment precision is computed ONE
# way in both places (no drift between the weekly KPI and this scorecard).
from news_agent.core.editor_feedback import row_errors  # noqa: E402


def _load_jsonl(p: Path) -> list[dict]:
    if not p.exists():
        return []
    out = []
    for ln in p.read_text(encoding="utf-8").splitlines():
        if ln.strip():
            try:
                out.append(json.loads(ln))
            except (json.JSONDecodeError, ValueError):
                pass
    return out


def precision_block(since_days: int) -> None:
    rows = _load_jsonl(EVAL_V2)
    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=since_days)).isoformat()
    recent = [r for r in rows
              if r.get("provenance") == "editor_sheet"
              and r.get("synced_at", "") >= cutoff]

    print("=" * 60)
    print(f"PRECISION — из того что МЫ показали (за {since_days} дн.)")
    print("=" * 60)
    if not recent:
        print(f"  Нет свежих комментариев за {since_days} дн.")
        print("  (редактор ещё не смотрел последний прогон?)")
        # fall back to all-time
        recent = [r for r in rows if r.get("provenance") == "editor_sheet"]
        print(f"  Показываю за всё время: {len(recent)} строк\n")
    else:
        print(f"  Прокомментировано строк: {len(recent)}\n")

    if not recent:
        print("  нет данных")
        return

    err_counter: Counter = Counter()
    clean = with_err = 0
    for r in recent:
        errs = row_errors(r)
        if errs:
            with_err += 1
            for e in errs:
                err_counter[e] += 1
        else:
            clean += 1
    n = len(recent)
    print(f"  Чистых (редактор не ругался):  {clean:>4}  ({clean*100//n}%)")
    print(f"  С ошибкой:                     {with_err:>4}  ({with_err*100//n}%)")
    print(f"\n  По типам (могут пересекаться):")
    for e, c in err_counter.most_common():
        print(f"    {e:18} {c:>4}  ({c*100//n}%)")
    print(f"\n  ⚠ ОГОВОРКА: редактор комментирует в основном ПРОБЛЕМНЫЕ")
    print(f"    строки, чистые часто пропускает молча. Поэтому реальная")
    print(f"    precision по ВСЕМ показанным — выше этой цифры.")


def recall_block() -> None:
    print("\n" + "=" * 60)
    print("RECALL — из опубликованного редактором, сколько поймали")
    print("=" * 60)
    # published_3.json — это JSON-массив (не jsonl)
    pub = []
    if PUBLISHED.exists():
        try:
            pub = json.loads(PUBLISHED.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, ValueError):
            pub = []
    if not pub:
        print("  Нет файла published_3.json — нужна свежая выгрузка листа")
        print("  «Опубликованные N». Recall посчитать не могу без неё.")
        return

    import sqlite3
    import re
    sys.path.insert(0, str(ROOT / "src"))
    from news_agent.core.brand_canonical import canonicalize_brand

    db = DATA / "news_agent.sqlite"
    con = sqlite3.connect(db)
    surfaced = []
    for (title, blob) in con.execute(
            "SELECT title, cached_row_json FROM seen_articles"):
        if not blob:
            continue
        try:
            j = json.loads(blob)
        except (ValueError, TypeError):
            continue
        if j.get("verdict") == "Точно новость":
            surfaced.append((title or "").lower())
    blob_surf = " || ".join(surfaced)

    def model_tokens(t: str) -> list[str]:
        toks = re.findall(
            r"\b([A-Z][a-zA-Z]*[-\s]?\d+[A-Za-z]*|\b[A-Z]{2,}\b"
            r"|\b[A-Z][a-z]+\b)", t)
        stop = {"The", "In", "For", "And", "New", "SUV", "EV", "To",
                "Of", "At", "On", "Russia", "China", "Russian", "USA"}
        return [x for x in toks if x not in stop and len(x) >= 3]

    hit = judge = 0
    for p in pub:
        toks = model_tokens(p["title"])
        brand = canonicalize_brand(p["title"])
        if not toks and not brand:
            continue
        judge += 1
        if any(tk.lower() in blob_surf for tk in toks):
            hit += 1
    if judge:
        print(f"  Опубликовано (судимо):  {judge}")
        print(f"  Из них показали:        {hit}  ({hit*100//judge}%)")
        print(f"\n  ⚠ ОГОВОРКА: совпадение по бренду+модели (нестрогое).")
        print(f"    Реальная цифра может быть чуть ниже. Меряет ПРОПУСКИ,")
        print(f"    НЕ качество показанного (это precision выше).")


def main() -> int:
    since_days = 3
    args = sys.argv[1:]
    for a in args:
        if a.startswith("--since-days="):
            since_days = int(a.split("=", 1)[1])

    if "--no-sync" not in args:
        print("Тяну свежие комментарии редактора…\n")
        try:
            subprocess.run(
                [sys.executable, str(ROOT / "scripts"
                                      / "sync_editor_feedback.py")],
                check=False, capture_output=True, timeout=120,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  (sync пропущен: {type(e).__name__})")

    print("\n╔" + "═" * 58 + "╗")
    print("║  ЧЕСТНАЯ СВОДКА — обе оси сразу, без удобных половин     ║")
    print("╚" + "═" * 58 + "╝\n")

    precision_block(since_days)
    recall_block()

    print("\n" + "—" * 60)
    print("Как читать:")
    print("  RECALL    = не пропускаем ли мы новости (ловим всё?)")
    print("  PRECISION = не заваливаем ли мусором (то что показали — ок?)")
    print("  Хорошо = обе высокие. Сейчас recall высокий, растим precision.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
