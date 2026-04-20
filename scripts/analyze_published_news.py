"""Extract learnings from the 'Новости опубликованные' tab.

We treat published rows as the gold standard: whatever ended up there is,
by definition, what this editorial team considers a newsworthy item.

The script builds:
  • section distribution
  • region & country distribution
  • top source domains         (→ whitelist for prioritising sources)
  • URL path patterns          (→ article-URL heuristic)
  • brand frequency            (→ which brands to keep in brand_domains.yaml)
  • headline length stats
  • publication cadence        (hour-of-day histogram)
  • few-shot examples per section (→ LLM classifier)

Writes a consolidated summary into the АНАЛИЗ опубликованных tab, and a
few-shot YAML file at config/few_shots.yaml.
"""

from __future__ import annotations

import io
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.core.config_loader import load_brand_domains  # noqa: E402

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
PUBLISHED_TAB = os.environ.get("PUBLISHED_NEWS_TAB", "Новости опубликованные")
SUMMARY_TAB = "АНАЛИЗ опубликованных"
FEW_SHOTS_PATH = ROOT / "config" / "few_shots.yaml"


# ------------------------------------------------------------- helpers
def strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", " ", s).strip()


def domain_of(url: str) -> str:
    h = urlparse(url).netloc.lower()
    return h[4:] if h.startswith("www.") else h


def path_pattern(url: str) -> str:
    """Abstract /2026/04/13/title → /YYYY/MM/DD/_slug_; keep top segment otherwise."""
    parts = urlparse(url).path.strip("/").split("/")
    out: list[str] = []
    for p in parts[:4]:
        if re.fullmatch(r"20\d{2}", p):
            out.append("YYYY")
        elif re.fullmatch(r"\d{1,2}", p):
            out.append("NN")
        elif re.fullmatch(r"[0-9a-f-]{8,}", p) or p.isdigit():
            out.append("_id_")
        elif "-" in p or len(p) > 20:
            out.append("_slug_")
        else:
            out.append(p)
    if not out:
        return "/"
    return "/" + "/".join(out)


DATE_FMT_CANDIDATES = ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y")


def parse_dt(s: str) -> datetime | None:
    s = s.strip()
    if not s:
        return None
    for fmt in DATE_FMT_CANDIDATES:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


# ------------------------------------------------------------- read rows
def read_published(svc) -> list[list[str]]:  # type: ignore[no-untyped-def]
    resp = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"'{PUBLISHED_TAB}'")
        .execute()
    )
    return resp.get("values", [])


def col_index(header: list[str]) -> dict[str, int]:
    out = {}
    for i, h in enumerate(header):
        out[h.strip()] = i
    return out


# ------------------------------------------------------------- main
def main() -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    rows = read_published(svc)
    if not rows:
        print(f"{PUBLISHED_TAB} is empty.", file=sys.stderr)
        return 2
    header, data = rows[0], rows[1:]
    idx = col_index(header)
    print(f"Published rows: {len(data)}  (header cols: {len(header)})")

    def g(row: list[str], key: str) -> str:
        i = idx.get(key, -1)
        return row[i].strip() if 0 <= i < len(row) else ""

    # --- basic counters
    sections = Counter(g(r, "Раздел") for r in data)
    regions = Counter(g(r, "Region") for r in data)
    countries = Counter(g(r, "Country") for r in data)
    activity = Counter(g(r, "Активность") for r in data)

    # --- URL analytics
    domains: Counter[str] = Counter()
    url_patterns: Counter[str] = Counter()
    for r in data:
        url = g(r, "Outer Link")
        if not url.startswith(("http://", "https://")):
            continue
        domains[domain_of(url)] += 1
        url_patterns[path_pattern(url)] += 1

    # --- brands in titles
    brands = load_brand_domains()
    brand_hits: Counter[str] = Counter()
    for r in data:
        title = (g(r, "Название") + " " + g(r, "Заголовок локализованный")).lower()
        for b in brands:
            needles = [b.brand.lower(), *(a.lower() for a in b.aliases)]
            if any(n in title for n in needles):
                brand_hits[b.brand] += 1

    # --- headline length stats
    lengths = [len(g(r, "Название")) for r in data if g(r, "Название")]
    avg_len = round(sum(lengths) / len(lengths)) if lengths else 0

    # --- hour of day
    hour_hist: Counter[int] = Counter()
    for r in data:
        dt = parse_dt(g(r, "Дата создания"))
        if dt:
            hour_hist[dt.hour] += 1

    # --- bullets length (a proxy for content depth)
    bullets_lens = [len(strip_html(g(r, "Bullets"))) for r in data if g(r, "Bullets")]
    avg_bullets = round(sum(bullets_lens) / len(bullets_lens)) if bullets_lens else 0

    # --- few-shot examples per section (balanced, avoid near-duplicates)
    per_section: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen_titles: set[str] = set()
    for r in data:
        sec = g(r, "Раздел")
        if not sec:
            continue
        title = g(r, "Название")
        if not title or title[:60].lower() in seen_titles:
            continue
        seen_titles.add(title[:60].lower())
        region = g(r, "Region") or None
        if len(per_section[sec]) < 8:
            per_section[sec].append(
                {"title": title, "region": region or "", "url": g(r, "Outer Link")}
            )

    # -------------------------------------------------- print summary
    print("\n=== Разделы ===")
    for sec, n in sections.most_common():
        print(f"  {sec:30} {n:5} ({n/len(data):.1%})")

    print("\n=== Регион / страна ===")
    for r, n in regions.most_common():
        print(f"  region={r!r:10} {n:5}")
    for c, n in countries.most_common():
        print(f"  country={c!r:20} {n:5}")

    print("\n=== Активность ===")
    for a, n in activity.most_common():
        print(f"  {a!r:8} {n:5}")

    print(f"\n=== Топ-30 источников (доменов) ===  (всего уникальных: {len(domains)})")
    for d, n in domains.most_common(30):
        print(f"  {d:40} {n:5}")

    print("\n=== Топ-20 URL-паттернов ===")
    for p, n in url_patterns.most_common(20):
        print(f"  {p:40} {n:5}")

    print(f"\n=== Топ-25 брендов (в заголовках) ===")
    for b, n in brand_hits.most_common(25):
        print(f"  {b:20} {n:5}")

    print(f"\n=== Заголовки ===")
    print(f"  средняя длина: {avg_len} симв., медиана тела (буллеты): {avg_bullets} симв.")

    print(f"\n=== Час создания (UTC+03, по \"Дата создания\") ===")
    for h in range(24):
        n = hour_hist.get(h, 0)
        bar = "#" * (n // max(1, max(hour_hist.values()) // 30))
        print(f"  {h:02}:00  {n:5}  {bar}")

    # -------------------------------------------------- write summary tab
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    tabs = [s["properties"]["title"] for s in meta["sheets"]]
    if SUMMARY_TAB not in tabs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SUMMARY_TAB}}}]},
        ).execute()

    out_rows: list[list[Any]] = []
    out_rows.append(["=== АНАЛИЗ 'Новости опубликованные' ==="])
    out_rows.append([f"Строк: {len(data)}  Дата анализа (UTC): {datetime.utcnow().isoformat(timespec='seconds')}"])
    out_rows.append([])

    out_rows.append(["РАЗДЕЛЫ", "Кол-во", "Доля"])
    for sec, n in sections.most_common():
        out_rows.append([sec, n, f"{n/len(data):.1%}"])
    out_rows.append([])

    out_rows.append(["РЕГИОН", "Кол-во"])
    for r, n in regions.most_common():
        out_rows.append([r, n])
    out_rows.append([])

    out_rows.append(["СТРАНА", "Кол-во"])
    for c, n in countries.most_common():
        out_rows.append([c, n])
    out_rows.append([])

    out_rows.append(["ИСТОЧНИКИ (топ-50)", "Кол-во"])
    for d, n in domains.most_common(50):
        out_rows.append([d, n])
    out_rows.append([])

    out_rows.append(["URL-ПАТТЕРНЫ (топ-30)", "Кол-во"])
    for p, n in url_patterns.most_common(30):
        out_rows.append([p, n])
    out_rows.append([])

    out_rows.append(["БРЕНДЫ (топ-40 в заголовках)", "Кол-во"])
    for b, n in brand_hits.most_common(40):
        out_rows.append([b, n])
    out_rows.append([])

    out_rows.append(["ЧАС СОЗДАНИЯ", "Кол-во"])
    for h in range(24):
        out_rows.append([f"{h:02}:00", hour_hist.get(h, 0)])
    out_rows.append([])

    out_rows.append(["FEW-SHOT ПРИМЕРЫ ПО РАЗДЕЛАМ", "Регион", "Заголовок"])
    for sec in sorted(per_section.keys()):
        for ex in per_section[sec]:
            out_rows.append([sec, ex["region"], ex["title"]])
        out_rows.append([])

    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=f"'{SUMMARY_TAB}'"
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{SUMMARY_TAB}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": out_rows},
    ).execute()
    print(f"\nSummary written to tab: {SUMMARY_TAB!r}")

    # -------------------------------------------------- save few-shots yaml
    FEW_SHOTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    few_shots = {
        "sections": {
            sec: [{"title": e["title"], "region": e["region"]} for e in exs]
            for sec, exs in per_section.items()
        }
    }
    with FEW_SHOTS_PATH.open("w", encoding="utf-8") as f:
        yaml.safe_dump(few_shots, f, allow_unicode=True, sort_keys=True)
    print(f"Few-shots saved to {FEW_SHOTS_PATH.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
