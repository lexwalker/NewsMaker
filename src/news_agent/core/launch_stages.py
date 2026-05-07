"""Model-launch lifecycle tracking — Phase 1 (heuristic detection only).

Editor's mental model of an OEM model launch (apr-2026 conversation):

  1. certification         — type approval / VTA in source market
  2. images_published      — first official images released
  3. debut_announced       — pre-event teaser ("will debut on X")
  4. model_unveiled        — model officially shown
  5. preorders_announced   — pre-orders date set
  6. preorders_started     — pre-orders open
  7. sales_announced       — sales date set
  8. sales_started         — sales actually begin

Real Geely Galaxy M7 timeline (the case that prompted this feature):
    2025-12-09  certification
    2026-02-02  images_published
    2026-03-10  debut_announced
    2026-03-16  model_unveiled
    2026-04-07  preorders_announced
    2026-04-09  preorders_started
    2026-05-05  sales_started  (today's planned article)

Phase 1 = heuristic detection only. We populate two columns in the sheet:
  - "Стадия" (e.g. "model_unveiled, preorders_started")
  - "Бренд + модель" (e.g. "Geely Galaxy M7")

Phase 2 will add SQLite persistence + soft same-stage repeat detection.
"""

from __future__ import annotations

import re
from typing import Literal

LaunchStage = Literal[
    "certification",
    "images_published",
    "debut_announced",
    "model_unveiled",
    "preorders_announced",
    "preorders_started",
    "sales_announced",
    "sales_started",
]


# Order matters: most specific (latest in lifecycle) checked FIRST so that
# multi-stage articles get the latest stage as primary. Each stage has a
# list of trigger phrases (RU + EN). Substring match on lower-cased text.
_STAGE_PHRASES: list[tuple[LaunchStage, list[str]]] = [
    ("sales_started", [
        # russian — multiple verb forms (зафиксированные в нашей корпус-выборке)
        "стартовали продажи", "начались продажи",
        "поступила в продажу", "поступил в продажу", "поступили в продажу",
        "пошли в продажу", "запущены продажи",
        "запустила продажи", "запустил продажи", "запустили продажи",
        "стартовала продаж", "стартовал продаж",
        "продажи стартовали", "продажи начались",
        "начали продавать", "начал продавать", "начала продавать",
        "стал доступен", "стала доступна", "стало доступно",
        "доступен в россии", "доступна в россии",
        "доступен в рф", "доступна в рф",
        "вышел на рынок", "вышла на рынок", "вышли на рынок",
        "вывели на рынок", "вывел на рынок",
        "появился в продаже", "появилась в продаже",
        # english
        "sales started", "went on sale", "now on sale",
        "sales launched", "sales begin", "sales kicked off",
        "officially on sale",
        "now available", "available in russia", "available in china",
        "available in europe", "available in the u.s.",
        "available in india", "available in uae",
        "market entry",
        "available for sale",
    ]),
    ("sales_announced", [
        "анонсировали старт продаж", "анонсировал начало продаж",
        "объявили дату начала продаж",
        "to start sales on", "sales to begin",
        "to launch sales",
    ]),
    ("preorders_started", [
        "стартовал приём предзаказов", "стартовал прием предзаказов",
        "стартовала приём предзаказов",
        "открыли предзаказ", "открыли приём предзаказов",
        "открыли прием предзаказов",
        "начали приём предзаказов", "начали прием предзаказов",
        "начался приём", "начался прием",
        "запустили приём предзаказов",
        "стартует приём предзаказов",
        "pre-orders started", "pre-orders open",
        "now accepting pre-orders",
        "preorders started",
        "open for pre-orders", "open for preorders",
    ]),
    ("preorders_announced", [
        "анонсировали старт приёма предзаказов",
        "анонсировали старт приема предзаказов",
        "анонсировали приём предзаказов",
        "анонсировали приёма предзаказов",
        "to start pre-orders on", "pre-orders to begin",
        "pre-orders to start",
    ]),
    ("model_unveiled", [
        # russian — "В Geely представили..."
        "представили",
        "официально представили",
        "представил миру",
        # english
        "unveiled", "introduced",
        "дебютировал", "debuted at", "made debut",
        "showed the", "showcased the",
        "world premiere",
    ]),
    ("debut_announced", [
        "анонсировали дебют", "анонсировал дебют", "анонсировала дебют",
        "анонсировали премьеру",
        "to debut on", "will debut",
        "world premiere date",
        "premiere set for", "scheduled to debut",
    ]),
    ("images_published", [
        "опубликовали изображения", "опубликовали официальные изображения",
        "опубликовали интерьер", "опубликовали тизер",
        "поделились изображениями", "поделились первыми изображениями",
        "first images of", "first official images",
        "shared images of", "released images",
        "published images", "released the first images",
        "released teaser",
    ]),
    ("certification", [
        "сертифицирован", "сертифицировали",
        "получил сертификат", "получила сертификат",
        "получил оттс", "получила оттс",
        "received certification", "certified in",
        "passed certification",
        "vehicle type approval",
    ]),
]


def detect_launch_stages(title: str, body: str = "") -> list[LaunchStage]:
    """Return launch stages detected in title + first 300 chars of body.

    Returns an empty list — never None — for easy chaining. Latest
    lifecycle stage first when multiple match.
    """
    if not title:
        return []
    text = (title + " " + (body or "")[:300]).lower()
    found: list[LaunchStage] = []
    for stage, phrases in _STAGE_PHRASES:
        if any(p.lower() in text for p in phrases):
            found.append(stage)
    return found


# ============================================================================
# Brand + model extraction
# Used together with stage detection: a "stage" without a (brand, model)
# pair is dropped — prevents "Toyota запустила завод" being tagged as
# sales_started.
# ============================================================================

_MODEL_NOISE_TOKENS: frozenset[str] = frozenset({
    # body types
    "SUV", "SUVs", "EV", "EVs", "BEV", "BEVs", "PHEV", "PHEVs",
    "MPV", "MPVs", "NEV", "NEVs", "ICE", "AWD", "RWD", "FWD", "PWR",
    "Crossover", "Sedan", "Sedans", "Hatchback", "Pickup",
    "Coupe", "Wagon", "Truck", "Trucks", "Van", "Vans", "Minivan",
    # geographic
    "Russia", "China", "Europe", "America", "Korea",
    "Japan", "USA", "EU", "UK", "UAE", "India",
    "Germany", "France", "Italy", "Spain", "Mexico", "Brazil",
    "Indonesia", "Thailand", "Vietnam", "Beijing", "Shanghai",
    "Tokyo", "Seoul", "Moscow", "Munich", "Berlin",
    "Canada", "Australia", "Norway", "Sweden", "Belgium",
    # role / corporate / tech acronyms
    "CEO", "CFO", "CTO", "COO", "CMO", "CIO", "VP", "AI",
    "IT", "OTA", "USB", "API", "OEM",
    # months / common quarters
    "Q1", "Q2", "Q3", "Q4",
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    # common English words that appear capitalized in title-case
    # (English headlines capitalize most words → title-case noise)
    "Would", "Like", "Word", "Yet", "Will", "Should", "Could",
    "Most", "Best", "Worst", "Top", "Cheapest", "Largest", "Biggest",
    "Smaller", "Bigger", "Better", "Worse", "First", "Last", "Next",
    "New", "Old", "Recent", "Latest", "Upcoming",
    "Year", "Years", "Month", "Months", "Week", "Weeks", "Day", "Days",
    "Tested", "Available", "Coming", "Going", "Looks",
    "Sale", "Sales", "Price", "Cost",
    "Delays", "Delay", "Cancels", "Reveals", "Plans", "Plan",
    "Quietly", "Officially",
    "On", "At", "By", "Of", "To", "For", "In", "From", "With",
    "And", "Or", "But", "Now", "Then", "When", "Where", "Why",
    "Why", "How", "What", "Who",
    "Marginally", "Improved",
    # automotive industry generic words
    "Lineup", "Engine", "Engines", "Motor", "Motors", "Drive",
    "Edition", "Special", "Limited",
    "Concept", "Prototype",
    "Yearly",
})


def _is_valid_model_token(tok: str) -> bool:
    """Token qualifies as a model-name component when:
      • not in the noise set,
      • not pure digits (year),
      • alphanumeric model code (M7, X5, GV90) OR word ≥ 4 chars
        (Galaxy, Tucson, Octavia).
    """
    if not tok:
        return False
    if tok in _MODEL_NOISE_TOKENS:
        return False
    if tok.isdigit():
        return False  # likely year
    has_digit = any(c.isdigit() for c in tok)
    has_letter = any(c.isalpha() for c in tok)
    if not has_letter:
        return False
    if has_digit and len(tok) >= 2:
        return True   # alphanumeric model code
    return len(tok) >= 4


_MODEL_TOKEN_RE = re.compile(r"[A-Z][\w\-]*", re.UNICODE)


def extract_brand_model(
    title: str,
    brands: list,
) -> tuple[str, str] | None:
    """Find a (brand, model) pair in the title.

    Logic:
      1. Find the EARLIEST brand mention in title (case-insensitive,
         word-boundary check) using brand.brand + brand.aliases.
      2. After the brand, scan up to 80 chars for the next 1-3 tokens
         that qualify as model name components.
      3. Stop on noise tokens / lowercase / sentence end.

    Returns None if no brand found OR no valid model tokens after brand.

    Examples:
        "В Geely анонсировали Galaxy M7"     → ("Geely", "Galaxy M7")
        "Geely Galaxy M7 sales started"      → ("Geely", "Galaxy M7")
        "Hyundai unveiled the new Tucson"    → ("Hyundai", "Tucson")
        "Toyota запустила завод в Тольятти"  → None  (no model token)
    """
    if not title or not brands:
        return None
    tl = title.lower()

    found_brand_canonical: str | None = None
    found_pos = -1
    found_match_len = 0
    for b in brands:
        names: list[str] = [b.brand]
        names += list(getattr(b, "aliases", []) or [])
        for name in names:
            if not name or len(name) < 3:
                continue
            pat = re.compile(
                r"(?<!\w)" + re.escape(name.lower()) + r"(?!\w)"
            )
            m = pat.search(tl)
            if m and (found_pos == -1 or m.start() < found_pos):
                found_brand_canonical = b.brand
                found_pos = m.start()
                found_match_len = len(name)

    if found_brand_canonical is None:
        return None

    after = title[found_pos + found_match_len:][:80]
    candidates = _MODEL_TOKEN_RE.findall(after)

    # Brand-self-match guard: don't accept the brand name itself as a model
    # token. Common case: "Lexus unveiled TZ" — short model "TZ" rejected by
    # _is_valid_model_token, scan continues into the RU line where "Lexus"
    # appears again, would extract ("Lexus", "Lexus"). Build a set of forms
    # to compare against.
    brand_forms_lower: set[str] = set()
    for b in brands:
        if b.brand == found_brand_canonical:
            brand_forms_lower.add(b.brand.lower())
            for a in (getattr(b, "aliases", []) or []):
                brand_forms_lower.add(a.lower())
            break

    model_tokens: list[str] = []
    for tok in candidates[:5]:
        if tok.lower() in brand_forms_lower:
            # Repeated brand mention (RU/EN multi-line title) — skip.
            if model_tokens:
                break
            continue
        if _is_valid_model_token(tok):
            model_tokens.append(tok)
            if len(model_tokens) >= 3:
                break
        elif model_tokens:
            # Already captured tokens but hit a noise / invalid token — stop.
            break

    if not model_tokens:
        return None
    return (found_brand_canonical, " ".join(model_tokens))
