"""The ONE canonical schema of the 'ТЕСТ статьи vN' articles tab.

Written by batch_fetch_test.write_articles(); read positionally by the
cluster/push/retry/backfill scripts. Before this module every consumer
re-declared the column indices by hand (~10 scripts), and they had already
drifted (retry_failed_llm carried COL_LLM_REL=24 pointing at 'Hits темы') —
the same failure family as the A1:R6000 archive truncation: a mid-header
insert silently shifts every hand-copied index and wrong data flows to the
editor with no error.

Rules:
  * Add new columns AT THE END (the AC/AD/AE/AF-AH blocks were appended
    exactly this way). Never insert mid-list.
  * Named indices below are DERIVED from the list via .index() — they cannot
    drift from the header. Import them instead of writing integers.
  * Readers should call check_header() on the tab's actual header row and
    fail loudly on a mismatch instead of consuming shifted columns.
"""

from __future__ import annotations

# Blocks (colour-coded in the sheet):
#   Block 1 (light green):  "Что за новость"   — columns A-I
#   Block 2 (light blue):   "Первоисточник"    — columns J-L
#   Block 3 (light yellow): "Для редактора"    — columns M-Q
#   Block 4 (light grey):   "Отладка" (hidden) — columns R-AB
#   Phase-1 lifecycle + editorial reason + event-signature — AC-AH
ARTICLES_HEADER = [
    # --- Block 1: «Что за новость» --------------------------------------
    "Прогон (UTC)",                    # A
    "Заголовок (EN / RU)",             # B  combined EN+RU (+ source lang tag)
    "Лид",                             # C  first ~300 chars of body
    "URL статьи",                      # D
    "Раздел",                          # E  LLM section (+ "(неактивный)" for Test-drive)
    "Регион",                          # F  Local / Global
    "Страна",                          # G  Russia / Uzbekistan / Kazakhstan
    "Дата публикации",                 # H
    "Картинка (URL)",                  # I  full image URL, not just a flag
    # --- Block 2: «Первоисточник» ---------------------------------------
    "Первоисточник (домен)",           # J
    "Первоисточник URL",               # K
    "Уверенность источника",           # L  high / medium / low
    # --- Block 3: «Для редактора» ---------------------------------------
    "Пометка бота",                    # M
    "Confidence раздела",              # N  0.00-1.00
    "Итог бота",                       # O  ← colour formatting column
    "Ручная проверка (Новость / Не новость)",  # P
    "Комментарий",                     # Q
    # --- Block 4: «Отладка» (hidden by default) --------------------------
    "URL источника",                   # R
    "№ ист.",                          # S
    "№ ст.",                           # T
    "Тело (симв)",                     # U
    "is_article",                      # V
    "is_article score",                # W
    "Причины is_article",              # X
    "Hits темы",                       # Y
    "LLM relevance",                   # Z
    "Стоимость LLM, $",                # AA
    "Способ поиска источника",         # AB
    # --- Phase-1 launch lifecycle (visible after Block 4) ---------------
    "Стадия запуска",                  # AC
    "Бренд + модель",                  # AD
    # --- LLM editorial review reason ------------------------------------
    "Обоснование LLM",                 # AE
    # --- Hybrid dedup Stage 1: semantic event-signature -----------------
    "event_brand",                     # AF
    "event_model",                     # AG
    "event_type",                      # AH
]

_i = ARTICLES_HEADER.index


class COL:
    """Named column indices, derived from ARTICLES_HEADER (cannot drift)."""

    RUN = _i("Прогон (UTC)")                          # 0  A
    TITLE = _i("Заголовок (EN / RU)")                 # 1  B
    LEDE = _i("Лид")                                  # 2  C
    URL = _i("URL статьи")                            # 3  D
    SECTION = _i("Раздел")                            # 4  E
    REGION = _i("Регион")                             # 5  F
    COUNTRY = _i("Страна")                            # 6  G
    PUBLISHED = _i("Дата публикации")                 # 7  H
    IMAGE = _i("Картинка (URL)")                      # 8  I
    PRIMARY_DOM = _i("Первоисточник (домен)")         # 9  J
    PRIMARY_URL = _i("Первоисточник URL")             # 10 K
    PRIMARY_CONF = _i("Уверенность источника")        # 11 L
    NOTE = _i("Пометка бота")                         # 12 M
    CONFIDENCE = _i("Confidence раздела")             # 13 N
    VERDICT = _i("Итог бота")                         # 14 O
    MANUAL_CHECK = _i("Ручная проверка (Новость / Не новость)")  # 15 P
    COMMENT = _i("Комментарий")                       # 16 Q
    SOURCE_URL = _i("URL источника")                  # 17 R
    LLM_RELEVANCE = _i("LLM relevance")               # 25 Z
    COST = _i("Стоимость LLM, $")                     # 26 AA
    LAUNCH_STAGE = _i("Стадия запуска")               # 28 AC
    LAUNCH_BRAND_MODEL = _i("Бренд + модель")         # 29 AD
    LLM_REASON = _i("Обоснование LLM")                # 30 AE
    EVENT_BRAND = _i("event_brand")                   # 31 AF
    EVENT_MODEL = _i("event_model")                   # 32 AG
    EVENT_TYPE = _i("event_type")                     # 33 AH


def col_letter(idx: int) -> str:
    """0-based column index → A1 letter ('0'→'A', '26'→'AA')."""
    if idx < 0:
        raise ValueError(f"column index must be >= 0, got {idx}")
    out = ""
    n = idx + 1
    while n > 0:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


# The full read range for one row of this schema ("A1:AH").
FULL_RANGE_COLS = f"A:{col_letter(len(ARTICLES_HEADER) - 1)}"


def check_header(header_row: list, *, context: str = "") -> list[str]:
    """Validate a tab's actual header row against the canonical schema.

    Returns a list of problems (empty = ok). A SHORTER header is tolerated
    (older tabs pre-date the appended AC-AH columns); a NAME MISMATCH at any
    present position is the deadly case — someone inserted/renamed a column
    and every positional read after it is silently wrong. Callers should
    fail loudly on problems rather than consume shifted data.
    """
    problems: list[str] = []
    for pos, cell in enumerate(header_row):
        name = str(cell).strip()
        if pos >= len(ARTICLES_HEADER):
            problems.append(
                f"{context}: unexpected extra column {col_letter(pos)}={name!r}")
            continue
        if name and name != ARTICLES_HEADER[pos]:
            problems.append(
                f"{context}: column {col_letter(pos)} is {name!r}, schema says "
                f"{ARTICLES_HEADER[pos]!r} — positional reads after this point "
                f"are WRONG")
    return problems
