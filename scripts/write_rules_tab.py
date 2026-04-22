"""Build the 'Правила для ИИ' tab — full whitelist + blacklist + section
definitions, readable for both humans and the editorial team.

The tab is rebuilt from scratch on every run from the current config/*.yaml
files. Editors can read it; changes to the rules go via the YAML files
(and then this script re-syncs the tab).
"""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
load_dotenv(ROOT / ".env", override=True)

from news_agent.core.config_loader import (  # noqa: E402
    load_blacklist,
    load_brand_domains,
    load_primary_source_cues,
    load_sections,
    load_whitelist_domains,
)

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TAB = "Правила для ИИ"

# Column widths (pixels)
COL_WIDTHS = {0: 220, 1: 260, 2: 420, 3: 260}


def _sheets_client():  # type: ignore[no-untyped-def]
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def main() -> int:
    svc = _sheets_client()

    # Ensure tab exists; else create it
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    tabs_by_title = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta["sheets"]}
    if TAB not in tabs_by_title:
        resp = svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TAB}}}]},
        ).execute()
        sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    else:
        sheet_id = tabs_by_title[TAB]

    # Clear existing contents
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=f"'{TAB}'"
    ).execute()

    # Load configs
    sections = load_sections()
    brands = load_brand_domains()
    cues = load_primary_source_cues()
    whitelist = sorted(load_whitelist_domains())
    blacklist = load_blacklist()

    rows: list[list[Any]] = []
    section_starts: list[tuple[int, str]] = []  # row_index (0-based), colour tag

    def add_section_header(title: str, colour: str) -> None:
        rows.append([])
        rows.append([title])
        section_starts.append((len(rows) - 1, colour))

    def add_table_header(*cols: str) -> None:
        rows.append(list(cols))
        # caller marks styling via separate index list

    # ---- TITLE ----
    rows.append(["Правила для ИИ / редакции"])  # row 0
    title_row = 0
    rows.append([
        "Этот лист — полный источник истины по правилам. Добавляется ботом "
        "автоматически на основе YAML-конфигов. Если хочешь поменять правило — "
        "скажи ассистенту какое именно, он обновит соответствующий YAML и перезапишет лист."
    ])
    intro_row = 1
    rows.append([])

    table_header_rows: list[int] = []

    # ---- 1. РАЗДЕЛЫ ----
    add_section_header("1. Разделы (sections)", "header_blue")
    add_table_header("Раздел", "Ключевое правило", "Что сюда идёт", "Что НЕ сюда")
    table_header_rows.append(len(rows) - 1)
    # Hand-curated "в двух словах" + "не сюда" rules, derived from sections.yaml.
    section_quick: dict[str, tuple[str, str, str]] = {
        "Confirmed": (
            "Подтверждённый факт про модель/бренд",
            "анонс модели, старт продаж, концепт, патент, раскрытие салона, регистрация "
            "товарного знака в РФ, мониторинг (новая комплектация/коробка/версия 4WD)",
            "акции, trade-in, дилерские сети",
        ),
        "Local specifics": (
            "Привязано к стране портала (РФ)",
            "льготное автокредитование, утильсбор, пошлины, решения Минпромторга/Госдумы, "
            "дорожное строительство, локализация в РФ, региональная статистика продаж",
            "события в Узбекистане / Казахстане на RU-портале — туда идут в Other news",
        ),
        "Other news": (
            "Всё авто-релевантное вне других разделов",
            "зарядная инфра + автобренд, АКБ/чипы/ПО, партнёрства со стартапами, краш-тесты "
            "EuroNCAP/C-NCAP, рейтинги JD Power, судебные споры, покупка активов, отзывы в других "
            "странах, масштабная международная регуляторика, регулярные отчёты продаж по миру",
            "автобусы, спецтехника, сельхоз, цены на литий/кобальт (см. blacklist)",
        ),
        "Rumors": (
            "Утечки, слухи, leaks",
            "«по данным источников», «СМИ пишут», «в сети появились рендеры», «reportedly»",
            "официальные анонсы брендов — это Confirmed",
        ),
        "Economics": (
            "Экономика автобизнеса + макро",
            "статистика продаж AEB/АВТОСТАТ, финотчёты автопроизводителей, курсы ЦБ РФ и "
            "Мосбиржи, Brent/WTI, прогнозы ОПЕК/МАЭ, добыча нефти РФ за квартал/год, "
            "потребкредиты+автокредиты ВМЕСТЕ",
            "чистые автокредиты — это Local specifics; рынок акций — не берём",
        ),
        "LCV news": (
            "LCV ≤ 3.5 т",
            "фургоны, мелкие пикапы, лёгкие грузовики",
            "автобусы, тяжёлые грузовики, спецтехника, сельхоз — НЕ сюда (в blacklist)",
        ),
        "Test-drive": (
            "Развёрнутые обзоры/тест-драйвы",
            "yearlong reviews MotorTrend, YouTube-обзоры «За рулём», longform тесты",
            "мусорные список-статьи вроде «5 best cars»",
        ),
        "Dealer news / Promo": (
            "Дилерская сеть + маркетинг",
            "открытие/закрытие шоурумов, дилерские соглашения, акции с розыгрышами и "
            "условиями, trade-in, специальные кредитные условия, интервью с дилерами",
            "новость про саму модель — это Confirmed",
        ),
        "Motorshow": (
            "Крупные автосалоны",
            "Шанхай, Мюнхен, Детройт, CES auto, ММАС — анонс/репортаж/итог",
            "CES в целом (не-автомобильный) — не сюда",
        ),
    }
    for sec in sections:
        q = section_quick.get(sec.name)
        if q:
            rows.append([sec.name, q[0], q[1], q[2]])
        else:
            rows.append([sec.name, sec.description[:150].replace("\n", " "), "", ""])

    # ---- 2. WHITELIST ----
    add_section_header("2. Whitelist (что приоритетно берём)", "header_green")
    add_table_header("Тип правила", "Значение", "Как применяется", "Источник решения")
    table_header_rows.append(len(rows) - 1)

    # 2a. Trusted domains
    rows.append([
        "Доверенный домен (бост score)",
        ", ".join(whitelist[:6]) + (f", +ещё {len(whitelist)-6}" if len(whitelist) > 6 else ""),
        "+0.15 к article_score (из whitelist_domains.yaml)",
        "Топ-30 доменов из 2989 опубликованных",
    ])
    # 2b. Brands
    brand_sample = ", ".join(b.brand for b in brands[:12])
    rows.append([
        "Бренд в заголовке → авто-тема",
        f"{len(brands)} брендов (пример: {brand_sample}, …)",
        "Тематический фильтр считает хит, строка попадает в «возможно новость»",
        "brand_domains.yaml",
    ])
    # 2c. URL article patterns
    rows.append([
        "Позитивные URL-паттерны",
        "/news/, /article/, /mag/article/, /ekonomika/, /business/, /doc/, /ab/news/, "
        "/obschestvo/, /infographics/, /YYYY/MM/DD/",
        "+0.1 к article_score",
        "Из топ-20 URL-паттернов опубликованных 2989 новостей",
    ])
    # 2d. Topic keywords
    rows.append([
        "Ключевые слова темы (RU)",
        "автомобил, автопром, авторынок, дилерск, двигатель, электромобил, гибрид, "
        "модельный ряд, комплектаци, тойота, лада, бмв, чери, хавейл, джили, ...",
        "Срабатывание добавляет хит в тематический фильтр",
        "heuristic_relevance.py (_AUTO_KEYWORDS_RU)",
    ])
    rows.append([
        "Ключевые слова темы (EN)",
        "automotive, sedan, suv, crossover, pickup, ev, hybrid, dealer, horsepower, "
        "toyota, bmw, tesla, byd, …",
        "Срабатывание добавляет хит в тематический фильтр",
        "heuristic_relevance.py (_AUTO_KEYWORDS_EN)",
    ])
    # 2e. Макро-новости
    rows.append([
        "Макро-новости (Q1 Рита)",
        "курсы ЦБ РФ / Мосбиржа / Brent / WTI / прогнозы ОПЕК / МАЭ",
        "LLM относит в Economics. Ставить регулярный сбор после 14:00 из "
        "Финмаркет / Интерфакс / Прайм — отдельный пайплайн.",
        "Q1 ответ: Рита",
    ])
    # 2f. Автокредиты
    rows.append([
        "Автокредиты в РФ (Q1 Рита)",
        "льготное автокредитование, льготный лизинг, утильсбор, автокредиты",
        "LLM относит в Local specifics. Если вместе с потребкредитами — Economics.",
        "Q1 ответ: Рита",
    ])
    # 2g. EV смежное
    rows.append([
        "EV-смежная индустрия (Q2 Ольга+Д)",
        "зарядная инфраструктура, зарядные хабы, АКБ (батареи), чипы, ПО для авто",
        "LLM относит в Other news (при условии — связано с автобрендом)",
        "Q2 ответы: Д, Ольга",
    ])
    # 2h. Регуляторика
    rows.append([
        "Регуляторика РФ (Q7 Рита)",
        "льготные программы, пошлины, квоты на импорт, регулирование движения",
        "LLM относит в Local specifics",
        "Q7 ответ: Рита",
    ])
    rows.append([
        "Масштабная международная регуляторика (Q7 Д)",
        "ЕС директивы, тарифы Трампа на Китай",
        "LLM относит в Other news (только если затрагивают крупные рынки)",
        "Q7 ответ: Д",
    ])

    # ---- 3. BLACKLIST ----
    add_section_header("3. Blacklist (что точно НЕ берём)", "header_red")
    add_table_header("Категория", "Что отсеиваем", "Как срабатывает", "Источник решения")
    table_header_rows.append(len(rows) - 1)

    rows.append([
        "Тяжёлый транспорт / автобусы",
        ", ".join(
            p for p in (
                blacklist.topic_phrases_ru + blacklist.topic_phrases_en
            ) if any(k in p for k in ("автобус", "троллейбус", "электробус", "bus", "coach"))
        ),
        "Фраза в ЗАГОЛОВКЕ → reject до LLM. В теле — не срабатывает "
        "(чтобы не терять статьи про авторынок, где автобусы упоминаются мимоходом).",
        "Q3: Ольга, Рита",
    ])
    rows.append([
        "Спецтехника / строительная",
        ", ".join(
            p for p in (
                blacklist.topic_phrases_ru + blacklist.topic_phrases_en
            ) if any(k in p for k in ("спецтехник", "экскаватор", "бульдозер", "самосвал",
                                       "карьерный", "construction", "excavator", "bulldozer",
                                       "dump truck"))
        ),
        "Фраза в ЗАГОЛОВКЕ → reject до LLM",
        "Q3: Рита",
    ])
    rows.append([
        "Сельхозтехника",
        ", ".join(
            p for p in (
                blacklist.topic_phrases_ru + blacklist.topic_phrases_en
            ) if any(k in p for k in ("трактор", "комбайн", "сельхоз", "tractor",
                                       "harvester", "agricultural", "farm equipment"))
        ),
        "Фраза в ЗАГОЛОВКЕ → reject до LLM",
        "Q3: Рита",
    ])
    rows.append([
        "Сырьё для батарей (цены)",
        ", ".join(
            p for p in (
                blacklist.topic_phrases_ru + blacklist.topic_phrases_en
            ) if any(k in p for k in ("литий", "кобальт", "никель", "lithium", "cobalt",
                                       "nickel", "battery minerals"))
        ),
        "Фраза в ЗАГОЛОВКЕ → reject до LLM. Новости про технологии батарей — "
        "НЕ сюда, они в Other news.",
        "Q2: Ольга",
    ])
    rows.append([
        "Домены (целиком отказ)",
        ", ".join(blacklist.domains) if blacklist.domains else "(пусто)",
        "Если домен статьи совпадает — reject до LLM",
        "blacklist.yaml",
    ])
    rows.append([
        "Non-article URL-паттерны",
        "/search, /tag/, /category/, /archive, /insights/, /five-minutes-with-, "
        "/infographics/puteshestvuem-, /infocenter/autoarticles/, /how-to-, "
        "?camefrom=, /privacy, /terms, .pdf / .doc / .xls",
        "Score −0.5, статья валится в «Точно не новость (не статья)»",
        "heuristic_relevance.py (_NON_ARTICLE_URL_HINTS)",
    ])
    rows.append([
        "Op-ed заголовки (6 ответ)",
        "paradox of, time to rethink, rethinking, why chaos, defy the numbers, "
        "на грани, парадокс, переосмысление",
        "Score −0.6",
        "Пункт 6 от редактора",
    ])
    rows.append([
        "Лайфстайл-туризм (9 ответ)",
        "путешествуем, поездка из …, road trip, weekend trip",
        "Score −0.6",
        "Пункт 9 от редактора",
    ])

    # ---- 4. Gates до LLM ----
    add_section_header("4. Трёхуровневая градация до LLM", "header_grey")
    add_table_header("Уровень", "Когда", "Что делает LLM", "Цвет в таблицах ТЕСТ")
    table_header_rows.append(len(rows) - 1)
    rows.append([
        "🟢 Точно новость", "score ≥ 0.65 и тема авто + ≥2 тематич. хита",
        "Сразу classify_section + перевод, без relevance-check", "зелёный",
    ])
    rows.append([
        "🟡 Возможно новость", "score 0.35–0.65 или мало тематич. хитов",
        "Сначала дешёвый бинарный relevance-check, если Да → classify + перевод", "жёлтый",
    ])
    rows.append([
        "⚪ Точно не новость", "score < 0.35 или тема не авто, или blacklist-hit",
        "Не вызывается, экономия", "серый",
    ])
    rows.append([
        "🔵 Дубль", "Финальный URL уже видели в этом прогоне, "
        "ИЛИ был в SQLite-кеше из прошлых прогонов",
        "Не вызывается", "голубой",
    ])
    rows.append([
        "🔴 Ошибка", "HTTP 403/5xx или не удалось извлечь текст",
        "Не вызывается", "красный",
    ])

    # ---- 5. Свежесть ----
    add_section_header("5. Фильтр свежести", "header_grey")
    rows.append([
        "Берём только статьи не старше", f"{os.environ.get('FRESHNESS_HOURS', '48')} часов",
        "FRESHNESS_HOURS в .env. Применяется до эвристики и LLM.",
        "Пожелание пользователя",
    ])

    # ---- 6. Новые правила от редакторов (Ольга / Рита / Д / Эмма) ----
    add_section_header(
        "6. Новые правила от редакторов (все группы 1-7 закрыты)",
        "header_green",
    )
    add_table_header("Тема", "Решение", "Раздел", "Источник решения")
    table_header_rows.append(len(rows) - 1)
    new_rules = [
        ("Weekly summary продаж ТС (Cox Weekly и т.п.)",
         "Берём только для РФ", "Local specifics", "Д, группа 1"),
        ("Месячные/квартальные/годовые отчёты продаж по миру",
         "Берём", "Other news", "Д, группа 1"),
        ("ACEA: продажи LCV, продажи ЕС, Economic and Market Report",
         "Берём", "Other news", "Ольга, группа 3"),
        ("ACEA op-ed («time to rethink …»)", "Не берём", "(blacklist заголовка)",
         "Ольга, группа 3"),
        ("НАП: общие доли рынка", "Берём", "Economics / Local specifics",
         "Рита, группа 1"),
        ("НАП: изменения цен по моделям / инфографика",
         "Не берём", "—", "Рита, группа 1"),
        ("Прогнозы зарубежных цен (TrendForce и т.п.)",
         "Обычно не берём; крупные глобальные исследования — можно",
         "Other news (если берём)", "Ольга, группа 2"),
        ("Geotab blog", "Практически не используем (≈1 новость в год)",
         "—", "Ольга, группа 3"),
        ("Test-drive longform", "Берём с пометкой «требует ручной проверки»",
         "Test-drive (флаг)", "Ольга, группа 4"),
        ("Отзывные кампании (все рынки: РФ + зарубежные)",
         "Все берём", "Confirmed (если РФ) / Other news (если зарубежные)",
         "Ольга, группа 5"),
        ("Расширение дилерской сети (Газпромбанк, и т.п.)",
         "Берём", "Dealer news / Promo", "Ольга, группа 6"),
        ("Изменения юр. условий лизинга («общие условия»)",
         "Не берём", "(blacklist заголовка)", "Ольга, группа 6"),
        ("Мелкая корпоративная статистика (< месяц, напр. «за 2 недели»)",
         "Не берём; за месяц+ — можно рассмотреть",
         "Economics (если берём)", "Ольга, группа 6"),
        ("Годовые winners (Green NCAP category winners)",
         "Не берём; берём press-releases о конкретных пятизвёздочных моделях",
         "Other news", "Ольга, группа 7"),
        ("Музейные события (АВТОВАЗ и т.п.)",
         "Обычно нет; зависит от контекста", "—", "Ольга, группа 7"),
        ("Регистрация товарных знаков",
         "РФ → Local specifics; иностранные бренды на иностранных рынках → Other news",
         "Local specifics / Other news", "Ольга, строка 118"),
    ]
    for topic, decision, section, src in new_rules:
        rows.append([topic[:150], decision[:180], section[:100], src])

    # -------- Write values
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{TAB}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": rows},
    ).execute()

    # -------- Formatting (batchUpdate)
    requests: list[dict[str, Any]] = []

    def repeat_cell(
        row1: int, row2: int, col1: int, col2: int, fmt: dict[str, Any], fields: str
    ) -> None:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row1, "endRowIndex": row2,
                    "startColumnIndex": col1, "endColumnIndex": col2,
                },
                "cell": {"userEnteredFormat": fmt},
                "fields": fields,
            }
        })

    # Column widths
    for col, px in COL_WIDTHS.items():
        requests.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS",
                          "startIndex": col, "endIndex": col + 1},
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        })
    # Title
    repeat_cell(
        title_row, title_row + 1, 0, 4,
        {"backgroundColor": {"red": 0.17, "green": 0.24, "blue": 0.40},
         "textFormat": {"bold": True, "fontSize": 14,
                        "foregroundColor": {"red": 1, "green": 1, "blue": 1}}},
        "userEnteredFormat(backgroundColor,textFormat)",
    )
    # Intro row wrap
    repeat_cell(
        intro_row, intro_row + 1, 0, 4,
        {"textFormat": {"italic": True}, "wrapStrategy": "WRAP"},
        "userEnteredFormat(textFormat,wrapStrategy)",
    )
    # Section headers
    section_colour_map = {
        "header_blue":   {"red": 0.80, "green": 0.88, "blue": 1.00},
        "header_green":  {"red": 0.78, "green": 0.93, "blue": 0.80},
        "header_red":    {"red": 0.97, "green": 0.80, "blue": 0.80},
        "header_grey":   {"red": 0.88, "green": 0.88, "blue": 0.88},
        "header_yellow": {"red": 1.00, "green": 0.93, "blue": 0.72},
    }
    for idx, tag in section_starts:
        repeat_cell(
            idx, idx + 1, 0, 4,
            {"backgroundColor": section_colour_map[tag],
             "textFormat": {"bold": True, "fontSize": 12}},
            "userEnteredFormat(backgroundColor,textFormat)",
        )
    # Table header rows (bold grey)
    for idx in table_header_rows:
        repeat_cell(
            idx, idx + 1, 0, 4,
            {"backgroundColor": {"red": 0.85, "green": 0.85, "blue": 0.85},
             "textFormat": {"bold": True}, "wrapStrategy": "WRAP"},
            "userEnteredFormat(backgroundColor,textFormat,wrapStrategy)",
        )
    # Global wrap for body text
    repeat_cell(
        0, len(rows) + 2, 0, 4,
        {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"},
        "userEnteredFormat(wrapStrategy,verticalAlignment)",
    )
    # Freeze top 2 rows
    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 2}},
            "fields": "gridProperties.frozenRowCount",
        }
    })

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body={"requests": requests}
    ).execute()

    print(f"Tab '{TAB}' rebuilt: {len(rows)} rows.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
