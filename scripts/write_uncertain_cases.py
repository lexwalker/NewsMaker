"""Build a digest of borderline categories for the editor-in-chief.

Groups all 'Аналитика' / 'Уточнить' / 'Новость (погран.)' rows from
ТЕСТ статьи v4 into 11 editorial types, each with 2-3 real examples,
and leaves a single decision column («Учитываем / Не учитываем») plus
a comment column for the manager to fill in.

Output tab: 'Уточнение у руководителя'
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
load_dotenv(ROOT / ".env")

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

TAB = "Уточнение у руководителя"


# Each group: (name, description, [(example_url, example_title), …])
GROUPS: list[tuple[str, str, list[tuple[str, str]]]] = [
    (
        "1. Аналитика рынка / ежемесячные отчёты",
        "Регулярные обзоры состояния авторынка: доля брендов, ежемесячные / квартальные "
        "отчёты о продажах, дайджесты дилерских ассоциаций. Есть конкретная дата публикации, "
        "но по стилю это не отдельная новость, а периодическая аналитика.",
        [
            (
                "https://www.coxautoinc.com/insights/ev-market-monitor-march-2026/",
                "EV Market Monitor – March 2026 (Cox Automotive)",
            ),
            (
                "https://www.coxautoinc.com/insights/auto-market-weekly-summary-04-13-26/",
                "Auto Market Weekly Summary — 04-13-26 (Cox Automotive)",
            ),
            (
                "https://www.acea.auto/publication/economic-and-market-report-global-and-eu-auto-industry-full-year-2025/",
                "Economic and Market Report — Full year 2025 (ACEA)",
            ),
            (
                "https://napinfo.ru/press-releases/kakova-na-samom-dele-dolya-kitajskih-avtomobilej-na-rossijskom-rynke/",
                "Какова доля китайских автомобилей на российском рынке (НАП)",
            ),
            (
                "https://napinfo.ru/infographics/chto-budet-vliyat-na-prodazhi-gruzovyh-avtomobilej-v-blizhajshie-gody/",
                "Что будет влиять на продажи грузовых автомобилей в ближайшие годы (НАП, инфографика)",
            ),
        ],
    ),
    (
        "2. Прогнозы и forecast-материалы",
        "Статьи с заголовком типа «ожидается рост на …», «до конца года цены вырастут», "
        "прогнозы агентств. Формально новость — есть конкретная цифра и дата, но это "
        "не состоявшееся событие, а предсказание.",
        [
            (
                "https://www.trendforce.com/presscenter/news/20260416-13015.html",
                "Rising Costs to Drive Further Increases in EV Cell Prices in 2Q26",
            ),
            (
                "https://napinfo.ru/press-releases/kakie-avtomobili-v-marte-podorozhali-do-6-kakie-podesheveli-do-11/",
                "Какие автомобили в марте подорожали до 6%, какие подешевели до 11% (НАП)",
            ),
        ],
    ),
    (
        "3. Whitepapers / длинные аналитические блог-посты",
        "Исследования от корпоративных блогов, длинные посты в стиле «уроки из проекта», "
        "«как устроено …». Часто полезная информация, но это не newsworthy-событие.",
        [
            (
                "https://www.geotab.com/blog/run-on-less-messy-middle/",
                "Run on Less — Messy Middle: Battery Electric Truck Lessons (Geotab)",
            ),
            (
                "https://www.geotab.com/blog/reduce-cost-barriers-fleet-electrification/",
                "Breaking down EV fleet cost barriers (Geotab)",
            ),
            (
                "https://www.acea.auto/news/time-to-rethink-modal-shift-zero-emission-trucks-change-the-game/",
                "Time to rethink modal shift: Zero-emission trucks (ACEA, op-ed)",
            ),
        ],
    ),
    (
        "4. Test-drive / longform обзоры моделей",
        "Подробные тест-драйвы, yearlong reviews, сравнительные тесты. Относятся к разделу "
        "Test-drive, который ты отметил как «оставить, но пометить для ручной проверки».",
        [
            (
                "https://www.motortrend.com/reviews/yearlong-review-arrival-2026-honda-passport-trailsport-elite",
                "Yearlong Review: 2026 Honda Passport Trailsport Elite",
            ),
            (
                "https://www.motortrend.com/reviews/first-test-2025-ford-mustang-rtr-spec-3",
                "Tested: The 2025 Ford Mustang RTR Spec 3",
            ),
        ],
    ),
    (
        "5. Интервью-серии с представителями отрасли",
        "Регулярные серии коротких интервью с менеджерами компаний-партнёров. Новость есть "
        "(конкретный человек, конкретная тема), но жанр — интервью, а не новостная заметка.",
        [
            (
                "https://www.smmt.co.uk/five-minutes-with-jonathan-ramsey-senior-solutions-marketing-manager-samsara/",
                "Five minutes with… Jonathan Ramsey, Samsara (SMMT)",
            ),
        ],
    ),
    (
        "6. Op-ed / мнения / feature-материалы",
        "Авторские колонки ассоциаций и отраслевых изданий. Типичные заголовки: «Paradox of …», "
        "«Time to rethink …». Содержат тезисы автора, а не новостной повод.",
        [
            (
                "https://naamsa.net/where-people-defy-the-numbers-the-paradox-of-sas-automotive-industry/",
                "Paradox of SA's automotive industry (naamsa)",
            ),
            (
                "https://www.smmt.co.uk/investing-to-stay-ahead-van-dealerships-in-the-uk/",
                "Investing to stay ahead: Van dealerships in the UK (SMMT)",
            ),
            (
                "http://www.motorpage.ru/infocenter/autoarticles/na_grani_istoricheskogo_minimuma.html",
                "Обзор рынка: На грани исторического минимума (motorpage)",
            ),
        ],
    ),
    (
        "7. Пограничные / малоизвестные китайские бренды",
        "Новости про бренды, которых нет на рынках РФ/СНГ — IM Motors (Alibaba+SAIC), "
        "Jetta (бюджетный суббренд VW в Китае), Seres, Denza и т.п. Формально — настоящая "
        "новость, но читателю в РФ может быть малоинтересно.",
        [
            (
                "https://t.me/chinamashina_news/13005",
                "Кроссовер IM LS8 от Alibaba и SAIC получил более 8000 заказов",
            ),
            (
                "https://cnevpost.com/2026/04/17/seres-joins-bmw-mercedes-china-charging-jv/",
                "Seres joins BMW, Mercedes in China premium charging JV",
            ),
            (
                "https://t.me/chinamashina_news/13004",
                "Совместное предприятие FAW-Volkswagen — премьера концепт-кара Jetta X",
            ),
        ],
    ),
    (
        "8. Иностранные отзывы авто (короткий формат)",
        "База отзывных кампаний Канады / Австралии. Заголовки типа «Fuel pump could fail» — "
        "тело очень короткое, модели не всегда продаются на нашем рынке. Нужно решить — "
        "берём вообще зарубежные отзывы?",
        [
            (
                "https://wwwapps.tc.gc.ca/Saf-Sec-Sur/7/VRDB-BDRV/search-recherche/detail.aspx?lang=eng&rn=2026175",
                "Fuel pump could fail (Transport Canada)",
            ),
            (
                "https://wwwapps.tc.gc.ca/Saf-Sec-Sur/7/VRDB-BDRV/search-recherche/detail.aspx?lang=eng&rn=2026177",
                "Antilock braking system may not work (Transport Canada)",
            ),
        ],
    ),
    (
        "9. Тематическая инфографика (авто + туризм / авто-лайфстайл)",
        "Материалы «куда поехать на Haval», «цены поездки через регионы», стоимость владения. "
        "Про авто, но формат — лайфстайл, не новость.",
        [
            (
                "https://napinfo.ru/infographics/puteshestvuem-po-rossii-vo-skolko-obojdetsya-poezdka-iz-moskvy-v-pereslavl-zalesskij-cherez-nizhnij-novgorod-na-haval-jolion/",
                "Путешествие из Москвы в Переславль-Залесский на Haval Jolion (НАП)",
            ),
        ],
    ),
    (
        "10. Пресс-релизы дилерских / лизинговых компаний",
        "Новости финансовых партнёров авторынка — Газпромбанк Автолизинг, Ингосстрах. "
        "Связаны с рынком, но часто это корпоративные новости лизинговой / страховой компании, "
        "не события на автомобильном рынке.",
        [
            (
                "https://autogpbl.ru/press-center/novosti-kompanii/gazprombank-avtolizing-rossiyskaya-dilerskaya-set-uvelichilas-do-4-2-tys-shourumov/",
                "Газпромбанк Автолизинг: дилерская сеть увеличилась до 4,2 тыс. шоурумов",
            ),
            (
                "https://autogpbl.ru/press-center/novosti-kompanii/vstuplenie-v-silu-s-1-maya-2026-novykh-redaktsiy-dokumentov-obshchie-usloviya-lizinga-i-obshchie-usl/",
                "Новые редакции документов «Общие условия лизинга» с 1 мая 2026",
            ),
            (
                "https://www.ingos.ru/company/news/2026/adb9407a-08e3-4adf-925b-5c1d1ba9af51",
                "Ингосстрах выплатил 3,8 млрд в сегменте автострахования за 2 недели",
            ),
        ],
    ),
    (
        "11. Ежегодные премии / NCAP winners / музейные события",
        "Ежегодные подборки «лучший авто года», списки победителей NCAP, музейные открытия. "
        "Про авто, регулярно обновляются, но это не отдельная новостная заметка.",
        [
            (
                "https://www.greenncap.com/2025-category-winners/",
                "Green NCAP — Winners in the Automobiles category in 2025",
            ),
            (
                "https://xn--80aal0a.xn--80asehdb/auto-news/autovaz/47220-muzej-avtovaza-voshel-v-top-rossijskih-obektov-industrialnogo-turizma/",
                "Музей АВТОВАЗа вошёл в ТОП объектов индустриального туризма",
            ),
        ],
    ),
]


INSTRUCTIONS = (
    "Мы строим автоматический сборщик новостей. Часть материалов бот находит, но "
    "не уверены, нужны ли они в финальной ленте портала. Прошу напротив каждой группы "
    "в колонке «Учитываем?» написать «Да» или «Нет»; в колонке «Комментарий» можно "
    "уточнить условие (например: «только если упоминается бренд из нашего списка»)."
)


def main() -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # Ensure tab exists
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    tab_titles = [s["properties"]["title"] for s in meta["sheets"]]
    sheet_id: int
    if TAB not in tab_titles:
        resp = svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TAB}}}]},
        ).execute()
        sheet_id = resp["replies"][0]["addSheet"]["properties"]["sheetId"]
    else:
        sheet_id = next(
            s["properties"]["sheetId"] for s in meta["sheets"]
            if s["properties"]["title"] == TAB
        )

    # Clear previous content
    svc.spreadsheets().values().clear(
        spreadsheetId=SHEET_ID, range=f"'{TAB}'"
    ).execute()

    # Build rows
    rows: list[list[Any]] = []
    header_row_indexes: list[int] = []   # track which rows are group headers
    instructions_row_indexes: list[int] = []

    rows.append(["Учёт пограничных категорий — решение руководителя"])
    rows.append([INSTRUCTIONS])
    rows.append([])
    rows.append(["Группа / пример", "Заголовок", "Учитываем?", "Комментарий"])
    table_header_row = len(rows) - 1  # 0-based

    for group_name, description, examples in GROUPS:
        rows.append([])
        rows.append([group_name])
        header_row_indexes.append(len(rows) - 1)
        rows.append([description])
        instructions_row_indexes.append(len(rows) - 1)
        rows.append(["Учитываем эту категорию?", "", "← сюда Да / Нет", "← условия"])
        for url, title in examples:
            rows.append([f"   {url}", title[:200], "", ""])

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{TAB}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": rows},
    ).execute()

    # Formatting via batchUpdate
    requests: list[dict[str, Any]] = []

    def style_range(
        start_row: int,
        end_row: int,
        start_col: int,
        end_col: int,
        *,
        bold: bool | None = None,
        bg: dict[str, float] | None = None,
        wrap: bool = False,
        italic: bool | None = None,
    ) -> None:
        fmt: dict[str, Any] = {}
        tf: dict[str, Any] = {}
        if bold is not None:
            tf["bold"] = bold
        if italic is not None:
            tf["italic"] = italic
        if tf:
            fmt["textFormat"] = tf
        if bg is not None:
            fmt["backgroundColor"] = bg
        if wrap:
            fmt["wrapStrategy"] = "WRAP"
        if not fmt:
            return
        fields = []
        if "textFormat" in fmt:
            fields.append("textFormat")
        if "backgroundColor" in fmt:
            fields.append("backgroundColor")
        if "wrapStrategy" in fmt:
            fields.append("wrapStrategy")
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row,
                    "endRowIndex": end_row,
                    "startColumnIndex": start_col,
                    "endColumnIndex": end_col,
                },
                "cell": {"userEnteredFormat": fmt},
                "fields": "userEnteredFormat(" + ",".join(fields) + ")",
            }
        })

    # Column widths
    col_widths = {0: 460, 1: 500, 2: 140, 3: 320}
    for col, px in col_widths.items():
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": col,
                    "endIndex": col + 1,
                },
                "properties": {"pixelSize": px},
                "fields": "pixelSize",
            }
        })

    # Main title row 1
    style_range(0, 1, 0, 4, bold=True, bg={"red": 0.20, "green": 0.28, "blue": 0.45})
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": 0,
                "endColumnIndex": 4,
            },
            "cell": {
                "userEnteredFormat": {
                    "textFormat": {
                        "bold": True,
                        "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                        "fontSize": 13,
                    }
                }
            },
            "fields": "userEnteredFormat.textFormat",
        }
    })
    # Intro row
    style_range(1, 2, 0, 4, italic=True, wrap=True, bg={"red": 0.95, "green": 0.95, "blue": 0.98})
    # Table header
    style_range(
        table_header_row,
        table_header_row + 1,
        0,
        4,
        bold=True,
        bg={"red": 0.85, "green": 0.85, "blue": 0.85},
    )
    # Group headers
    for r in header_row_indexes:
        style_range(r, r + 1, 0, 4, bold=True, bg={"red": 0.87, "green": 0.93, "blue": 1.00})
    # Description rows
    for r in instructions_row_indexes:
        style_range(r, r + 1, 0, 4, italic=True, wrap=True)

    # Freeze first 4 rows
    requests.append({
        "updateSheetProperties": {
            "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 4}},
            "fields": "gridProperties.frozenRowCount",
        }
    })

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body={"requests": requests}
    ).execute()

    total_examples = sum(len(examples) for _, _, examples in GROUPS)
    print(f"Tab '{TAB}' written.")
    print(f"  groups:    {len(GROUPS)}")
    print(f"  examples:  {total_examples}")
    print(f"  total rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
