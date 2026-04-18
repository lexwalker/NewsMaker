"""Append Claude's open questions to the 'Уточнение у руководителя' tab.

The manager-facing tab already has 11 groups filled by the pipeline. This
script adds a second section with 6 open questions from the assistant —
topics where the boundary between "news" and "not news" is genuinely
ambiguous and needs a human decision.
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


# Each question: (id, title, context, [(example_label, example_title)], ask_text)
QUESTIONS: list[tuple[str, str, str, list[tuple[str, str]], str]] = [
    (
        "Q1",
        "Экономика — где проходит граница?",
        "Чисто «автомобильная экономика» (автокредиты, лизинг, продажи, пошлины, "
        "утильсбор) — понятно, берём. Но есть случаи, когда экономическая новость "
        "лишь косвенно связана с авто (ставка ЦБ, курс рубля, цены на нефть, общие "
        "макро-показатели). Нужно ли их учитывать, когда статья явно про авторынок?",
        [
            ("1prime «Российский рынок акций сдержанно повышается»", "сейчас — не по теме"),
            ("1prime «ЕС сократил импорт нефти из России в 3 раза»", "сейчас — не по теме"),
            ("autopotoknews «Портфель автокредитов 3 трлн руб»", "сейчас — Новость"),
            ("frankrg «Выдачи кредитов в марте — 925.7 млрд»", "сейчас — не по теме"),
            ("(гипотетика) «Утильсбор повысили с 1 июня»", "должно быть Local specifics"),
        ],
        "Берём ли макро-новости (ставка ЦБ, курс, нефть), когда они привязаны к "
        "авторынку, или только узко-автомобильную экономику?",
    ),
    (
        "Q2",
        "Электромобили и их смежная индустрия",
        "Модели EV, EV-бренды — очевидно берём. Но вокруг EV растут отрасли: "
        "зарядная инфраструктура, производство аккумуляторов, цены на литий/кобальт. "
        "Где граница между «новость авторынка» и «смежная промышленность»?",
        [
            ("«Nissan представил Juke EV»", "ясно — Новость"),
            ("cnevpost «Seres+BMW+Mercedes charging JV»", "зарядная инфра — Новость? Other news?"),
            ("«BASF + Transfar — партнёрство по компонентам»", "сейчас — Новость"),
            ("Benchmark Minerals (battery supply chain)", "сейчас — отклоняем"),
            ("TrendForce «EV Cell Prices 2Q26 forecast»", "сейчас — пограничная"),
        ],
        "Battery supply chain и зарядная инфраструктура — считаем частью авто-новостей "
        "или только если в заголовке прямо упомянут бренд / модель автомобиля?",
    ),
    (
        "Q3",
        "Коммерческий транспорт — что кроме LCV до 3.5 т?",
        "Есть отдельный раздел `LCV news` для LCV ≤ 3.5 тонны. А автобусы, большие "
        "грузовики, электро-траки, спецтехника и сельхозтехника — куда?",
        [
            ("«Record Q1 for zero emission bus demand» (SMMT)", "автобусы — сейчас Новость"),
            ("«Foton to launch in UK» (SMMT) — большой грузовой бренд", "сейчас — Новость"),
            ("«Tesla Semi» (гипотетика)", "LCV или Other news?"),
            ("Строительная и дорожная спецтехника", "в v4 не попалось"),
            ("Трактора / сельхозтехника", "в v4 не попалось"),
        ],
        "Куда относить автобусы и тяжёлые грузовики? Спецтехника и сельхозтехника — "
        "включаем или отсеиваем?",
    ),
    (
        "Q4",
        "Географический фокус — какие рынки в приоритете?",
        "2989 опубликованных новостей имеют country=Russia [7], но 57% помечены "
        "Region=Global. Значит пишете и про мировые рынки. Нужен ранжированный список — "
        "какие рынки приоритетные, какие «на всякий случай», какие вообще не "
        "интересны.",
        [
            ("Россия", "✅ очевидно"),
            ("Казахстан, Узбекистан", "✅ будущие порталы"),
            ("Европа (ACEA, SMMT, EuroNCAP, VDA)", "?"),
            ("Китай (cnevpost, carnewschina)", "?"),
            ("США (motortrend, thedrive, carbuzz)", "?"),
            ("Индия, ЮАР, Ближний Восток, Корея, Япония", "?"),
        ],
        "Какие рынки считаем приоритетными для портала RU? Какие «второстепенными» "
        "(берём, но только крупные события)? Какие пропускаем?",
    ),
    (
        "Q6",
        "Обновления моделей — новость или промо?",
        "Граница между разделами `Confirmed` (подтверждённый факт про модель) и "
        "`Dealer news / Promo` (промо-активности). Что считать «маркетингом», а что "
        "«новостью про модель»?",
        [
            ("«Omoda C5 в прайс-листе появилась 4WD версия»", "Confirmed или Dealer/Promo?"),
            ("«Geely объявил trade-in программу в апреле»", "Dealer/Promo"),
            ("«Цены на Haval снижены на 200 тыс рублей»", "Dealer/Promo? или Confirmed?"),
            ("«Lada Vesta получила новую коробку передач WLY»", "Confirmed"),
            ("«Haval Jolion — специальные условия кредитования»", "Dealer/Promo"),
        ],
        "Граница Confirmed vs Dealer/Promo — это про содержание (модельная новость "
        "vs маркетинг) или про источник (пресс-релиз бренда vs рекламная кампания "
        "дилера)?",
    ),
    (
        "Q7",
        "Регуляторные и правовые новости — раздел?",
        "Новости про законодательство и регуляторные решения. Сейчас всё, что про РФ, "
        "идёт в `Local specifics`. Но если это международная регуляторика (ЕС, США), "
        "куда относить?",
        [
            ("«Правительство РФ продлило льготное автокредитование»", "Local specifics — ок?"),
            ("«Госдума отклонила инициативу выделенных полос»", "Local specifics — ок?"),
            ("«Узбекистан повысил пошлины на подержанные авто»", "Local specifics (UZ)"),
            ("«Европейская директива о выбросах принята»", "Other news? Economics?"),
            ("«Trump вводит тарифы на китайские авто» (гипотетика)", "Other news? Economics?"),
        ],
        "Международная регуляторика (ЕС, США) — Other news или Economics? Вся "
        "РФ-регуляторика точно в Local specifics, или часть могут быть Confirmed?",
    ),
]


def main() -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    sheet_id = next(
        s["properties"]["sheetId"]
        for s in meta["sheets"]
        if s["properties"]["title"] == TAB
    )

    # Find where current content ends
    existing = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"'{TAB}'")
        .execute()
        .get("values", [])
    )
    start_row = len(existing) + 2  # leave one blank row as separator

    # Build rows
    rows: list[list[Any]] = []
    # Section header
    rows.append([])
    rows.append(["═══════════════════════════════════════════"])
    rows.append([
        "ВОПРОСЫ ОТ АССИСТЕНТА ДЛЯ РУКОВОДИТЕЛЯ "
        "(категории, где правило неочевидно — нужен человек)"
    ])
    rows.append(["═══════════════════════════════════════════"])
    rows.append([])

    # Remember row indexes for formatting
    section_header_idx = start_row - 1 + 2  # absolute row of the big title (1-based)

    question_header_rows: list[int] = []
    description_rows: list[int] = []
    ask_rows: list[int] = []

    cur_row_abs = start_row + len(rows) - 1  # track absolute 1-based row

    for qid, title, context, examples, ask_text in QUESTIONS:
        # question title
        rows.append([f"{qid}.  {title}"])
        cur_row_abs += 1
        question_header_rows.append(cur_row_abs)

        # context / description
        rows.append([context])
        cur_row_abs += 1
        description_rows.append(cur_row_abs)

        # examples
        rows.append(["Пример", "Сейчас / как бы выглядело"])
        cur_row_abs += 1
        for label, result in examples:
            rows.append([f"   • {label}", result])
            cur_row_abs += 1

        # decision row
        rows.append([ask_text, "", "← Да / Нет / условие", "← комментарий"])
        cur_row_abs += 1
        ask_rows.append(cur_row_abs)

        # spacer
        rows.append([])
        cur_row_abs += 1

    # Write values
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{TAB}'!A{start_row}",
        valueInputOption="USER_ENTERED",
        body={"values": rows},
    ).execute()

    # Formatting
    requests: list[dict[str, Any]] = []

    def repeat(
        row1: int,
        row2: int,
        col1: int,
        col2: int,
        *,
        bold: bool | None = None,
        italic: bool | None = None,
        bg: dict[str, float] | None = None,
        fg: dict[str, float] | None = None,
        wrap: bool = False,
        font_size: int | None = None,
    ) -> None:
        fmt: dict[str, Any] = {}
        tf: dict[str, Any] = {}
        if bold is not None:
            tf["bold"] = bold
        if italic is not None:
            tf["italic"] = italic
        if fg is not None:
            tf["foregroundColor"] = fg
        if font_size is not None:
            tf["fontSize"] = font_size
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
                    "startRowIndex": row1 - 1,  # 0-based
                    "endRowIndex": row2,
                    "startColumnIndex": col1,
                    "endColumnIndex": col2,
                },
                "cell": {"userEnteredFormat": fmt},
                "fields": "userEnteredFormat(" + ",".join(fields) + ")",
            }
        })

    # Big banner (3 rows of the === banner)
    repeat(
        section_header_idx,
        section_header_idx + 1,
        0, 4,
        bold=True,
        font_size=12,
        bg={"red": 0.25, "green": 0.33, "blue": 0.50},
        fg={"red": 1.0, "green": 1.0, "blue": 1.0},
        wrap=True,
    )
    # Question headers
    for r in question_header_rows:
        repeat(r, r, 0, 4,
               bold=True,
               bg={"red": 0.95, "green": 0.87, "blue": 0.70})
    # Description rows
    for r in description_rows:
        repeat(r, r, 0, 4, italic=True, wrap=True)
    # Ask rows
    for r in ask_rows:
        repeat(r, r, 0, 4,
               bold=True,
               bg={"red": 1.00, "green": 0.97, "blue": 0.85},
               wrap=True)

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID, body={"requests": requests}
    ).execute()

    print(f"Appended {len(QUESTIONS)} questions starting at row {start_row}.")
    print(f"Rows written: {len(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
