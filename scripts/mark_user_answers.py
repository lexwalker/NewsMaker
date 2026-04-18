"""Mark the 'Учитываем?' column for groups the user has already answered.

Groups 5, 6, 7, 9 already have decisions from the user — record them in the
shared sheet so the manager can skip them and focus on 1, 2, 3, 4, 8, 10, 11.
"""

from __future__ import annotations

import io
import os
import sys
from pathlib import Path

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

# By group header text fragment → (answer, comment)
USER_ANSWERS: dict[str, tuple[str, str]] = {
    "5. Интервью-серии":
        ("Только если с новостью",
         "Чистое интервью — нет. Если в заголовке/теле есть анонс (новая модель, сделка, "
         "планы) — считаем как новость. Требует LLM-проверки тела статьи."),
    "6. Op-ed / мнения / feature":
        ("Нет",
         "Мнения и аналитические колонки не учитываем. Добавлены правила по URL и "
         "заголовку (paradox, на грани, rethink и т.п.)."),
    "7. Пограничные / малоизвестные китайские бренды":
        ("Да",
         "Мониторим — расширен config/brand_domains.yaml: IM, Seres, Denza, Jetta, "
         "Zeekr, Avatr, Voyah, Aito, Maxus, Wuling, Baojun, Rising, Neta, HiPhi, "
         "Xiaomi Auto, Leapmotor, Lynk & Co, Belgee, JAC, GAC, Foton, Soueast, Tank, "
         "UMO, Jeland."),
    "9. Тематическая инфографика (авто + туризм":
        ("Нет",
         "Лайфстайл / авто-туризм не учитываем. Добавлены правила: URL "
         "/infographics/puteshestvuem- и заголовочные слова «путешеств», «поездка из», "
         "«road trip»."),
}


def main() -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    rows = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"'{TAB}'")
        .execute()
        .get("values", [])
    )

    updates = []
    for i, row in enumerate(rows, start=1):
        if not row:
            continue
        cell_a = row[0].strip() if row else ""
        for marker, (answer, comment) in USER_ANSWERS.items():
            if marker in cell_a:
                # The "decision line" is 2 rows down:
                #  group header              ← here
                #  description               ← +1
                #  Учитываем эту категорию? | (empty) | (answer here) | (comment here)  ← +2
                decision_row = i + 2
                updates.append({
                    "range": f"'{TAB}'!C{decision_row}:D{decision_row}",
                    "values": [[answer, comment]],
                })
                print(f"row {decision_row}: {marker} → {answer}")
                break

    if updates:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={
                "valueInputOption": "USER_ENTERED",
                "data": updates,
            },
        ).execute()
    print(f"Wrote {len(updates)} pre-filled decisions.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
