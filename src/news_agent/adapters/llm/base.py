"""LLMClient protocol and shared prompt-building helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Protocol

from news_agent.core.models import (
    Classification,
    FewShotExample,
    LLMUsage,
    RelevanceCheck,
    SectionDefinition,
    TitlePair,
)


class LLMCallResult(Protocol):
    """Bundle returned by every LLM call for bookkeeping."""

    usage: LLMUsage


class LLMClient(Protocol):
    """Provider-agnostic LLM facade."""

    provider_name: str
    model: str

    def is_automotive(self, title: str, body_excerpt: str) -> tuple[RelevanceCheck, LLMUsage]:
        ...

    def classify_section(
        self,
        *,
        title: str,
        body: str,
        sections: list[SectionDefinition],
        few_shots: list[FewShotExample],
        portal_country: str,
    ) -> tuple[Classification, LLMUsage]:
        ...

    def translate_title(
        self, *, title: str, source_language_hint: str | None
    ) -> tuple[TitlePair, LLMUsage]:
        ...


# -------------------------------------------------- shared prompt components
RELEVANCE_SYSTEM = (
    "You are a strict binary filter for an automotive news aggregator. "
    "Return true only if the article is about the automotive industry, "
    "automotive market economy, car brands, dealer networks, auto regulation, "
    "or closely auto-adjacent macroeconomics that directly affects car sales or prices. "
    "Return false for general politics, sports, non-auto tech, entertainment, etc."
)

CLASSIFY_SYSTEM = (
    "You classify automotive / economy news into one of a fixed list of sections. "
    "You also decide whether the news is specifically about the portal's country (Local) "
    "or not (Global). You return structured JSON only. "
    "If the news is NOT automotive or auto-economy at all, still pick the closest section "
    "but set confidence ≤ 0.2."
)

TRANSLATE_SYSTEM = """\
You produce a headline pair (English + Russian) for an automotive news
aggregator. The style is trained on 2,817 headlines the editorial team
actually published — follow it strictly. Return CLEAN titles (no trailing
language tag — the system appends "(EN)" / "(АНГЛ)" / "(НЕМ)" / "(ИТАЛ)"
/ "(КИТ)" itself based on the `source_language` ISO code you return).

============== HARD CONSTRAINTS (do not violate) ==============

1) **The English and the Russian headline MUST mean exactly the same
   thing.** Same subject, same verb tense, same time period, same numbers,
   same place. Don't paraphrase one into a different statement. If the EN
   says "X will arrive in Russia in 2027", the RU MUST say "X появится в
   РФ в 2027 г." — NOT "В X поделились характеристиками для РФ".

2) **Don't fabricate dates, years, prices or numbers.** Take them only
   from the source title or body. If the source body says nothing about
   the year, OMIT the year — don't guess "2024" or "2027". When in
   doubt, drop the time qualifier.

3) **Today's reality.** The current year of operation is 2026 — never
   produce a headline that refers to a past year (2023, 2024) as if it
   were a future event. If a year is in the past relative to 2026 and
   the verb is "will", that is a contradiction — drop the year or fix
   the tense.

4) **Translate, don't editorialise.** No clickbait, no opinions, no
   adjectives that aren't in the source ("groundbreaking", "stunning",
   "amazing").

5) **No invented brands or models.** If the body doesn't name a model,
   don't invent one. Use the brand alone.

============== STYLE GUIDE ==============

=========== GENERAL RULES (apply to both languages) ===========
- Declarative, news-wire neutral tone. No clickbait, no "breaking:",
  no hyperbole, no emoji, no quotes unless naming a product.
- Median length is 10 words in EN and 9 in RU; keep under 15.
- Subject-first: brand / company / trend noun / geographic marker.
- Past simple for completed actions: introduced (174 uses), announced
  (116), published (96), started (60), revealed (40), launched (38),
  certified (46), recalls (57), got, refreshed (64 uses as adjective).
- Future: "will {verb}" or "{brand} to {verb}": will open, to partner,
  to launch, to build, to expand.
- Location at the END: "in Russia", "in China", "in India", "in the U.S.",
  "in Europe", "in UAE", "in Germany".
- Time period at the END: "in Q1 2026", "in January-March 2026", "in 2025",
  "on April 14", "by 2028".
- Proper-noun brand names stay in original Latin (Kia, BMW, Toyota, GWM,
  Li Auto) EXCEPT Russian corporate acronyms which go Cyrillic only in
  the Russian version: АвтоВАЗ, ЦБ РФ, ЕЦБ, Эксперт РА, Соллерс, Автотор,
  Мотор-Плейс, ДАВ-Авто, Мэйджор.

=========== DECIMAL SEPARATOR — CRITICAL ===========
The editorial style uses COMMA as the decimal separator in BOTH languages
(this is unusual for English — preserve it):
  "82,13 RUB" / "82,13 руб."
  "4,6%" / "4,6%"
  "76,24" — not "76.24"

=========== RUSSIAN STYLE NUANCES ===========
- Prefer "В {Brand} {verb past perfective plural}":
    "В Kia объявили...", "В GAC показали...", "В Hongqi разработали...",
    "В BMW выпустили прототипы...", "В Roewe раскрыли интерьер..."
  Verbs triggering this: объявили, представили, показали, запатентовали,
  выпустили, получили, анонсировали, опубликовали, рассказали, разработали,
  раскрыли, сохранили, ввели.
- Direct "{Brand} {verb}" for future-singular or self-actor subject:
    "Li Auto пересмотрит...", "Mercedes-Benz откроет центры...",
    "АвтоВАЗ выделит...", "Dongfeng ищет дилеров...",
    "Mahindra построит новый завод...", "Volvo завершит продажи...",
    "Ford has introduced fee..." → "В Ford ввели плату..."
- Use "в РФ" (NOT "в России"). "г." after a year: "в марте 2026 г.".
- "Во Владимире / В Москве / В Казани / В Санкт-Петербурге / В Люберцах /
  В Курской обл. / В Ингушетии" — city / region openers for local news.
- Geographic openers common too: "В РФ" (169), "В Китае" (147), "В США"
  (57), "В Москве" (28), "В Индии" (16), "В Беларуси" (12), "В Европе",
  "В ОАЭ", "В Германии", "В Японии".

=========== PRESERVE THESE ABBREVIATIONS (both languages) ===========
Russian originals that stay as-is:
  ТС (транспортное средство)  — in RU keep ТС; in EN use "car" / "vehicle"
  РФ / Russia                 — EN: "Russia", RU: "РФ"
  ДТП                         — EN: "accident" / "road accident", RU: "ДТП"
  СИМ                         — EN: "PMD" (personal mobility device), RU: "СИМ"
  ОСАГО                       — EN: "CTP" (compulsory third-party), RU: "ОСАГО"
  КАСКО                       — EN: "KASKO", RU: "КАСКО"
  ЦБ РФ                       — EN: "Central Bank", RU: "ЦБ РФ"
  ФНБ                         — EN: "NWF" (national wealth fund), RU: "ФНБ"
  ВВП                         — EN: "GDP", RU: "ВВП"
  ОТТС                        — EN: "Vehicle Type Approval", RU: "ОТТС"
  МСД                         — EN: "MHSD", RU: "МСД"
  ЕЦБ                         — EN: "ECB", RU: "ЕЦБ"
  ФИПС РФ                     — EN: "FIPS database", RU: "ФИПС РФ"
  ГИБДД / МВД                 — keep acronyms literal
Acronyms that stay universal: LCV, SUV, EV, PHEV, MPV, BEV, NEV.

=========== SECTION-SPECIFIC PATTERNS ===========

CONFIRMED (product launches, intros, unveils):
  EN: Kia announced plans to expand its line-up to 10 models in India
  RU: В Kia объявили о планах расширить модельный ряд в Индии до 10 моделей
  ---
  EN: UMO introduced the 5 EV in Russia
  RU: В UMO представили электромобиль 5 в РФ
  ---
  EN: Sales of the new GAC Aion V SUV started in Russia
  RU: В РФ стартовали продажи нового кроссовера GAC Aion V
  ---
  EN: Mercedes-Maybach published teaser of the VLS MPV
  RU: В Mercedes-Maybach опубликовали тизер минивэна VLS
  ---
  EN: Roewe revealed interior of the new gen i6 sedan
  RU: В Roewe раскрыли интерьер седана i6 нового поколения
  ---
  EN: New version of the Geely EX5 EM-i SUV was certified in Russia
  RU: В РФ сертифицирована новая версия кроссовера Geely EX5 EM-i

RUMORS (spy shots, leaks, spy photos, next-gen previews):
  Common title patterns: "X spied during tests in Y", "X spy shots",
  "New gen X SUV prototype spied...", "Refreshed X prototype spy shots",
  "Hybrid X spied in Y", "X spied during Winter tests".
  EN: New gen Honda HR-V SUV spied in Japan
  RU: Шпионское фото кроссовера Honda HR-V нового поколения в Японии
  ---
  EN: New gen BMW X7 SUV prototype spy shots
  RU: Шпионские фото прототипа кроссовера BMW X7 нового поколения
  ---
  EN: Volkswagen T-Roc R SUV spied during Winter tests
  RU: Кроссовер Volkswagen T-Roc R замечен на зимних тестах
  ---
  EN: Genesis GV60 Magma SUV prototype spied during tests in the U.S. (Video)
  RU: Кроссовер Genesis GV60 Magma замечен на тестах в США (Видео)

ECONOMICS (rates, oil, forecasts, financial reports):
Oil prices have a CANONICAL format — never rephrase:
  EN: Oil prices (USD): Brent 109,3/ WTI 108,56
  RU: Цены на нефть (долл.): Brent 109,3/ WTI 108,56
Central Bank USD / EUR rates:
  EN: Central Bank decreased USD rate on April 14 to 76,24 RUB
  RU: ЦБ РФ снизил курс доллара на 14 апреля до 76,24 руб.
Other:
  EN: OPEC retains forecasts for world oil demand in 2026
  RU: В ОПЕК сохранили прогноз по мировому спросу на нефть в 2026 г.
  ---
  EN: Size of Russian NWF decreased by 0,9% in March 2026
  RU: Объем ФНБ РФ в марте 2026 г. снизился на 0,9%
  ---
  EN: Volkswagen Group sales decreased by 4% in Q1 2026
  RU: Продажи Volkswagen Group в 1 квартале снизились на 4%

LOCAL SPECIFICS (RU-only regulations, stats, market):
  EN: Demand for KASKO policies in Russia increased by 80% in January-March 2026
  RU: В РФ спрос на полисы КАСКО увеличился на 80% в январе-марте 2026 г.
  ---
  EN: Used car imports from Japan to Russia increased by 32% in January-March 2026
  RU: Импорт ТС из Японии в РФ увеличился на 32% в январе-марте 2026 г.
  ---
  EN: Taxi drivers and motorcyclists will be able to apply for CTP via Gosuslugi in Russia
  RU: Таксисты и мотоциклисты смогут оформить ОСАГО через Госуслуги в РФ
  ---
  EN: MHSD travel rules will change effective April 10, 2026
  RU: Правила проезда по МСД изменятся с 10 апреля 2026 г.
  ---
  EN: New passenger car imports in Russia decreased by 57% in 2025
  RU: Импорт новых легковых автомобилей в РФ снизился на 57% в 2025 г.

OTHER NEWS (global market, partnerships, awards, recalls abroad):
  EN: Hyundai recalls 27 units of Ioniq 5 and Ioniq 9 EVs in the U.S.
  RU: В США отзываются 27 электромобилей Hyundai Ioniq 5 и Ioniq 9
  ---
  EN: BMW produced prototypes of the electric i3 sedan at its plant in Germany
  RU: В BMW выпустили прототипы электроседана i3 на заводе в Германии
  ---
  EN: Volvo will end sales of electric EX30 SUV in the U.S. in Summer 2026
  RU: Volvo завершит продажи электрокроссовера EX30 в США летом 2026 г.
  ---
  EN: China may ban the transfer of vehicle controls
  RU: В Китае могут запретить перенос элементов управления ТС
  ---
  EN: BYD production and sales in February 2026
  RU: Производство и продажи BYD в феврале 2026 г.

LCV NEWS (vans, small pickups, up to 3.5 t):
  EN: Changan Hunter Plus pickup got Vehicle Type Approval in Russia
  RU: Пикап Changan Hunter Plus получил ОТТС в РФ
  ---
  EN: Avior introduced the V90 Business in Russia
  RU: В Avior представили фургон V90 Business в РФ
  ---
  EN: Russian LCV production in January 2026
  RU: Производство LCV в РФ в январе 2026 г.
  ---
  EN: AvtoVAZ to spin off commercial vehicles into a separate business
  RU: АвтоВАЗ выделит коммерческие ТС в отдельный бизнес

DEALER NEWS / PROMO (dealership openings, ratings, interviews, promos):
  EN: New Voyah dealership opened in Lyubertsy
  RU: В Люберцах открыт новый дилерский центр Voyah
  ---
  EN: Major got Deepal dealership
  RU: В Мэйджор получили дилерство Deepal
  ---
  EN: TOP-10 Russian dealers in sales of used cars in January-December 2025
  RU: ТОП-10 дилеров РФ по продажам ТС с пробегом в январе-декабре 2025 г.
  ---
  EN: AvtoVAZ launches Hot Days for LADA promotion in Russia
  RU: АвтоВАЗ запускает акцию Жаркие дни LADA в РФ

MOTORSHOW:
  EN: Stellantis will take part in Paris Motor Show 2026
  RU: Stellantis примет участие в выставке Paris Motor Show 2026

=========== VIDEO MARKER ===========
If the article contains a video (common for Rumors / tests), suffix the
TITLE content before the tag with " (Video)" in EN and " (Видео)" in RU:
  EN: Genesis GV60 Magma SUV prototype spied during tests in the U.S. (Video)
  RU: Кроссовер Genesis GV60 Magma замечен на тестах в США (Видео)
Do NOT add Video marker speculatively; only if the source article clearly
is video-based (video platform, "смотрите видео", embedded player).

=========== SOURCE LANGUAGE ===========
Return a two-letter ISO-639-1 uppercase code in `source_language` based
on the language of the article you are given (EN, RU, DE, FR, IT, ES,
ZH, JA, KO, PL, PT, NL, CS, TR, UK). The caller appends the tag — e.g.
DE → "(DE)" on EN line, "(НЕМ)" on RU line — do NOT include the tag in
the title itself.
"""


def build_classify_user_prompt(
    *,
    title: str,
    body: str,
    sections: list[SectionDefinition],
    few_shots: list[FewShotExample],
    portal_country: str,
) -> str:
    """Legacy one-shot prompt (kept for OpenAI path which has no explicit cache)."""
    sections_block = "\n".join(
        f"- {s.name}: {s.description.strip()}" for s in sections
    )
    few_shot_block = ""
    if few_shots:
        lines = [f"  • [{fs.section}] {fs.title}" for fs in few_shots[:20]]
        few_shot_block = "Few-shot examples from curated news:\n" + "\n".join(lines) + "\n\n"
    body_trunc = body[:4000]
    valid = ", ".join(s.name for s in sections)
    return (
        f"{few_shot_block}"
        f"Portal country: {portal_country}.\n"
        f"Task: classify the news below into exactly one of: {valid}.\n"
        f"Also set region='Local' iff the news is specifically about {portal_country}.\n\n"
        f"Sections:\n{sections_block}\n\n"
        f"Title: {title}\n\nBody:\n{body_trunc}"
    )


# ---------- cache-friendly split: static system + dynamic user ----------
# Static part is identical across all 89 articles in a single batch run.
# Anthropic prompt caching gives it 90% discount on reads.

def build_classify_system(
    sections: list[SectionDefinition],
    few_shots: list[FewShotExample],
    portal_country: str,
) -> str:
    sections_block = "\n".join(
        f"- {s.name}: {s.description.strip()}" for s in sections
    )
    few_shot_block = ""
    if few_shots:
        lines = [f"  • [{fs.section}] {fs.title}" for fs in few_shots[:20]]
        few_shot_block = "\nFew-shot examples from curated news:\n" + "\n".join(lines)
    valid = ", ".join(s.name for s in sections)
    return (
        f"{CLASSIFY_SYSTEM}\n\n"
        f"Portal country: {portal_country}.\n"
        f"Task: classify every news item that follows into exactly one of: {valid}.\n"
        f"Also set region='Local' iff the news is specifically about {portal_country}.\n\n"
        f"Sections:\n{sections_block}"
        f"{few_shot_block}"
    )


def build_classify_user(title: str, body: str) -> str:
    return f"Title: {title}\n\nBody:\n{body[:4000]}"


def prompt_hash(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"\n---\n")
    return h.hexdigest()[:16]


# JSON Schema reused by both providers
CLASSIFY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["section", "region", "confidence", "reasoning"],
    "properties": {
        "section": {"type": "string"},
        "region": {"type": "string", "enum": ["Local", "Global"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "reasoning": {"type": "string"},
    },
}

RELEVANCE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["is_automotive_or_economy", "reason"],
    "properties": {
        "is_automotive_or_economy": {"type": "boolean"},
        "reason": {"type": "string"},
    },
}

TRANSLATE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["english", "russian", "source_language"],
    "properties": {
        "english": {"type": "string"},
        "russian": {"type": "string"},
        "source_language": {
            "type": "string",
            "pattern": "^[A-Z]{2}$",
        },
    },
}


def dumps(obj: object) -> str:
    return json.dumps(obj, ensure_ascii=False)
