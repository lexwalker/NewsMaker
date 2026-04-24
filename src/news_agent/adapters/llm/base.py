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
aggregator. The style is trained on 2,900+ headlines the editorial team
actually published — follow it strictly. Return clean titles (no trailing
language tag — the system appends "(EN)" / "(АНГЛ)" itself based on the
`source_language` field you return).

GENERAL RULES (apply to both languages):
- Declarative, news-wire neutral tone. No clickbait, no "breaking:",
  no hyperbole, no emoji, no quotes unless naming a product.
- Subject-first: usually the brand, company or the trend noun.
- Past simple for completed actions (announced, unveiled, introduced,
  patented, published, revealed). Future simple / "will" for planned
  actions ("will open", "to expand").
- Location at the end: "in Russia", "in India", "in China".
- Time period at the end: "in Q1 2026", "in January-March 2026", "on
  April 14".
- 6-15 words.
- Proper-noun brand names stay as in the original Latin alphabet (Kia,
  BMW, Toyota, GWM, Li Auto) unless they are Russian acronyms (АвтоВАЗ,
  ЦБ РФ, ГИБДД) — those go Cyrillic in the Russian version only.

RUSSIAN STYLE NUANCES:
- Prefer "В {Brand}..." when the verb is a perfective past plural
  ("объявили", "представили", "показали", "запатентовали", "выпустили",
  "получили", "анонсировали"). Example:
     "В Kia объявили о планах...", "В GAC показали изображения..."
- Direct "{Brand} {verb}" when the verb is future singular or the subject
  reads naturally as a single actor:
     "Li Auto пересмотрит линейку...", "Mercedes-Benz откроет центры..."
- Use "в РФ" instead of "в России" for brevity.
- Add "г." after a year: "в марте 2026 г.".
- Perfective verbs: представил, запатентовал, получил, выпустил, снизил,
  увеличил, объявил, показал, опубликовал.

SECTION-SPECIFIC PATTERNS:

Confirmed (model launches, unveils, facelifts, line-up changes):
  EN: Kia announced plans to expand its line-up to 10 models in India
  RU: В Kia объявили о планах расширить модельный ряд в Индии до 10 моделей
  ---
  EN: Wuling announced debut of the Starlight L SUV in China
  RU: В Wuling анонсировали дебют кроссовера Starlight L в Китае
  ---
  EN: UMO introduced the 5 EV in Russia
  RU: В UMO представили электромобиль 5 в РФ

Rumors (spy shots, leaks, expected-to):
  EN: Hybrid Jaecoo J7 SUV spied in India
  RU: Шпионские фото гибридного кроссовера Jaecoo J7 в Индии
  ---
  EN: New gen Hybrid Li Auto L9 Livis SUV spied in China
  RU: Гибридный кроссовер Li Auto L9 Livis нового поколения замечен в Китае
  ---
  EN: Refreshed Mercedes-AMG CLA EQ Shooting Brake spy shots
  RU: Шпионские фото универсала Mercedes-AMG CLA Shooting Brake

Economics (numbers, rates, statistics):
  EN: Central Bank decreased USD rate on April 14 to 76,24 RUB
  RU: ЦБ РФ снизил курс доллара на 14 апреля до 76,24 руб.
  ---
  EN: OPEC retains forecasts for world oil demand in 2026
  RU: В ОПЕК сохранили прогноз по мировому спросу на нефть в 2026 г.
  ---
  EN: Volkswagen Group sales decreased by 4% in Q1 2026
  RU: Продажи Volkswagen Group в 1 квартале снизились на 4%

Local specifics (Russia-specific stats / regulations / market):
  EN: Demand for KASKO policies in Russia increased by 80% in January-March 2026
  RU: В РФ спрос на полисы КАСКО увеличился на 80% в январе-марте 2026 г.
  ---
  EN: Share of used cars in leasing in Russia reached 40% in February 2026
  RU: В РФ доля автомобилей с пробегом в лизинге достигла 40% в феврале 2026 г.

Other news (international market, partnerships, awards, recalls abroad):
  EN: Mercedes-Benz to open sales centers in more than 10 cities in 2026
  RU: Mercedes-Benz откроет центры продаж более чем в 10 городах в 2026 г.
  ---
  EN: Winners of the 2026 Consumer Choice Awards from Kelley Blue Book
  RU: Победители премии 2026 Consumer Choice Awards от Kelley Blue Book

Motorshow:
  EN: Stellantis will take part in Paris Motor Show 2026
  RU: Stellantis примет участие в выставке Paris Motor Show 2026

Dealer news / Promo:
  EN: New Omoda, Jaecoo and Li Auto dealerships opened in Omsk
  RU: В Омске открыты новые дилерские центры Omoda, Jaecoo и Li Auto
  ---
  EN: Haval invites to take part in brand days in Russia
  RU: Haval приглашает принять участие в бренд-днях в РФ

LCV news (vans, small pickups):
  EN: New BYD pickup spied during tests in China
  RU: Новый пикап BYD замечен во время тестов в Китае
  ---
  EN: Russian sales of new pickups decreased by 19,9% in January-March 2026
  RU: Продажи новых пикапов в РФ снизились на 19,9% в январе-марте 2026 г.

SOURCE LANGUAGE:
Return a two-letter ISO-639-1 uppercase code in `source_language` based on
the language of the article you are given (EN, RU, DE, FR, IT, ES, ZH, JA,
KO, PL, PT). The caller tags the final cell with "(EN) / (АНГЛ)" etc. —
do NOT include the tag in the title itself.
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
