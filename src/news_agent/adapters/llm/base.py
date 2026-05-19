"""LLMClient protocol and shared prompt-building helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Protocol

from news_agent.core.models import (
    Classification,
    EditorialReview,
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

    def editorial_review(
        self,
        *,
        title: str,
        body: str,
        sections: list[SectionDefinition],
        portal_country: str,
    ) -> tuple[EditorialReview, LLMUsage]:
        """Consolidated editorial decision: should we publish? which section?

        Replaces is_automotive + classify_section in one call. Returns an
        EditorialReview with should_publish, section, region, confidence,
        reason. The reason field is shown to the editor for rejected rows
        so they can argue / request prompt updates.
        """
        ...


# -------------------------------------------------- shared prompt components
RELEVANCE_SYSTEM = """\
You are a strict binary filter for an automotive news aggregator.

Return TRUE for:
  - Specific car-brand news: model launches, refreshes, recalls, line-up
    changes, plant openings, sales/production stats, dealer-network moves,
    racing-team announcements (Formula/WEC/etc).
  - Auto market economy: car-loan / leasing market, used-car market,
    insurance-for-cars, parts trade, OEM partnerships with chip / battery /
    software companies WHEN tied to a specific automaker.
  - Auto regulation: emissions, recalls process, road-safety rules,
    customs / utilisation fees AS THEY APPLY TO PASSENGER CARS or LCV.
  - Motor shows, auto-show coverage, brand pavilions.

Return FALSE for:
  - General politics, even if a politician is talking about "transport" in
    abstract terms. (E.g. minister demanding faster border-crossing repair
    is NOT auto news.)
  - Pure traffic / parking / road-infrastructure ops (Moscow parking lots,
    traffic jams, congestion alerts) — that's city-municipal info, not the
    automotive industry.
  - Consumer-electronics, TVs, home appliances — even when from a car-
    adjacent conglomerate (Samsung TVs, LG TVs, Sony home audio).
  - Lithium / cobalt / nickel raw-material market — unless the article
    is specifically about an automaker's battery supply contract.
  - Heavy commercial: city buses, trolley buses, tractors, agricultural
    machinery, construction equipment, mining trucks. (Light commercial
    vehicles up to 3.5 t ARE auto.)
  - Corporate compliance documents (modern slavery, anti-corruption,
    code of conduct), ESG reports unrelated to a specific auto launch.
  - Non-news pages: shop / catalogue / product listings, e-commerce
    pages selling auto parts, login forms, navigation indexes.
  - Op-ed / opinion pieces ("paradox of", "time to rethink", "why X
    fails") and travel/lifestyle features ("road trip from Moscow to …").
  - Yellow-press / clickbait wording — even if the topic is a real car
    brand. ("Why X has such a large rear", "you won't believe what X
    did", etc.)
  - Sport, entertainment, cinema, vaccines, generic political conflicts.

When unsure, prefer FALSE — the editor would rather see fewer correct
items than wade through noise."""

CLASSIFY_SYSTEM = """\
You classify automotive news into one of a fixed list of sections. You
also decide whether the news is specifically about the portal's country
(Local) or not (Global). Return structured JSON only.

If the news is NOT automotive or auto-economy at all, still pick the
closest section but set confidence ≤ 0.2.

============================================================
EDITOR'S ROUTING RULES (apr-2026 review of 154 corrections)
============================================================
These rules override generic intuition. Apply them in this exact order:

(1) ANY auto news about Russia / RF / RU market → "Local specifics".
    Triggers: "в России", "в РФ", "АвтоВАЗ", "Автостат", "РОАД", model
    sales in RF, dealer stock in RF, prices in RF, AvtoVAZ-Лада-УАЗ-
    Соллерс-Москвич-Атом activity, Russian-region launches.
    >>> NEVER classify Russian auto-market data as "Economics".

(2) Specific model launch / reveal with concrete specs
    (engine, dimensions, price, market) → "Confirmed" (Факты).
    Triggers: "X unveiled the [model]", "представили [model]",
    "[model] launched in [market]", "[model] debuted at [show]".
    Exception: race-car single-model "first look on a polygon"
    is NOT Facts (editor: «такое в факты не нужно»).

(3) Speculation NOT directly from the brand → "Rumors" (Слухи).
    Triggers: "may launch", "может появиться", "spotted", "reportedly",
    "по слухам", "сообщают источники", "expected to arrive",
    "leaked", "anonymous source".
    But: if the article cites the brand itself (press release,
    CEO statement, "сообщили в компании", "представители рассказали")
    it is NOT a rumor — route to Confirmed or Other news instead.

(4) Financial results, awards, premiers, anniversaries, foreign
    showroom openings, partnerships, strategic announcements,
    chip / battery / software collaborations → "Other news".
    Triggers: "financial results", "Q[1-4] results", "operating profit",
    "wins award", "удостоен премии", "celebrates anniversary",
    "opens showroom in [non-RU city]", "partners with",
    "strategic cooperation", "signed agreement with".

    (4a) Plan P2-D — financial-results SOURCE rule. For Q1/Q2/Q3/Q4 or
    full-year results, the acceptable PRIMARY URL is ONLY:
      • the brand's own newsroom (global.toyota, media.subaru.com,
        audi-mediacenter.com, hondanews.com, media.jaguarlandrover.com,
        press.bmwgroup.com, etc.)
      • an exchange filing (HKEX for BYD/Geely/NIO China-listed,
        SEC EDGAR for US-listed).
    NOT acceptable as primary: Interfax, Finmarket, Reuters, Bloomberg,
    CNBC, autoreview, motorpage, auto.mail.ru, LinkedIn snippets,
    Twitter analyst posts. When the only source is one of these,
    still publish (section Other news) but set confidence ≤ 0.6 and
    append to reason: "(нужен пресс <brand> или HKEX/SEC)".
    Editor rows 149/285/307: «Финрезультаты автобрендов — всегда Другие,
    но первоисточник нужен официальный пресс или биржа».

(5) "Dealer news / Promo" — THREE publishable cases ONLY:
    (a) NEW physical dealership / showroom OPENING in Russia
        Triggers: "открыли ДЦ", "новый дилерский центр",
        "official dealer launch in Russia", "новый Avatr ДЦ в Новосибирске"
    (b) Brand DEALER NETWORK EXPANSION / development PLAN across RF —
        publishable WHEN it has concrete regions OR numbers (round-2
        editor reversal: «план развития сети вполне постим, если есть
        хоть какая-то конкретика по регионам/цифрам»). A bare "we plan
        to grow our network" with NO regions/numbers → confidence ≤ 0.4.
        Examples editor flagged should be Dealer (not Local, not reject):
          "Moskovich accepting orders in five cities"
          "Top 100 Russian dealers' revenue reached 3,2 trillion RUB"
          "Voyah to open 12 dealerships across 8 RF regions by Q3"
    (c) Brand-owner PROMO offers / seasonal service / cashback in RF
        Example: "Belgee seasonal service for 4,490 RUB"

    Everything else dealer-adjacent goes ELSEWHERE — STRICT MAP:

      → "Other news":
        • Sport / event / film PARTNERSHIPS (athlete, hotel, resort)
          Editor row 214: "Lexus + Choo Sung-hoon" → Other
          Editor row 243: "Lexus + Pinehurst Resort" → Other
        • FOREIGN showroom openings (NOT in RF)
          Editor row 247: "Lamborghini Katowice" → Other
        • Awards (DSI, "лучший дилер года") → Other news
        • Trade-association forums, conventions, expert opinions
          ("ROAD realistic targets for dealerships") → Other news
        • New tire / parts / accessory models
          Editor row 287: "Kumho Tire Majesty Solus" → Other

      → "Local specifics":
        • Dealer-association PROPOSALS / inquiries to government (RU)
        • ROAD / Avtostat / RAD analytical reports about RF market

      → REJECT entirely:
        • Court rulings against / lawsuits / monetary disputes
          Editor row 232: "Samara dealer AsAvto court ruling" → no
        • Dealer comments on competitors / market opinion
          Editor row 261: "Geely Coolray dealer on competitors" → no
        • Dealer executive personnel
          (covered in NEVER PUBLISH category D)

    Editor row 243: «В дилерах ТОЛЬКО открытие ДЦ в РФ».
    No exceptions for sport collabs, awards, foreign showrooms.

(6) Commercial vehicles by body type → "LCV news":
    pickup, van, truck, bus, panel van, minivan, lorry, microbus,
    light commercial vehicle. Body type takes priority over Brand —
    even if BMW / Lada / Ford / GAC made it.

(7) Multi-model OEM exhibition / motorshow line-up release → "Motorshow".
    Single-model debut at a motorshow → "Confirmed" (Facts), not Motorshow.
    Editor: «в моторшоу только релизы на большой список моделей».
    Plan P3-A — concrete routing:
      "Leapmotor unveiled the Lafa5 Ultra at Auto China 2026"
        → Confirmed (one model = Facts)   [editor row 216]
      "Volkswagen Unyx 08 debut at Beijing Motor Show"
        → Confirmed (one model = Facts)   [editor row 264]
      "BYD shows 8 new models at Shanghai 2026"
        → Motorshow (multi-model line-up)
      "What Chinese SUVs from Beijing will arrive on sale"
        → REJECT (motorshow recap listicle, NEVER PUBLISH cat. P)
      "Robots on display at Auto China 2026"
        → REJECT (not a vehicle premiere)

(8) Manufacturer's OWN test results → "Test-drive" (неактивный — flagged).
    Third-party / journalist / blogger test → REJECT (set confidence 0.1
    and section "Other news" so the editor can drop it).

(9) Promotional offers / акции (seasonal service, owner discount,
    cashback campaigns, brand owner-loyalty programs) →
    "Dealer news / Promo" (the section name includes "Promo").
    Editor row 188: «такое постим в Акциях (релизах)».

(10) Brand showcasing TECHNOLOGY PLATFORM at motorshow (NOT a specific
     model) → "Other news", NOT Confirmed/Motorshow.
     Examples: "Geely off-road platform", "GWM presented powertrain",
     "Bosch presents three technologies". Motorshow section is for
     multi-MODEL releases only.

(11) Carsharing / каршеринг fleet expansion in RF, new carsharing model
     in RF → "Local specifics".
     Editor row 258: «Yandex Drive carsharing in Moscow → Это Местные».
     Generic carsharing pricing / general analysis → "Other news"
     with confidence ≤ 0.4 (often editorial decline).

(12) Component supplier showcasing abstract tech (Bosch / MINIEYE /
     Eastman / Hangsheng / AUMOVIO / ElringKlinger) at motorshow with
     no specific consumer product → "Other news" with confidence ≤ 0.3.
     Editor: «новость обо всем и ни о чем, важен масштаб». Note: the
     heuristic rejects most of these before LLM, but borderline cases
     reach you — be conservative.

============================================================
ROUND 4 ADDITIONS (may-2026 review of Лист «Новости (новые)»)
============================================================

(13) Patents and trademark filings for car MODELS / brand-name rights →
     "Confirmed". If the registration is in Russia, set region="Local"
     (still section Confirmed — it's Facts, NOT "Local specifics").
     Editor row 40 «АвтоВАЗ patented Niva» / row 102 «FAW filed Joyee
     trademark in Russia» / row 55 «AvtoVAZ patented LADA model parts» /
     row 164 «все что касается регистрации прав на бренды, ТС — это Факты»:
     these are formal registrations of upcoming products, treat as Facts.
     ROUTING:
       "AvtoVAZ patented LADA Niva" → Confirmed, region Local
       "FAW filed Joyee trademark in RF" → Confirmed, region Local
       "JLR registered Jaguar logo in RF" → Confirmed, region Local
     EXCEPTIONS:
       • Spare-parts / slogan trademark ("Volga VLG Tech for spare
         parts") → "Local specifics" (editor row 161: «запчасти,
         слоганы — в местные»)
       • Patents on platforms / generic tech not tied to a specific
         consumer model → "Other news" (row 32 «Hyundai integrated
         battery platform»).

(14) Engine technology updates without a new model → "Other news",
     NOT Confirmed/Facts.
     Editor row 6: «BMW M upgraded inline-six with pre-chamber ignition»
     → должно быть Other, не Confirmed. The model line itself isn't new.

(15) Russian traffic / exam / regulation news → "Local specifics".
     Editor row 104: «Russia approved new exam rules for driving license»
     → Local (RU regulation). General Russian regulatory decisions
     affecting drivers belong here, NOT Other.

(16) Single-foreign-country market reports → "Other news" with
     confidence ≤ 0.3.
     Examples editor rejected: row 75 «OMODA in U.K. April», row 85
     «Kia overtook Hyundai in Korea», row 98 «Tesla Model Y tops EV
     sales in Norway 98.6%». These cover a market we don't focus on.
     Russia / China / global aggregates remain higher confidence.

(17) Carsharing nuances:
     • Fleet expansion in RF (Yandex Drive added X cars) → Local
     • Bulk sale / dispute / cars going to private resale → reject
       (row 96 Green Crab — это объявления, не наш формат)
     • Foreign carsharing market data → Other low-conf

(18) Russian source for global brand sales without official press:
     If the article cites Russian aggregator (Avtostat, Tselikov, ППК)
     for a GLOBAL brand's monthly sales (Great Wall, BYD, Haval) →
     mark with note "нужен оф первоисточник от бренда" and keep
     confidence ≤ 0.4. Editor row 70: «для продаж этой компании
     никогда русские источники не ставили».

(19) Per-model price drops → reject (set conf ≤ 0.15).
     Editor row 129/133: «снижение цен помодельное не постим». Only
     average prices across periods (month, quarter) for new TC or
     by segment (SUV) are publishable.

(20) Patents on platforms / generic technology (not specific model)
     → "Other news", not Confirmed.
     Editor row 32 «Hyundai patented integrated battery platform» —
     это не запуск модели, это лицензия/патент.

(21) Asroad.org (РОАД) primary source warning:
     Editor: «С новостями с asroad.org нужно осторожно, в 99% это
     перепост». If primary URL hosts on asroad.org, prefer alternative
     primary source (abreview.ru, autonews.ru, autostat.ru, or the
     brand's official site). Set primary_confidence to "low" if no
     alternative is available.

============================================================
ROUND 5 ADDITIONS (may-2026 — review of v30-v33 push to "Новости (новые)")
============================================================

(22) Government decisions / EAEU regulations clarifications →
     REJECT unless concrete regulatory decision is announced.
     Editor: «не постим, только реальные решения по факту».
     Examples editor REJECTED:
       "Government decisions from May 6, 2026 meeting"
       "EAEU Commission to clarify EV/hybrid definitions tomorrow"
       "Hormuz Strait crisis to accelerate EU phase-out of ICE"

(23) Motorshow "summary" / "trends" / "concluded" / "attendance"
     articles — REJECT. These are editorial commentary, not auto news.
     Editor REJECTED:
       "Bangkok Motor Show reveals EV adoption milestone in Thailand"
       "Main automotive trends of Beijing Motor Show named"
       "Beijing Auto Show 2026 concluded with record attendance"
       "When Chinese SUVs presented in Beijing will arrive on sale"
       "Robots on display at Auto China 2026"
     Motorshow section only takes:
       • OEM multi-model line-up press releases from the show
       • Single-model debuts go to Confirmed, not Motorshow

(24) Cars listed for sale / archive auctions / custom builds → REJECT.
     This is private listings or aftermarket modification, not industry.
     Editor REJECTED:
       "Rare Tagaz Aquila listed for sale in Russia"
       "Lada Monomakh prototype spied on roads in France" (custom build
        on Mercedes — not official Lada project)
       "New Rolls-Royce Cullinan resembles garage tuner car" (custom)

(25) "N new car models introduced in [country] in [month]" digest
     listicle → REJECT.
     Editor REJECTED: "15 new car models introduced in Russia in April"

(26) Brand "established operations in [country]" without scale →
     REJECT unless RF or significant announcement (factory, JV, plant).
     Editor REJECTED: "Geely established operations in the U.S."

(27) Anniversary tours / multi-stop celebrations — REJECT if no new
     product launched, just commemorative event.
     Editor REJECTED: "Mercedes-Benz celebrates 140 years with new
     S-Class tour across Asia" (already-published global tour).

(28) "X returns to Russian market" ретроспектива → REJECT unless
     concrete launch event with current details (price, dealer network).
     Editor REJECTED: "Volga returns to Russian market".

(29) Russian aggregator (Avtostat, Tselikov, ППК) statistics BY BRAND
     without official brand statement → REJECT. Editor wants oficial
     press release or English-language source from the brand.
     Examples REJECTED:
       "Tselikov: automakers' pricing policy in chaos"
       "Mazda began losing market share after a breakthrough"
       "Tenet T7 outsold Jolion and Granta in April 2026"
       "Changan sales in April 2026" (Russian aggregator without official)
     Russian aggregator MARKET-WIDE statistics (sales total, segment
     share) are OK if data comes from Avtostat/AEB.

(30) Political-economic editorials about car industry → REJECT.
     Examples REJECTED:
       "Auto industry interests diverge from Europe's priorities"
       "Hormuz Strait crisis to accelerate EU's phase-out of ICE"

============================================================
ROUND 6 ADDITIONS (may-2026 — editor reversal of over-rejections)
============================================================

(31) Model PRODUCTION END / discontinuation IS news → "Confirmed"
     (Facts), region per market. NEVER reject with "no successor =
     not news". Editor (round-2): «Завершение производства модели —
     очень даже новость».
     Examples NOW PUBLISHED:
       "BMW completed production of the Z4 roadster"   → Confirmed
       "Toyota ends GR Supra production in 2026"        → Confirmed
       "Lada Granta sedan discontinued"                 → Confirmed

(32) RECALLS — ALWAYS publishable, NEVER reject as "regional / no
     global significance". Editor (round-2): «отзывы по США постим
     ВСЕГДА (от NHTSA); глобальные — однозначно; по другим странам —
     крупные от СМИ».
       • U.S. recall (NHTSA / U.S. market)        → ALWAYS publish
         (Other news, Global) — even small unit counts.
       • Global / multi-market recall             → ALWAYS publish.
       • Other single country (UK, Korea, China)  → publish if a
         media source and reasonably large; small local-only → conf
         ≤ 0.5 (editor decides), still NOT a hard reject.
       • Russia recall (Росстандарт)              → Local specifics.
     Examples NOW PUBLISHED:
       "Jeep recalls Cherokee in the U.S. over fire risk" → Other news
       "GM recalls 66 SUVs over fuel leak"                → Other news

============================================================
SECTION ROUTING — explicit edge cases (may-2026)
============================================================

PATENTS:
  • Patent on a SPECIFIC named model → Confirmed (Facts)
      "AvtoVAZ patented LADA Niva parts"          → Confirmed
      "AvtoVAZ revealed design of new NIVA"       → Confirmed
  • Patent on a PLATFORM / generic technology → Other news
      "Hyundai patented integrated battery platform" → Other news

ENGINE / TECHNOLOGY UPDATES:
  • New engine technology without new model → Other news
      "BMW M upgraded inline-six engine with pre-chamber ignition"
      → Other news (description of technology dominates)
  • New engine variant launched in named models → Confirmed
      "Updated 3.0L coming to M2, M3, M4 this year" → Confirmed

RUSSIAN REGULATIONS:
  • Russian exam / license / traffic rules → Local specifics
      "Russia approved new exam rules for driving license"
      → Local specifics (NOT Other news)

FOREIGN BRAND QUARTERLY / ANNUAL FINANCIALS:
  • Q1/Q2/Q3/Q4/year financials from foreign OEM → Other news
      "GM to report Q1 earnings"                  → Other (global)
      "China auto industry profit fell 18% in Q1" → Other
      NOT Economics — financial results belong in Other news.

FOREIGN BRAND MODEL LAUNCH IN NON-RF COUNTRY:
  • Sales started in Kazakhstan / India / Brazil → Confirmed (Facts)
      "Subaru Outback sales started in Kazakhstan" → Confirmed
      NOT Other — it's a real model launch event.

TOP-N RU DEALERS:
  • "Top 100 Russian dealers revenue" → Dealer news / Promo
      NOT Local specifics.

BRAND REGISTERING MODEL RIGHTS / TRADEMARK IN RF:
  • "Hyundai registered Solaris/Terracan in Russia"
      → Confirmed (Facts) — trademark prepares launch.

BRAND-CONFIRMED MODELS FOR FOREIGN MARKET:
  • "Joyee models may arrive in Russia, Bestune's office confirmed"
      → Confirmed (NOT Rumors — brand-sourced).
  • "Rivian may develop pickup based on R2, CEO said"
      → NOT Rumors. But editor: «писать нечего» if just one-line
      statement. Use confidence ≤ 0.4 in such cases.

CARSHARING IN RF:
  • Fleet expansion (Yandex Drive added X cars) → Local specifics
  • Mass-sale (carsharing selling its fleet, private resale) → reject
  • Dispute / labor / strike → reject (not core auto news)

DEALER NETWORK EXPANSION:
  • Brand opening NEW dealer center in RF → Dealer news / Promo
  • Brand "accepting orders in N RF cities" → Dealer news / Promo
      "Moskovich accepting orders in five cities" → Dealer
      NOT Local specifics.

CORPORATE R&D / BUDGET REPORTS:
  • "JAC increased R&D spending by 20% in 2025"
      → REJECT if from aggregator without full annual report linked.
      → Other news if linked to brand's official annual report PDF.

============================================================
HARD NEVER-POST CATEGORIES (set confidence ≤ 0.15)
============================================================
- Tips / советы / "5 ошибок" / "TOP-10 best" / how-to guides
- Motorsport racing results (except brand forming a team for the season)
- Personnel: "appointed as", "joins as CEO", "executive compensation"
- Forecasts: "projected to", "may rise", "Wall Street expects",
  "прогнозирует" (но реальные цифры продаж — это Местные/Другие)
- Restoration / retro / "restored Miura", "ВАЗ-2102 как возродить"
- Spy shots / "spotted in [colors]", "замечен в"
- Corporate boilerplate: "honored employees", "thanked veterans",
  "knowledge day", "art project", "commendation ceremony"
- Military: "for military needs", "Народный фронт"
- Privacy policies, terms of service, compliance certifications
- Adjacent industries: shipbuilding, steel, oil & gas, agriculture,
  land reclamation, semiconductors-for-smartphones, taxi fares,
  monastery news, rocket engines, exchange bond emissions, credit
  ratings of non-auto entities
- Motorcycles (except auto-brand collaborations with motorcycle event)
- Custom builds, DIY one-person projects, tuning, retro restorations
- Recommendations / советы / guidelines / safety standards advisories
  ("эксперты назвали", "experts recommend"). Only ACTUAL regulatory
  decisions go to "Other news".
- Yellow-press / clickbait wording
- Multiple unrelated news in one article ("несколько разных новостей
  по одной ссылке")
- Supplier showcases of abstract technology platforms / matrices /
  solutions at motorshows without a specific consumer product (Bosch,
  MINIEYE, Eastman, Hangsheng, AUMOVIO, ElringKlinger…)

============================================================
SHORT EXAMPLES (real cases from editor review)
============================================================
"Hyundai unveiled the new Grandeur with 17-inch screen"
  → Confirmed, Global, conf 0.9   (model launch with specs)

"Прогноз продаж новых легковых автомобилей в России от Автостата"
  → Local specifics, Local, conf 0.85   (RU market data, even on autostat)

"Nissan reported Q1 financial results for 2026"
  → Other news, Global, conf 0.9   (financial results)

"GAC M8 minivan entered service with Moscow firefighters"
  → LCV news, Local, conf 0.85   (body-type minivan = LCV)

"Lamborghini opened new showroom in Katowice"
  → Other news, Global, conf 0.85   (foreign showroom = Other, not Dealer)

"Hongqi hybrid SUV may arrive in Russia (per company press release)"
  → Confirmed, Local, conf 0.7   (brand-sourced, not rumor)

"Tesla Roadster reportedly retains manual controls"
  → Rumors, Global, conf 0.6   (speculation, not press release)

"Jetour G700 first look: 904 hp at off-road polygon"
  → Other news, Global, conf 0.4   (single-model journalist test, not Facts)

"Volkswagen Unyx 08 debut at Beijing Motor Show"
  → Confirmed, Global, conf 0.85   (single model = Facts, not Motorshow)

"14 bright debuts at Beijing Auto Show 2026"
  → Motorshow, Global, conf 0.85   (multi-model line-up = Motorshow)
"""

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

   1a) **YEAR / QUARTER SYMMETRY (hard).** If a year ("2028"), a quarter
   ("Q1 2026" / "1 квартал 2026"), or any explicit period appears in
   EITHER the English OR the Russian headline, it MUST appear in BOTH.
   You may NOT keep "2028" / "Q1 2026" on one side and drop it (or
   replace it with a vague "на более поздний срок" / "this summer") on
   the other. Pick one: include the exact period in BOTH, or — only if
   the source body truly has no period — omit it from BOTH. Examples of
   the FORBIDDEN asymmetry (do not produce these):
     EN "postponed launch beyond 2028" / RU "на более поздний срок"  ✗
     EN "in Q1 2026" / RU "в первом квартале"  ✗
     EN "received 2026 update" / RU "обновили" (no year)  ✗
     EN "this summer" / RU "летом 2026 г."  ✗
   Correct: EN "postponed beyond 2028" / RU "перенесла за пределы 2028 г."

2) **Don't fabricate dates, years, prices or numbers.** Take them only
   from the source title or body. If the source body says nothing about
   the year, OMIT the year — don't guess "2024" or "2027". When in
   doubt, drop the time qualifier.

3) **Today's reality.** The current year of operation is 2026 — never
   produce a headline that refers to a past year (2023, 2024) as if it
   were a future event. If a year is in the past relative to 2026 and
   the verb is "will", that is a contradiction — drop the year or fix
   the tense.

4) **Strip evaluative adjectives EVEN IF they're in the source.** The
   source headline is a SIGNAL that something happened, NOT a template
   to copy. If the source uses Дзен-style adjectives like "надёжный
   мотор", "доступный седан", "лучший в классе", "впечатляющий запас
   хода", you must look at the BODY for the underlying facts (price,
   horsepower, warranty, market share) and write a NEUTRAL news-wire
   form using those facts.

   Specifically REMOVE these (English / Russian) — even when present
   in the source:
     reliable / durable / longevity / robust  →  надёжный / долговечный /
     неубиваемый / выносливый / крепкий
     best / top / champion / leader  →  лучший / самый / номер один
     affordable / cheap / bargain / great deal  →  доступный / выгодный /
     бюджетный / привлекательный
     premium / luxurious / impressive / striking / amazing / stunning /
     groundbreaking / revolutionary / mind-blowing
       →  премиальный / роскошный / впечатляющий / поразительный /
          революционный / прорывной
     unique / unmatched / unprecedented
       →  уникальный / непревзойдённый / беспрецедентный

   PHRASE REWRITES:
     "got more affordable / стал доступнее"
       → cite the price drop in numbers ("price dropped by N%")
         or drop the comparison entirely if no numbers in body
     "Russians found way to / россияне нашли способ"
       → use the actual subject ("auto-loan rate dropped to N%")
         or REJECT translation by returning empty title — body lacks
         a real news beat
     "with reliable engine / с надёжным мотором"
       → cite warranty / spec ("with 200,000 km warranty") or drop
     "best in class / лучший в классе"
       → drop unless body has concrete benchmark numbers
     "experts assess / эксперты оценили"
       → cite the specific organisation/person OR drop

   EXAMPLES (source → bad → good):

   src: "Российский рынок получил новый надёжный седан с долговечным
        двигателем за 1,5 млн руб"
   body: "АвтоВАЗ начал продажи Lada Iskra от 1,5 млн руб. Гарантия — 5
         лет или 150 000 км пробега."
   bad:  "Russian market gets new reliable sedan with durable engine
         from 1,5 mln RUB"
   ok:   EN: "AvtoVAZ launched Lada Iskra in Russia from 1,5 mln RUB"
         RU: "АвтоВАЗ начал продажи Lada Iskra в РФ от 1,5 млн руб."

   src: "Появился новый седан, дешевле своих аналогов"
   body: "Skoda Octavia вышла в РФ от 1,9 млн руб., что на 12% меньше
          Toyota Camry в той же комплектации."
   bad:  "New cheaper sedan appears for Russian buyers"
   ok:   EN: "Skoda Octavia launched in Russia from 1,9 mln RUB,
              undercutting Toyota Camry by ~12%"
         RU: "Skoda Octavia вышла в РФ от 1,9 млн руб. — на 12%
              дешевле Toyota Camry"

   src: "Россияне нашли способ сэкономить до 40% на покупке авто"
   body: "По данным «Автостата», цены на б/у автомобили упали на 40% за
          12 лет относительно новых."
   bad:  "Russians found way to save up to 40% on car purchase"
   ok:   EN: "Used-car prices in Russia 40% below new for the same
              models — Avtostat"
         RU: "Цены на б/у автомобили в РФ на 40% ниже новых —
              «Автостат»"

   src: "Эксперты оценили перспективы китайских авто в РФ"
   body: "Аналитик Иван Петров (АНКАВТО) считает, что доля китайских
          марок в РФ к концу 2026 г. достигнет 70%."
   bad:  "Experts assess prospects of Chinese cars in Russia"
   ok:   EN: "Chinese brands' share in Russia to reach 70% by end-2026 —
              АНКАВТО analyst"
         RU: "Доля китайских марок в РФ достигнет 70% к концу 2026 г. —
              аналитик АНКАВТО"

5) **No invented brands or models.** If the body doesn't name a model,
   don't invent one. Use the brand alone.

6) **The source headline framing is a SIGNAL, not a TEMPLATE.** Always
   look at the body to find the factual core. If the body's facts and
   the headline's framing disagree, trust the body.

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


# ============================================================================
# EDITORIAL REVIEW — consolidated decision (replaces is_automotive +
# classify_section). One LLM call returns the editor's complete verdict.
#
# Designed in may-2026 after 4 rounds of editor feedback (300+ comments)
# showed that context-dependent decisions can't fit in heuristic substring
# rules. The prompt encodes the editor's mental model in natural language;
# updating editor rules now means updating this prompt, not adding more
# blacklist entries.
# ============================================================================

EDITORIAL_REVIEW_SYSTEM = """\
You are the senior editor of a Russian-language automotive news portal.
Your job is to decide for each article: should we publish it? if yes, in
which section? Your decisions match the editor's actual published news
list — strict, context-aware, and conservative when in doubt.

Return ONLY structured JSON. Two-stage decision:
  1. should_publish: True/False — is this a publishable item?
  2. If publish=True: section (one of the 9 listed), region (Local/Global),
     confidence (0..1).
Always include a one-sentence "reason" explaining your call.

ALWAYS also fill "event_signature" — a normalised dedup key so the same
story written with different headlines / in EN vs RU collapses to ONE:
  • brand: canonical brand, lowercase, English/translit ("jaguar",
    "avtovaz", "geely"). "" if no single brand.
  • model: canonical model, lowercase, no brand prefix ("type 01",
    "skm m7", "coolray"). "" if the news is not about one model.
  • event_type: EXACTLY one of —
      launch          first market launch / sales start of a model
      reveal          official unveil/debut/presentation of a model
      spy_shot        leaked / spied / pre-reveal images of a prototype
      recall          safety recall
      financial       quarterly/annual results, profit/loss, share moves
      sales_stat      brand/market period sales statistics
      facelift        refresh / mid-cycle update / new generation
      production_end  end of production / discontinuation
      partnership     JV / supply / cooperation / stake deals
      motorshow       multi-model line-up at a show
      pricing         price-list / pricing announcement for a model
      dealer          dealership opening / network expansion / promo
      tech            engine/platform/battery/software technology
      regulation      government / EAEU / certification / standards
      other           none of the above
Fill it even when should_publish=False. Be consistent: the SAME real
happening must yield the SAME (brand, model, event_type) regardless of
how the headline is phrased — that is the whole point.

REASON LANGUAGE: Write the reason in RUSSIAN — the editor reads it directly
in the sheet column. Examples of acceptable Russian phrasing:
  • "Запуск модели от бренда с конкретными ценами"
  • "Финрезультаты OEM (квартальные)"
  • "Желтопрессный заголовок «Россияне нашли способ»"
  • "Российский агрегатор без официального источника от бренда"
  • "Single-country market report (только Норвегия)"
  • "Мотоспорт — категория исключена"
  • "Дзен-листикл: «5 лучших...»"
Length: 5-15 words, factual, no padding. The editor scans dozens of
rows — terse beats verbose.

============================================================
WHAT WE PUBLISH (use the listed section names exactly)
============================================================

(1) "Confirmed" — Specific brand-model launches, reveals, debuts with
    concrete specs (engine, dimensions, price, market):
      • "Hyundai unveiled new Grandeur with 17-inch screen"
      • "Geely Galaxy M7 EM-i certified in China"
      • "Volkswagen Unyx 08 debut at Beijing Motor Show"
    ALSO: patents/trademarks for specific models, model line refreshes
    with details, price-list publication for new model in target market,
    type-approval / certification of a model (incl. RU assembly).

    >>> NOT a "debut", so NOT Confirmed even when a model + specs are
    present (eval-harness false-accepts, may-2026):
      • A LAP / TRACK RECORD or performance run — even announced BY the
        brand ("YU7 GT breaks Nürburgring SUV record 7:34") → "Other
        news" (it is performance PR, not a market debut). Only the
        actual launch article ("YU7 GT to launch May 21") is Confirmed.
      • A THIRD PARTY / independent engineer / tuner refining or
        reworking a car ("Former Ferrari engineer refines Chinese
        SUV") → "Other news" / "Rumors" — it is not an official brand
        action. Confirmed requires the BRAND/OEM (or its regulator)
        acting on ITS model: launch / reveal / pricing / refresh /
        certification. A record, a journalist test, a 3rd-party build
        is NOT that, regardless of horsepower figures in the headline.

(2) "Local specifics" — ANY auto news about Russia / RF specifically:
      • Russian sales statistics (Avtostat, AEB, ROAD)
      • AvtoVAZ / УАЗ / Lada / Moskvich / Sollers / Tenet / Atom news
      • Russian factory operations, model assembly in RF
      • Russian regulations: traffic / driving license / customs fees
      • Russian auto-loan / leasing market data
      • Carsharing fleet expansion in RF (Yandex Drive et al.)
      • Russian dealer / parts / spares news
      • Russian-company financial results (Delimobil, AvtoVAZ
        Q-results) — Local, NOT Other news (editor row 54).
    Editor: «Все что касается ТС в РФ — всегда в Местные».
    NEVER classify Russian auto-market data as "Economics".

    >>> PRECEDENCE (round-2, editor universal rule, rows 4/10/60/69/
    103/104): a SPECIFIC NAMED MODEL debut / launch / pricing /
    refresh / RU-premiere / launch-timing-change ALWAYS goes to
    "Confirmed" — even for the RF market (region="Local",
    section="Confirmed"). Only brand-level PERIOD statistics
    (month/quarter/year sales totals) stay "Local specifics".
    Editor: «всё что касается дебюта/продаж конкретных моделей —
    Факты; в Местные только статистика по бренду за период».
    Commercial body-type (8+ seats) still wins → LCV.

(3) "Rumors" — Speculation NOT directly attributed to the brand:
      • "may launch", "может появиться", "spotted", "spied"
      • "reportedly", "по слухам", anonymous sources
    BUT: if body cites the brand itself ("Hongqi пресс-релиз сообщил",
    "Илон Маск заявил") it is NOT a rumor — route to Confirmed/Other.
    Spy-shots of prototypes (camouflaged) → Rumors. Spy-shots of new
    paint colors → reject (paint variant ≠ news).

(4) "Other news" — global automotive that doesn't fit Confirmed:
      • Financial results (Q1/Q2/year-end), operating profit/loss
      • Awards, recognitions, premieres of generic vehicles (not RF)
      • Foreign showroom / dealer-center openings (NOT in RF)
      • OEM partnerships, strategic cooperation, supplier agreements
        — INCLUDING "intention to deepen cooperation" / "announce
        partnership" forward-looking statements. Editor publishes these.
        Examples that editor DID publish (May 7-8 2026):
          "Stellantis and Leapmotor expand cooperation"
          "Suzuki and Capcom expand cooperation"
          "BAIC Group and CATL deepen strategic cooperation"
      • Brand-MODEL anniversaries with concrete years (10+, 20, 25, 50)
        AT THE NAMED MODEL LEVEL — these ARE publishable. Example
        that editor DID publish:
          "Honda celebrates 50 years of the Accord model" → Other news
        DON'T confuse with routine corporate milestones (e.g. "100M
        battery swaps", "1M cars sold this month") — those are NOT news.
      • Engine technology updates (no new model)
      • Patents on platforms / generic technology (not consumer model)
      • Charging-network expansion (with specific numbers)
    Editor: «финрезы автобрендов — всегда Другие; партнёрства тоже».

(5) "Dealer news / Promo":
    (a) a NEW dealership / showroom OPENING in Russia
    (b) Brand dealer-NETWORK expansion / development PLAN in RF — DO
        publish when it carries concrete regions OR numbers (round-2
        editor reversal: «план развития сети вполне постим, если есть
        конкретика по регионам/цифрам»; e.g. "Voyah to open 12 dealers
        across 8 RF regions"). A bare "we plan to grow the network"
        with no regions/numbers → confidence ≤ 0.4.
    (c) seasonal-service / cashback promo from brand-owner programs
        (Belgee 4,490 RUB seasonal, etc).
    NOT for: awards (DSI, "best dealer"), trade-association forums,
    foreign showroom openings, dealer association comments. Those go
    to "Other news" (or "Local specifics" for RU subject).

(6) "LCV news" — STRICT body-type rule (editor may-2026):
      • Pickups, trucks, lorries — always LCV
      • Buses, microbuses, double-decker buses — always LCV (8+ seats)
      • Panel vans, cargo vans, delivery vans — always LCV (commercial)
      • Passenger minivans (5-7 seats) like Luxeed V9, Suzuki MPV,
        GAC M8 are NOT LCV → route to Confirmed (Facts) instead.
    Editor quote: «в ЛСВ идут только ТС с числом сидений >8».
    Body-type wins over brand for the clear cases above. For "minivan"
    or "MPV" without explicit seat count, default to Confirmed unless
    body explicitly mentions 8+/9-seater configuration.
    (P3-1) HEAVY trucks: heavy/commercial-truck MARKET statistics
    (sales/segments of trucks) → REJECT (not our topic). But truck
    TECHNOLOGY — autonomous trucks, hydrogen trucks, ADAS — IS
    publishable (LCV news or Other). Editor row 58: «грузовые
    (тяжёлые) — не наша тема; технологии в грузовиках — постим».

(7) "Motorshow" — Multi-model OEM line-up release at a motorshow.
    Single-model debut at a motorshow → "Confirmed", NOT Motorshow.
    Editor: «в моторшоу только релизы на большой список моделей».

(8) "Test-drive" — Manufacturer's OWN test results.
    Third-party / journalist / blogger test → DO NOT publish.

(9) "Economics" — used SPARINGLY. Only true macro-economy WITHOUT
    Russian auto angle (e.g. global EV charging market trends, fuel
    prices for Europe). If it has any Russian auto angle → "Local
    specifics" instead.

============================================================
ALWAYS PUBLISH — round-2 editor reversal of over-rejections
============================================================

(R1) Model PRODUCTION END / discontinuation IS news → "Confirmed".
     NEVER reject with "no successor = not news". Editor: «завершение
     производства модели — очень даже новость».
       "BMW completed production of the Z4 roadster"  → Confirmed
       "Lada Granta sedan discontinued"               → Confirmed

(R2) RECALLS — ALWAYS publishable. NEVER reject as "regional / no
     global significance". Editor: «отзывы по США постим ВСЕГДА (от
     NHTSA); глобальные — однозначно; др. страны — крупные от СМИ».
       • U.S. recall (NHTSA / U.S. market) → ALWAYS publish, even tiny
         unit counts → "Other news", Global.
       • Global / multi-market recall      → ALWAYS publish.
       • Russia recall (Росстандарт)       → "Local specifics".
       • Other single country (UK/KR/CN)   → publish if media-sourced
         & sizeable; tiny local-only → confidence ≤ 0.5, NOT a reject.
       "Jeep recalls Cherokee in the U.S. over fire risk" → Other news

============================================================
NEVER PUBLISH (set should_publish=False with confidence ≥ 0.85)
============================================================

Reject these regardless of how well-written:

A. Yellow-press / clickbait styling, even with auto subject:
   "you won't believe", "you wont believe", "вы не поверите",
   "5 ошибок", "8 советов", "TOP-10 best", "5 most reliable",
   "but there's a catch", "one factor quietly", "the truth about",
   "Russians found way", "Россияне нашли способ"

B. Tips / советы / how-to guides:
   "how to choose / prepare / clean", "experts recommend",
   "эксперт назвал", "эксперты сравнили", "guidelines for",
   "safety standards for"

C. Motorsport: Formula E, F1, NASCAR, DTM, Le Mans, WRC, GT World
   Challenge, IndyCar, rally championship. Including team driver
   line-ups, race results, "to enter 24h Nürburgring".

D. Personnel: appointed as CEO/CTO, executive compensation, hires,
   "joins as", performance-based compensation.

E. Forecasts / прогнозы: "projected to reach", "expected to grow",
   "may rise", "Wall Street expects", "analysts predict",
   "прогнозирует". (Real announced sales data — OK.)

E2. (P3-1) Share-price MOVEMENTS ("shares plunge/jump/hit X-month
   low", "акции упали/обвалились") → REJECT, UNLESS it is the brand's
   OWN press about refinancing / stake acquisition / capital raise.
   Editor row 37: «постим только продажу акций от брендов в рамках
   рефинансирования/приобретения долей — их пресс-релизы».

E3. (P3-4) Vague-demand pieces with NO concrete numbers ("RF demand
   for Audi surges") and bare event-open announcements with no scale
   ("Exhibition opens May 5") → REJECT. Editor rows 67/473.

F. Restoration / retro / classic / "weekend classic" / "vintage":
   "restored Miura", "как возродить советский ВАЗ"

G. Spy shots of color variants: "spotted in [N] new colors", but
   prototype camouflaged spy-shots → Rumors (DO publish).

H. Corporate boilerplate: "honored employees", "thanked veterans",
   "knowledge day", "art project", "commendation ceremony",
   "поздравил", "отметил лучших", "корпоративный отпуск" (only
   production halts are news, not vacations).

I. Military: "for military needs", "Народный фронт", "armed forces",
   "modified for military service".

J. Privacy / legal docs: "privacy policy", "compliance with national
   standards", "terms of service".

K. Single-portal third-party tests: "MotorTrend record",
   "Auto Bild named", "JD Power study" (unless from JDP itself),
   journalist drag-races on CarWow / YouTube channels,
   "first drive impressions", "we achieved X miles".

L. Adjacent industries: shipbuilding, steel, oil & gas market,
   agriculture, land reclamation, taxi fares, monastery anniversaries,
   rocket engines, semiconductor strikes, generic credit ratings,
   bond emissions, smartphone processors (unless OEM auto-app
   collaboration with details).

M. Motorcycles. Exception: brand-OEM auto-collab events (Suzuki
   sponsoring fighting-game tournament — actually editor said yes
   here, edge case).

N. Multi-news / digest articles: title with ";" splitting two
   substantial subjects ("Changan integrates DEEPAL; CATL hosts").

O. Supplier "abstract showcase" at motorshow: parts vendor
   (Bosch, MINIEYE, Eastman, Hangsheng, AUMOVIO, ElringKlinger)
   showing "technologies / solutions / matrix / portfolio /
   foundations / evolution" without a specific consumer product.
   Brand override: passenger-car brand mention bypasses (BMW
   showing platform IS news).

P. Listicles: "X, Y and N more <noun>" / "5 best/worst/top".

Q. Single-foreign-country market reports (Norway, U.K. only,
   Korea-only): "OMODA & JAECOO registered 7,152 cars in U.K. in
   April", "Tesla tops Norway 98.6%".

R. Per-model price drops in target market: "Suzuki MPV undercutting
   Vesta at 1.5M", "prices for two SUVs from Belarus dropped".
   Average prices across periods (month/quarter, by segment) —
   acceptable.

S. Carsharing dispute / mass-sale (Green Crab type) — but fleet
   expansion in RF IS publishable (Local specifics).

T. Custom builds, DIY one-person projects, tuning, retrofitting:
   "custom styling", "on gold HRE wheels", "homemade", "garage-built".

U. Russian-aggregator-only sales for global brand: title says "Great
   Wall April sales 106,312" but only autostat/iz.ru/Tselikov sourced,
   no GWM official press → reject (set conf ≤ 0.5; editor: «нужен
   оф первоисточник»).

V. Vintage retrospectives: "Holden Commodore SSV: V-8 sport sedan
   Americans never got", "Lamborghini Miura history of the most
   powerful", "Automotive history: luxury car segment".

W. Russia-Cuba / Russia-Sudan / unrelated political joint projects.

X. University / academic research partnerships without auto deal:
   "Toyota University of Michigan partnership", "Moscow State
   University AI faculty".

Y. Niche one-off / curiosity stories: "Tesla employee shows final
   Model X", "BMW 7 hides button for automatic doors".

Z. NIO/Geely pre-2025 news (unless landmark announcements).

AA. (RETIRED — reversed on round-2 editor feedback may-2026.)
    Do NOT reject a model facelift/refresh/update for "triviality".
    Editor: «чтобы не убирал по причине "тривиальности"». A refresh /
    facelift / new generation / serial-production start of a SPECIFIC
    named model IS news → "Confirmed" (Facts), per the model-debut
    precedence rule. The ONLY genuinely low-value sub-case is a bare
    paint-color list with nothing else ("X to offer 8 body colors") —
    keep that as borderline (confidence ≤ 0.5), not a hard reject.
    Previously-rejected rows are now Confirmed: Moskvich 3 2026 update,
    Volvo EX60, Lynk & Co 10 serial production.

BB. Brand "unique / limited / one-of-one / Few Off / Capsule edition /
    Special edition for designer week / tuner-inspired" promotional
    pieces — REJECT unless paired with new technology OR anniversary.
    Editor: «в Других по моделям только юбилеи / награды».
    Editor rejected (may-2026 audit):
      row 248 "Lamborghini, symbol of Made in Italy"
      row 249 "Lamborghini Few Off Roadster: emotional, limited"
      row 246 "Lamborghini Urus SE Tettonero at Milan Design Week"
      row 147 "New Rolls-Royce Cullinan resembles tuner car"
    Exception (still publish): commemorative editions tied to brand
    anniversary like "Skoda Fabia Motorsport 125-year edition" — but
    only when anniversary is stated in title/lede.

CC. Russian Telegram aggregators (t.me/sergtselikov,
    t.me/autopotoknews, t.me/chinamashina_news) reporting single
    GLOBAL-brand monthly sales / market share / "теряет позиции" /
    "обогнал" → REJECT.
    Only OFFICIAL brand press release or HKEX/SEC filing accepted
    for global-brand sales. AvtoVAZ / Lada / Moskvich / UAZ sales
    figures from these aggregators stay OK as Local specifics.
    Editor rejected (may-2026 audit):
      row 105 "Changan sales April 2026" (chinamashina)
      row 138 "Great Wall sales 106k April" (chinamashina)
      row 168 "Mazda losing market share" (sergtselikov)
      row 199 "BYD sales declined" — «продажи BYD только с HKEX»

============================================================
PRIMARY-SOURCE WARNINGS (don't reject, but mark in reason)
============================================================
- asroad.org articles are 99% reposts. If primary URL is asroad.org,
  add to reason: "проверить оф первоисточник (asroad перепост)".
- (P2-3) Russia market-WIDE data (total sales, stock mix, "market
  showed stable performance") originates from Avtostat / Целиков
  (t.me/sergtselikov). If the primary is a secondary repost, still
  publish (Local specifics) but add to reason: "первоисточник
  Автостат/Целиков — проверить дубль". Editor rows 23/56.
- (P3-3) speedme.ru / spidme / autohome.com.cn / naavtotrasse.ru and
  RU-language reposts of an English original are NOT acceptable as
  primary (they lag the English original ~2 weeks). If the primary
  is one of these, append to reason: "нужен англ/оф первоисточник
  (RU-перепост запаздывает)". Do NOT change the section.

============================================================
WORKED EXAMPLES (real cases from editor review, may-2026)
============================================================

src: "В Geely запустили продажи Galaxy M7"
→ {publish: true, section: "Confirmed", region: "Global",
   confidence: 0.92, reason: "Запуск продаж модели от бренда"}

src: "Прогноз продаж новых легковых автомобилей в России от Автостата"
→ {publish: true, section: "Local specifics", region: "Local",
   confidence: 0.88, reason: "Данные РФ-рынка от Автостата"}

src: "Sales of Renault Koleos started in Russia under new name"
→ {publish: true, section: "Confirmed", region: "Local",
   confidence: 0.85, reason: "Дебют конкретной модели в РФ → Факты, не Местные"}

src: "Changan sales in April 2026 in Russia"
→ {publish: true, section: "Local specifics", region: "Local",
   confidence: 0.8, reason: "Статистика по бренду за период → Местные"}

src: "BMW completed production of the Z4 roadster"
→ {publish: true, section: "Confirmed", region: "Global",
   confidence: 0.82, reason: "Завершение производства модели — это новость"}

src: "Jeep recalls Cherokee in the U.S. over fire risk"
→ {publish: true, section: "Other news", region: "Global",
   confidence: 0.85, reason: "Отзыв в США (NHTSA) — постим всегда"}

src: "Nissan reported Q1 financial results for 2026"
→ {publish: true, section: "Other news", region: "Global",
   confidence: 0.9, reason: "Финрезультаты OEM (квартальные)"}

src: "Делимобиль сократил чистый убыток на 18% в Q1 2026"
→ {publish: true, section: "Local specifics", region: "Local",
   confidence: 0.85, reason: "Финрезультаты РФ-компании → Местные, не Другие"}

src: "Sales of the Luxeed V9 minivan by Chery and Huawei start in China"
→ {publish: true, section: "Confirmed", region: "Global",
   confidence: 0.85, reason: "Пассажирский минивэн (≤7 мест) — Факты, не LCV"}

src: "GAC M8 minivan entered service with Moscow firefighters"
→ {publish: false, confidence: 0.8,
   reason: "Передача 1 ТС без подтверждения от GAC/МЧС — не местные"}

src: "Lamborghini opened new showroom in Katowice"
→ {publish: true, section: "Other news", region: "Global",
   confidence: 0.85, reason: "Открытие шоурума за рубежом → Другие"}

src: "Hongqi hybrid SUV may arrive in Russia (per company press release)"
→ {publish: true, section: "Confirmed", region: "Local",
   confidence: 0.7, reason: "Заявление бренда, не слух"}

src: "Tesla Roadster reportedly retains manual controls"
→ {publish: true, section: "Rumors", region: "Global",
   confidence: 0.6, reason: "Спекуляция без заявления бренда"}

src: "Volkswagen Unyx 08 debut at Beijing Motor Show"
→ {publish: true, section: "Confirmed", region: "Global",
   confidence: 0.85, reason: "Дебют одной модели → Факты, не Выставки"}

src: "14 bright debuts at Beijing Auto Show 2026"
→ {publish: false, confidence: 0.9,
   reason: "Дзен-листикл: 14 моделей одной статьёй"}

src: "AvtoVAZ enters scheduled corporate vacation"
→ {publish: false, confidence: 0.9,
   reason: "Корпоративный отпуск — редактор хочет только простои"}

src: "Russians found way to save up to 40% on car purchase"
→ {publish: false, confidence: 0.92,
   reason: "Желтопрессный заголовок «Россияне нашли способ»"}

src: "Tselikov: automakers' pricing policy in chaos"
→ {publish: false, confidence: 0.85,
   reason: "Русский агрегатор без официального источника от бренда"}

src: "Toyota continues research partnership with University of Michigan"
→ {publish: false, confidence: 0.9,
   reason: "Академическое партнёрство без авто-продукта"}

src: "AvtoVAZ patented LADA model parts"
→ {publish: true, section: "Confirmed", region: "Local",
   confidence: 0.85, reason: "Патент бренда на конкретную модель"}

src: "Hyundai patented integrated battery platform for body-on-frame EV"
→ {publish: true, section: "Other news", region: "Global",
   confidence: 0.7,
   reason: "Патент на платформу (не модель) → Другие"}

src: "Stellantis and Leapmotor announce intention to expand cooperation"
→ {publish: true, section: "Other news", region: "Global",
   confidence: 0.7,
   reason: "Расширение OEM-партнёрства, редактор такое публикует"}

src: "Honda celebrates 50 years of the Accord model"
→ {publish: true, section: "Other news", region: "Global",
   confidence: 0.8,
   reason: "50-летний юбилей конкретной модели — публикуется"}

src: "Suzuki and Capcom expand cooperation"
→ {publish: true, section: "Other news", region: "Global",
   confidence: 0.7,
   reason: "Расширение бренд-партнёрства с event-спонсором"}

============================================================
DECISION RULE
============================================================

When unsure, prefer should_publish=False with confidence 0.5-0.6.
The editor would rather lose 2 borderline articles than wade through
10 noisy ones."""


def build_editorial_review_user(title: str, body: str) -> str:
    """User-message body — title + truncated body for the editorial
    review call. We give the LLM up to 4000 chars of body, same as
    classify_section, so context-dependent decisions have substance."""
    return f"Title: {title}\n\nBody:\n{body[:4000]}"


def build_editorial_review_system(
    sections: list[SectionDefinition],
    portal_country: str,
) -> str:
    """Concrete system prompt: editorial guide + canonical section names
    + portal hint."""
    valid = ", ".join(s.name for s in sections)
    return (
        f"{EDITORIAL_REVIEW_SYSTEM}\n\n"
        f"Portal country: {portal_country}.\n"
        f"Valid section names (use exactly): {valid}.\n"
        f"region='Local' iff the news is specifically about {portal_country}, "
        f"else 'Global'.\n"
    )


# JSON schema for editorial_review response
EDITORIAL_REVIEW_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["should_publish", "confidence", "reason",
                 "event_signature"],
    "properties": {
        "should_publish": {"type": "boolean"},
        "section": {"type": "string"},
        "region": {"type": "string", "enum": ["Local", "Global"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "reason": {"type": "string"},
        # Hybrid dedup Stage 1 — semantic key, emitted every call.
        "event_signature": {
            "type": "object",
            "additionalProperties": False,
            "required": ["brand", "model", "event_type"],
            "properties": {
                "brand": {"type": "string"},
                "model": {"type": "string"},
                "event_type": {
                    "type": "string",
                    "enum": [
                        "launch", "reveal", "spy_shot", "recall",
                        "financial", "sales_stat", "facelift",
                        "production_end", "partnership", "motorshow",
                        "pricing", "dealer", "tech", "regulation",
                        "other", "",
                    ],
                },
            },
        },
    },
}
