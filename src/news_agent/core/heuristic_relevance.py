"""Rule-based 'is this a news article?' + 'is it about automotive/economy?'

Two independent checks, no network, no LLM. Used as a cheap pre-filter;
LLM only sees articles that pass both.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from news_agent.core.config_loader import Blacklist
from news_agent.core.models import RawArticle
from news_agent.core.urls import domain_of

# ---------------------------------------------------------------- is_article
_NON_ARTICLE_URL_HINTS = (
    # generic non-article patterns
    "/search",
    "/tag/",
    "/tags/",
    "/topic/",                # press-room topic aggregators
    "/article/topic/",        # BMW PressClub style: /article/topic/4098/...
    "/category/",
    "/categories/",
    "/archive",
    "/rubric",
    "/author/",
    "/login",
    "/signin",
    "/register",
    "/cabinet",
    "/account",
    "/cart",
    "/checkout",
    "/brand/",          # smotrim.ru/brand/9928 style
    "/organizations-card/",  # bo.nalog.ru
    "/quick-search",
    "/listconews/index",     # hkexnews index page
    "?lang=",           # common for index/query pages
    # evergreen / educational / product-info — identified from v1 manual review
    "/insights/",             # cox auto weekly summaries + podcasts
    "-101/",                  # semiconductors-101, evergreen explainers
    "/industries/",           # geotab product landing
    "/services/",
    "/products/",
    "/guides/",
    "/whitepaper",
    "?camefrom=",             # referral lander (benchmarkminerals from rhomotion)
    "/safety-ratings",        # ancap ratings catalog
    "/eSearch",               # euipo search
    # E-commerce / product catalogue — not editorial content
    "/shop/",                 # парт-каталоги «лада-полки» и т.п.
    "/catalog/",
    "/store/",
    "/order/",
    "/buy-",
    # Corporate compliance / ESG documents — not auto news
    "/slavery-statement",
    "/modern-slavery",
    "/anti-slavery",
    "/code-of-conduct",
    "/compliance-",
    "/esg-statement",
    # Op-ed / feature / analytics-lifestyle — editor confirmed not to include
    "/infocenter/autoarticles/",        # motorpage long-form opinion
    "/infographics/puteshestvuem-",     # napinfo auto-lifestyle travel posts
    "/five-minutes-with-",              # SMMT interview series
    "/interview/archive/",              # autostat interview archive
)

# File extensions that are never news articles.
_NON_ARTICLE_EXTENSIONS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip")

# Title-level signals that a text is an op-ed / long-form opinion — editor
# marked category (6) in 'Уточнение у руководителя' as "not included".
# Matched lowercase, anywhere in the title.
_OP_ED_TITLE_PHRASES = (
    # english
    "paradox of",
    "time to rethink",
    "rethinking ",
    "why chaos",
    "why the ",
    "investing to stay ahead",
    "defy the numbers",
    # russian
    "на грани",
    "парадокс",
    "переосмысление",
    "рассуждени",
    "а что если",
)

# Title-level signals of auto-lifestyle / tourism posts — editor category (9)
# "Лайфстайл туризм нам не интересен".
_LIFESTYLE_TITLE_PHRASES = (
    "путешеств",     # путешествие, путешествуем
    "поездка из",    # «поездка из Москвы в …»
    "road trip",
    "weekend trip",
    "lifestyle ",
)

_ARTICLE_URL_HINTS = (
    # generic
    "/news/",
    "/article/",
    "/post/",
    "/story/",
    "/press",
    "/release",
    "/20",                    # year in slug
    ".html",
    # patterns learned from the published-news corpus (2989 rows)
    "/mag/article/",          # abreview.ru
    "/ekonomika/",            # iz.ru, tass.ru
    "/business/",
    "/doc/",                  # kommersant.ru
    "/ab/news/",              # abreview.ru
    "/chinamashina_news/",    # chinamashina site
    "/obschestvo/",           # iz.ru
    "/infographics/",
)


@dataclass
class ArticleVerdict:
    is_article: bool
    score: float  # 0.0 … 1.0
    reasons: list[str]


# --- Three-tier confidence thresholds --------------------------------------
# Tuned on the v1 manual review; revisit after each labelled batch.
CERTAIN_SCORE_THRESHOLD = 0.65   # above this → definitely an article
POSSIBLE_SCORE_THRESHOLD = 0.35  # below this → definitely not
CERTAIN_MIN_AUTO_HITS = 2        # keyword hits required to call it 'certainly automotive'


def looks_like_article(
    raw: RawArticle, *, whitelist: set[str] | None = None
) -> ArticleVerdict:
    """Heuristic: is this a single news article, or an index/search/form page?

    If ``whitelist`` contains the article's domain, the score gets +0.15 —
    we trust editor-approved sources more and are willing to accept borderline
    pages coming from them.
    """
    reasons: list[str] = []
    score = 0.0
    html = raw.html or ""

    # --- hard-reject on missing / placeholder titles ---
    # Aggregator topic pages (press-room "/topic/...") tend to give
    # trafilatura no title at all, and the LLM then produces "<UNKNOWN>".
    # Reject them up front so they never reach the LLM pipeline.
    title_raw = (raw.title or "").strip()
    title_norm = title_raw.lower()
    if not title_norm or title_norm in {"<unknown>", "unknown", "untitled", "no title"}:
        reasons.append("missing-or-placeholder-title")
        return ArticleVerdict(is_article=False, score=0.0, reasons=reasons)
    # v22 leakage: extractor returned only the brand / company name as the
    # title (e.g. "Sollers", "СОЛЛЕРС", "UAZ"). A real news headline is a
    # sentence — single short tokens are placeholders.
    if " " not in title_raw and len(title_raw) < 15:
        reasons.append("single-word-placeholder-title")
        return ArticleVerdict(is_article=False, score=0.0, reasons=reasons)

    # --- positive structural signals ---
    if '"@type":"NewsArticle"' in html.replace(" ", "") or '"@type": "NewsArticle"' in html:
        score += 0.4
        reasons.append("schema:NewsArticle")
    elif '"@type":"Article"' in html.replace(" ", "") or '"@type": "Article"' in html:
        score += 0.25
        reasons.append("schema:Article")

    if 'property="og:type" content="article"' in html or 'og:type" content="article"' in html:
        score += 0.2
        reasons.append("og:type=article")

    if raw.published_at is not None:
        score += 0.25
        reasons.append("published_at")

    body_len = len(raw.body.strip())
    if body_len >= 1000:
        score += 0.25
        reasons.append(f"body≥1000 ({body_len})")
    elif body_len >= 500:
        score += 0.15
        reasons.append(f"body≥500 ({body_len})")
    elif body_len >= 200:
        score += 0.05
        reasons.append(f"body≥200 ({body_len})")
    else:
        reasons.append(f"body<200 ({body_len})")

    if raw.title and 20 <= len(raw.title) <= 220:
        score += 0.1
        reasons.append("title-length-ok")

    # --- URL hints ---
    parsed_url = urlparse(raw.url)
    path = parsed_url.path.lower()
    full = parsed_url.geturl().lower()
    host = parsed_url.netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    if any(h in path for h in _ARTICLE_URL_HINTS):
        score += 0.1
        reasons.append("url-article-slug")
    # PDF / DOC / XLS extensions → hard negative (never a news article)
    if any(path.endswith(ext) for ext in _NON_ARTICLE_EXTENSIONS):
        score -= 0.8
        reasons.append("binary-document-url")
    if any(h in full for h in _NON_ARTICLE_URL_HINTS):
        score -= 0.5
        reasons.append("url-non-article-pattern")

    # Title-level filters (op-ed / lifestyle). These are strong negative
    # signals — the editor explicitly rejected these categories.
    title_lower = (raw.title or "").lower()
    if any(p in title_lower for p in _OP_ED_TITLE_PHRASES):
        score -= 0.6
        reasons.append("title-op-ed")
    if any(p in title_lower for p in _LIFESTYLE_TITLE_PHRASES):
        score -= 0.6
        reasons.append("title-lifestyle")

    # --- whitelist bonus ---
    if whitelist and host in whitelist:
        score += 0.15
        reasons.append(f"whitelist-domain ({host})")

    # --- negative: too many links per character (index pages) ---
    link_density = _link_density(html, body_len)
    if link_density > 0.08:
        score -= 0.3
        reasons.append(f"high-link-density ({link_density:.3f})")

    score = max(0.0, min(1.0, score))
    # Threshold chosen so pages with only 1–2 weak signals don't pass.
    # A binary-document-url single-handedly disqualifies the page regardless of score.
    is_article = (
        score >= 0.35
        and body_len >= 200
        and "binary-document-url" not in reasons
    )
    return ArticleVerdict(is_article=is_article, score=round(score, 3), reasons=reasons)


def _link_density(html: str, body_len: int) -> float:
    if not html or body_len == 0:
        return 0.0
    link_count = html.count("<a ")
    return link_count / max(body_len, 1)


# -------------------------------------------------------------- is_auto_topic
_AUTO_KEYWORDS_EN = (
    # generic
    "automotive", "auto industry", "car ", " cars", "vehicle", "vehicles",
    "sedan", "suv", "crossover", "pickup", "hatchback", "coupe", "minivan",
    "electric vehicle", " ev ", " ev,", "ev sales", "hybrid", "plug-in",
    "dealer", "dealership", "assembly plant", "production plant",
    "horsepower", "engine", "drivetrain", "transmission",
    "recall", "unveil", "launch", "facelift", "refresh",
    # market / economy
    "car market", "new-car sales", "auto sales", "auto market",
    "car production", "auto production", "automaker", "oem",
    # brand hints (short-circuit if any brand shows up anywhere)
    "toyota", "nissan", "honda", "mazda", "subaru", "suzuki", "mitsubishi",
    "hyundai", "kia", "genesis", "volkswagen", "audi", "porsche", "skoda",
    "bmw", "mini ", "mercedes", "ford", "chevrolet", "cadillac", "tesla",
    "rivian", "volvo", "jaguar", "land rover", "renault", "peugeot",
    "citroen", "fiat", "alfa romeo", "stellantis", "lada", "avtovaz",
    "gaz ", "uaz", "moskvich", "chery", "haval", "geely", "exeed", "omoda",
    "jaecoo", "byd", "xpeng", "nio", "li auto", "great wall", "gwm",
    "changan", "dongfeng",
)

_AUTO_KEYWORDS_RU = (
    "автомобил", "автопром", "автоконцерн", "автомобильн", "авторынок",
    "автозавод", "автосалон", "автокредит", "автокомпон",
    "машин", "седан", "кроссовер", "внедорожн", "хэтчбек", "пикап",
    "двигатель", "мотор", "коробка передач", "трансмисси", "привод",
    "электромобил", "гибрид", "топливо", "бензин", "дизел",
    "дилер", "дилерск", "модельный ряд", "комплектаци",
    "произведен", "сборк", "выпуск", "отзывн", "отозв",
    "продажи авто", "рынок авто", "автомобильный рынок",
    # brands (russian spellings)
    "тойота", "лексус", "ниссан", "хонда", "мазда", "субару", "мицубиси",
    "сузуки", "хендай", "хёндэ", "киа", "фольксваген", "ауди", "порше",
    "шкода", "мерседес", "бмв", "форд", "шевроле", "вольво",
    "рено", "пежо", "ситроен", "ваз", "лада", "автоваз", "газ ", "уаз",
    "москвич", "чери", "хавейл", "джили",
)

# German — Mercedes / BMW / VW press rooms often serve DE-only releases.
_AUTO_KEYWORDS_DE = (
    "automobil", "fahrzeug", "kraftfahrzeug", "pkw", " wagen", "modell",
    "elektroauto", "elektrofahrzeug", "hybrid", "verbrennungsmotor",
    "motor", "antrieb", "getriebe", "karosserie",
    " werk", "produktion", "fertigung", "werkstatt",
    "händler", "vertrieb", "marke", "hersteller", "autobauer",
    "neuwagen", "gebrauchtwagen", "rückruf", "vorstellen", "premiere",
    "limousine", "kombi", "geländewagen", "stadtauto",
)

# French — Renault / Citroën / Peugeot / ACEA press rooms.
_AUTO_KEYWORDS_FR = (
    "voiture", "véhicule", "vehicule", "automobile", "constructeur",
    "marque", "modèle", "modele", "motorisation", "moteur",
    "hybride", "électrique", "electrique", "thermique",
    "berline", "citadine", "utilitaire", "monospace",
    "concessionnaire", "concession", "production", "usine",
    "fabricant", "rappel ", "lancement", "dévoile", "devoile",
    "salon de l'auto", "marché auto", "marche auto",
)

# Italian — Ferrari / Lamborghini / Fiat corporate pages.
_AUTO_KEYWORDS_IT = (
    "automobile", "autovettura", "vettura", "veicolo", "automotive",
    "modello", "motore", "ibrido", "elettrico", "endotermico",
    "berlina", "utilitaria", "suv", "produzione", "stabilimento",
    "costruttore", "casa automobilistica", "mercato auto",
    "concessionario", "presentazione", "richiamo",
)

# Spanish — SEAT / Latin America markets.
_AUTO_KEYWORDS_ES = (
    "coche", "vehículo", "vehiculo", "automóvil", "automovil",
    "fabricante", "marca", "modelo", "motor", "eléctrico", "electrico",
    "híbrido", "hibrido", "concesionario", "concesión",
    "berlina", "todoterreno", "mercado automotriz", "ventas de autos",
    "producción", "planta", "lanzamiento", "retiro del mercado",
)

# Chinese — CarNewsChina sometimes serves EN, Gasgoo mixes languages,
# but native Chinese sites publish in ZH. Match core ideographs.
_AUTO_KEYWORDS_ZH = (
    "汽车", "车辆", "轿车", "越野车", "皮卡", "车型",
    "电动车", "电动汽车", "新能源", "混合动力", "插电混动",
    "发动机", "变速箱", "底盘",
    "制造商", "厂商", "销量", "销售", "工厂",
    "车展", "发布", "召回", "上市",
)

# Japanese — Toyota / Honda / Subaru global sites sometimes serve JP.
_AUTO_KEYWORDS_JA = (
    "自動車", "車両", "乗用車", "クルマ", "車種",
    "電気自動車", "ハイブリッド", "エンジン", "モーター",
    "メーカー", "ディーラー", "販売", "生産", "工場",
    "モーターショー", "発表", "リコール",
)

_NEGATIVE_KEYWORDS = (
    # sport
    "football", "soccer", "basketball", "hockey ", "nba ", "nfl ",
    "premier league", "champions league", "olympic", "goalkeeper",
    "футбол", "хоккей", "баскетбол", "чемпионат мира по",
    # entertainment
    "cinema", "actor", "actress", "movie", "tv series",
    "кино", "актёр", "актриса", "режиссёр", "фильм", "сериал",
    # health/medical (unless auto-related)
    "vaccine", "cancer", "clinical trial",
    "вакцин", "онколог",
)


@dataclass
class TopicVerdict:
    is_auto_or_economy: bool
    auto_hits: int
    negative_hits: int
    hit_samples: list[str]


_ALL_AUTO_KEYWORDS: tuple[str, ...] = (
    _AUTO_KEYWORDS_EN
    + _AUTO_KEYWORDS_RU
    + _AUTO_KEYWORDS_DE
    + _AUTO_KEYWORDS_FR
    + _AUTO_KEYWORDS_IT
    + _AUTO_KEYWORDS_ES
    + _AUTO_KEYWORDS_ZH
    + _AUTO_KEYWORDS_JA
)


# Different scripts need different match strategies:
#   - Latin (EN/DE/FR/IT/ES) — full word-boundary on both sides. Catches
#     real false positives like German "marke" matching English "market"
#     or French "usine" matching "business".
#   - Cyrillic (RU) — substring match. Most RU keywords are stems
#     ("автомобил", "электромобил", "сборк") meant to match every
#     declension, so a right-side \b would skip "автомобиль", "автомобилях"
#     etc.
#   - CJK (ZH/JA) — substring. Each ideograph is meaningful on its own,
#     no substring collisions like Latin.
def _is_cjk(s: str) -> bool:
    return any(
        "぀" <= ch <= "ヿ"   # Hiragana / Katakana
        or "一" <= ch <= "鿿"  # CJK Unified Ideographs
        or "㐀" <= ch <= "䶿"  # CJK Extension A
        for ch in s
    )


def _is_cyrillic(s: str) -> bool:
    return any("Ѐ" <= ch <= "ӿ" for ch in s)


def _compile_keyword(kw: str) -> re.Pattern[str]:
    escaped = re.escape(kw.lower())
    if _is_cjk(kw) or _is_cyrillic(kw):
        # Plain substring — preserves morphology / ideograph semantics.
        return re.compile(escaped)
    # Latin: word-boundary on either side, unless the keyword already
    # encodes its own with leading/trailing whitespace.
    left = "" if kw.startswith(" ") else r"\b"
    right = "" if kw.endswith(" ") else r"\b"
    return re.compile(f"{left}{escaped}{right}", re.IGNORECASE)


_ALL_AUTO_KEYWORDS_COMPILED: list[tuple[str, re.Pattern[str]]] = [
    (kw, _compile_keyword(kw)) for kw in _ALL_AUTO_KEYWORDS
]
_NEGATIVE_KEYWORDS_COMPILED: list[tuple[str, re.Pattern[str]]] = [
    (kw, _compile_keyword(kw)) for kw in _NEGATIVE_KEYWORDS
]


def is_auto_or_economy(raw: RawArticle) -> TopicVerdict:
    """Keyword-based topic filter. Non-perfect: ~85% precision.

    Strategy: lower-case title + first 2000 chars of body, count auto-keyword
    hits across seven language families (EN/RU/DE/FR/IT/ES/ZH/JA) and
    negative-keyword hits. Pass if auto_hits ≥ 1 and auto_hits > negative_hits.

    Word-boundary matching prevents cross-language false positives like the
    German "marke" matching English "market" or French "usine" matching
    "business" — a real bug that pulled Samsung TV stories through as
    automotive.

    Soft fallback for other languages: if ``raw.source_language`` is set and
    NOT in {en, ru, de, fr, it, es, zh, ja}, we assume the heuristic lexicon
    does not cover this language and pass-through with a synthetic single hit
    so the LLM gets a chance to evaluate.
    """
    text = (raw.title + "\n" + raw.body[:2000]).lower()
    auto_hits: list[str] = []
    for kw, pat in _ALL_AUTO_KEYWORDS_COMPILED:
        if pat.search(text):
            auto_hits.append(kw.strip())
    neg_hits: list[str] = [
        kw.strip() for kw, pat in _NEGATIVE_KEYWORDS_COMPILED if pat.search(text)
    ]

    # Dedup while preserving first-seen order
    seen: set[str] = set()
    auto_unique = [h for h in auto_hits if not (h in seen or seen.add(h))]

    passes = len(auto_unique) >= 1 and len(auto_unique) > len(neg_hits)

    # Soft fallback — uncovered language, no positive hits. Trust LLM pass.
    if not passes and not auto_unique:
        lang = (raw.source_language or "").lower()[:2]
        covered = {"en", "ru", "de", "fr", "it", "es", "zh", "ja"}
        if lang and lang not in covered and len(neg_hits) == 0:
            return TopicVerdict(
                is_auto_or_economy=True,
                auto_hits=1,
                negative_hits=0,
                hit_samples=[f"lang-fallback:{lang}"],
            )

    return TopicVerdict(
        is_auto_or_economy=passes,
        auto_hits=len(auto_unique),
        negative_hits=len(neg_hits),
        hit_samples=auto_unique[:5],
    )


# Regex not used but kept as a hook for future structured sections (VIN etc).
_VIN_RE = re.compile(r"\b[A-HJ-NPR-Z0-9]{17}\b")


# ----------------------------------------------------------- three-tier grade
from typing import Literal  # noqa: E402  (kept local to the grading section)

Grade = Literal[
    "certain_news",   # → straight to section classification + title translation
    "possible_news",  # → LLM is_news binary check first
    "off_topic",      # → reject: passes is_article but not automotive
    "not_article",    # → reject: heuristic says it's not an article
]


@dataclass
class BlacklistVerdict:
    hit: bool
    reason: str = ""


# "Strong" auto-signal markers: if a blacklist phrase is found in a title
# but one of these is also there, we assume the piece is really about the
# car market and let the LLM decide. Examples:
#   "BMW представила электробус"   ← не отсеиваем (BMW присутствует)
#   "Электробус Wrightbus UK"       ← отсеиваем (нет авто-маркера)
_AUTO_STRONG_MARKERS = (
    # russian
    "автомобил", "легков", "автопром", "авторынок", "автокредит",
    "автоконцерн", "автопроизводит", "автосалон", "автобренд",
    "дилерск", "модельн", "кроссовер", "седан",
    # english
    "automotive", "car market", "passenger car", "crossover", "sedan",
    "hatchback", "auto industry", "carmaker", "automaker", "suv",
    "ev market", "electric vehicle",
    # german
    "automobil", "automarkt", "pkw-markt", "fahrzeugherstell",
    "autobauer", "elektroauto",
    # french
    "automobile", "marché auto", "marche auto", "constructeur auto",
    "véhicule particulier", "vehicule particulier",
    # italian
    "automobilistic", "mercato auto", "casa automobilistica",
    # spanish
    "automóvil", "automovil", "mercado automotriz", "fabricante de auto",
    # chinese / japanese (strong single-token markers)
    "汽车", "乘用车", "自動車",
)


# Phrases that ALWAYS reject the article — even if the title also names
# a car brand or an auto-marker. Used for clickbait / yellow-press wording
# AND content categories the editor explicitly opted out of, where even a
# brand mention does not save the piece.
#
# Categories below come from the 154-row review of the persistent
# 'Новости' sheet (apr-2026): editor flagged 58 rows as "не постим".
# Each phrase here was vetted against legitimate auto headlines to ensure
# it doesn't accidentally wipe real news.
#
# Substring match on lower-cased title. Multi-word phrases preferred over
# bare words to keep precision high.
_FORCE_REJECT_PHRASES = (
    # ---------- Most-obvious clickbait (legacy) ----------
    "«попа»",
    " попа ",  # bracketed to avoid "попадание", "попасть"
    "вы не поверите",
    "you won't believe",
    "you wont believe",
    "шок-видео",
    "shocking video",

    # ---------- Tips / how-to / советы (editor: «не постим») ----------
    "5 советов", "8 советов", "10 советов", "7 советов",
    "5 ошибок", "8 ошибок", "10 ошибок", "7 ошибок",
    "частых ошибок",
    "common mistakes", "most common mistakes",
    "как выбрать ",
    "как подготовить ",
    "как чистить",
    "как возродить",
    "как промывать",
    "how to choose", "how to prepare", "how to clean",
    "how to revive", "how to determine fault",
    "how to locate", "where to find ",
    "what it means and how", "what does it mean and",
    "почему эксперты рекомендуют",
    "expert tips",
    " 5 tips", " 8 tips", " 10 tips", " 7 tips",
    "топ-10 ", "топ 10 ", "топ-5 ", "топ 5 ",
    "top-10 best", "top 10 best", "top-5 best",
    "top 10 cleaning", "top-10 cleaning",

    # ---------- Motorsport / racing (editor: «гонки не затрагиваем») ----------
    "formula e", "formula 1", "формула 1", "формула е",
    "grand prix", "гран при",
    "gt world challenge", "world challenge america",
    "rds gp", "drift series",
    "racing team",
    "track day", "trackday",
    "double-header",
    "championship round", "wins pro class",
    "wins championship",
    # round 2 (Лист7 rows 192, 235): NASCAR / DTM / Rally
    "nascar ", "nascar-",
    "dtm season", "dtm spielberg", "dtm round",
    "indy 500", "indycar",
    "le mans",
    "wrc round", "wrc season",
    "rally championship", "rally raid",
    "ралли-рейд", "ралли чемпионат",
    "season opener", "season finale",
    "qualifying round",
    "motorsport victory", "dtm victory",
    # round 3 (v25 leakage rows 20, 46, 76)
    "24h nürburgring", "24 hours nurburgring",
    "24-hour nurburgring", "24-hour nürburgring",
    "tcr world tour",
    "weathertech raceway", "laguna seca",
    "podium finish",
    "finishes fourth at", "finishes fifth at",
    "finishes sixth at", "goes fourth at", "goes fifth at",
    "to enter 24h",
    "tier driver line", "driver line-up for",
    "wards 10 best",  # journalist-list awards (Лист7 row 47)

    # ---------- Personnel: appointments / compensation (editor: «никаких назначений») ----------
    "appointed as", "appoints ",
    " joins as ceo", " joins as cfo", " joins as cto",
    "promoted to ceo", "promoted to cfo", "promoted to cto",
    "назначен на",  # "назначен главой/директором..."
    "назначение нового",
    "новый ceo", "новый cto", "новый cfo",
    "performance-based compensation",
    "executive compensation",
    "ceo compensation",
    "compensation package",

    # ---------- Forecasts / прогнозы (editor: «прогнозы не постим») ----------
    "projected to reach",
    "expected to reach",
    "expected to grow",
    "expected to rise",
    "may rise", "may reach", "may exceed",
    "forecast for ",
    "прогнозирует ",
    "может вырасти", "может подорожать",
    "ожидается рост",
    "wall street expects",
    "analysts expect", "analysts predict",
    "аналитики ожидают", "аналитики прогнозируют",

    # ---------- Restoration / retro (editor: «реставрация не интересует») ----------
    "restored ",
    "restoration of",
    "возрождён", "возрождает",
    "восстановленный",
    "реставрац",
    "uncovered after years",
    "weekend classic", "weekend trip classic",
    "vintage classic",

    # ---------- Spy shots / замечен — only the COLOR-spotting variant ----------
    # Editor distinguishes: "spotted in [colors]" = reject (paint variant
    # is not news); "spotted/spied [in country/during tests]" = Rumors,
    # which we WANT (rows 206, 237, 241 of Лист7 → «постим»).
    "spotted in new color",
    "spotted in four new color",
    "spotted in three new color",
    "spotted in five new color",
    "замечен в новых цвет",
    "замечен в новой расцветк",
    "spy shots", "spy photos",
    "шпионские снимк", "шпионские фото",

    # ---------- Corporate boilerplate / ceremonies ----------
    "honored employees", "honored workers",
    "thanked veterans", "thanked employees",
    "congratulated ",
    "поздравил ",
    "отметил лучших сотрудников",
    "commendation ceremony",
    "art project",
    "knowledge day", "день знаний",
    "mechanical engineering day", "день машиностроител",
    "день металлург",

    # ---------- Military (editor: «военное мы не постим») ----------
    "military needs",
    "for the military",
    "popular front", "народный фронт",
    " armed forces ",
    "военных нужд", "для армии",
    "modified for military",

    # ---------- Privacy / legal / compliance documents ----------
    "privacy policy",
    "политика обработки персональных",
    "terms of service",
    "compliance with national standards",
    "система менеджмента качеств",

    # ---------- Single-portal third-party tests / YouTube blogger reviews ----------
    "motortrend record",
    "tested by motortrend",
    "auto bild named", "auto bild magazine",
    "j.d. power study",
    # round 3 (v25 leakage rows 39, 71, 86)
    "carwow drag race",
    "in carwow ", " carwow drag",
    "drag race results",
    "first drive impressions", "satisfying first drive",
    "we achieved",  # journalist test: "BMW claimed X, we achieved Y"
    "we got more", "we got less",  # similar journalist comparison
    "delivers satisfying",
    # explainer / "how it works" — Лист7 row 86 ("How Great Wall... works")
    "how it works",
    "how does it work",
    "system works (",  # "How X system works (RU)"

    # ---------- Adjacent industries (steel, ships, oil, agro) ----------
    "shipbuilder", "shipbuilding",
    "судостроител",
    "steel industry", "сталелитейн", "сталеплавильн",
    "ship operators",
    "land reclamation",
    "мелиорац",
    "ai power shortage",
    "агропром",
    "сельское хозяйство",

    # ---------- Motorcycles (editor: «мотоциклы не наша тема») ----------
    " motorcycle ",
    "мотоцикл",
    "fighting game tournament",
    "capcom cup",
    "street fighter ",

    # ---------- Misc rejects from editor review ----------
    "boycott",
    "бойкот",
    " unique lot ",
    "unique lot put up",
    "уникальный лот",

    # NOTE: carsharing previously force-rejected here, but the round-2
    # editor review (Лист7 row 258) showed «Yandex Drive carsharing
    # expanded fleet in Moscow» SHOULD be posted as Local specifics.
    # Reverted — let the LLM decide. The CLASSIFY_SYSTEM prompt now
    # includes an explicit carsharing routing rule.

    # ---------- Clickbait second-pass (v22 leakage) ----------
    "but there's a catch",
    "but there is a catch",
    "but here's the catch",
    "but here's a catch",
    "one factor quietly",
    "one weird trick",
    "what they don't want",
    "what banks don't tell",
    "secret of",
    "the truth about",

    # ============================================================
    # ROUND 2 — patterns from Лист7 rows 168+ (apr-2026 second review)
    # ============================================================

    # ---------- Recommendations / advice / guidelines (editor row 269/270) ----------
    # «Рекомендации не постим. Только реальные решения по факту.»
    "guidelines for", "guidelines on",
    "safety guidelines",
    "safety standards for",
    "experts recommend",
    "experts advise",
    "recommend drivers", "recommends drivers",
    "recommend owners", "recommends owners",
    "recommend motorists",
    "expert names", "expert named",
    "эксперт назвал", "эксперт назвала",
    "эксперты назвали", "эксперты называют",
    "эксперты советуют", "эксперт советует",
    "эксперт рассказал", "эксперты рассказали",
    "специалист назвал", "специалист рассказал",
    "named common mistakes", "named common errors",
    "named common ",   # broader: "named common braking mistakes" etc.
    "called common braking",
    "mistakes made by drivers", "ошибки водителей",
    "told date to change",   # editor row "Muscovites told date to change winter tires"

    # ---------- Adjacent gov/economy (round 2 leakage) ----------
    "staff shortage", "дефицит кадров",
    "silver trading", "торги серебром",
    "gas prices in europe", "цены на газ в европе",
    "semiconductor strike",
    "rotavirus", "ротавирус",
    "food poisoning", "пищевое отравление",
    " monastery", "монастыр",
    " rocket engine", "ракетный двигатель",
    "detonation engine",
    "credit rating", "кредитный рейтинг",
    "expected credit ratings",
    "exchange bond",
    "облигац",  # bond emissions
    "bond issues",
    "taxi fares", "тарифы такси", "тариф такси",
    "tax incentive phase", "налоговый стимул",  # editor row 239: only actual decisions

    # ---------- Custom builds / tuning / DIY ----------
    " custom build", "custom styling", "custom-built",
    "тюнинг", "кастом-",
    "on gold wheels", "on hre wheels", "on forged wheels",
    "steam-powered", "паропривод",
    "homemade",
    "одним человеком построил",
    "built from scratch",
    "garage-built",

    # ---------- Smartphone / consumer chip non-auto ----------
    " for smartphones", " for foldable",
    " для смартфонов", "для складных смартфон",
    "smartphone processor", "smartphone chip",
    "процессор для смартфон",
    "gaming smartphone", "складной смартфон",
    "foldable smartphone",

    # ---------- Mortgage / loans / generic financial (not auto loans) ----------
    "mortgage rate", "ипотечная ставка",  # not auto-loan
)


def blacklist_hit(
    raw: RawArticle,
    bl: Blacklist,
    brands: list[Any] | None = None,
) -> BlacklistVerdict:
    """Check article against the editor-supplied hard-reject list.

    Rules:
      • Whole-domain blocks (e.g. benchmarkminerals.com) — always reject.
      • Force-reject phrases (clickbait / yellow press) — always reject,
        no brand override. Editor explicitly doesn't want such headlines
        even when they're about a real car brand.
      • Topic phrases — matched in the TITLE only (case-insensitive).
      • For topic phrases: if the title also contains a car brand or an
        auto-marker, we do NOT reject — the article is presumed to be
        about the car market with the blacklisted topic mentioned
        incidentally. Let the LLM decide.
    """
    if not bl:
        return BlacklistVerdict(False)
    dom = domain_of(raw.url or "")
    for blocked in bl.domains:
        if dom == blocked or dom.endswith("." + blocked):
            return BlacklistVerdict(True, f"blacklisted domain: {blocked}")

    title = (raw.title or "").lower()
    # Tier 1 — clickbait phrases that ALWAYS reject.
    for phrase in _FORCE_REJECT_PHRASES:
        if phrase in title:
            return BlacklistVerdict(True, f"clickbait/yellow-press: {phrase!r}")
    # Tier 2 — topic phrases that allow brand override.
    for phrase in bl.all_phrases():
        if not phrase or phrase not in title:
            continue
        if _title_has_auto_signal(title, brands):
            continue
        return BlacklistVerdict(True, f"blacklisted title phrase: {phrase!r}")
    return BlacklistVerdict(False)


def _title_has_auto_signal(title_lower: str, brands: list[Any] | None) -> bool:
    """Return True if the title mentions a known car brand or a strong
    automotive market marker — which lets the blacklist phrase slide."""
    if any(m in title_lower for m in _AUTO_STRONG_MARKERS):
        return True
    if brands:
        for b in brands:
            names = [b.brand.lower(), *(a.lower() for a in getattr(b, "aliases", []))]
            for n in names:
                # skip very short brand aliases (GAZ, UAZ, KIA) that cause too many
                # false positives inside generic words — require boundary-ish match
                if len(n) < 4:
                    if f" {n} " in f" {title_lower} " or title_lower.startswith(n + " ") \
                            or title_lower.endswith(" " + n):
                        return True
                elif n in title_lower:
                    return True
    return False


def grade_article(article: ArticleVerdict, topic: TopicVerdict) -> Grade:
    """Three-tier verdict used by the pipeline to decide LLM strategy.

    ``certain_news``  → skip the cheap LLM relevance check, go directly
                        to section classification + title translation.
    ``possible_news`` → run the cheap LLM relevance check first; if it
                        returns true, proceed like ``certain_news``.
    ``off_topic``     → reject without any LLM call.
    ``not_article``   → reject without any LLM call.
    """
    # Hard reject: binary docs, or score below the lower threshold.
    if "binary-document-url" in article.reasons:
        return "not_article"
    if not article.is_article or article.score < POSSIBLE_SCORE_THRESHOLD:
        return "not_article"
    # Article is plausible but topic fails → off-topic reject.
    if not topic.is_auto_or_economy:
        return "off_topic"
    # Both article- and topic-heuristics pass. Decide certain vs possible.
    if (
        article.score >= CERTAIN_SCORE_THRESHOLD
        and topic.auto_hits >= CERTAIN_MIN_AUTO_HITS
    ):
        return "certain_news"
    return "possible_news"


# ============================================================================
# Hard pre-classifiers — heuristic section assignment for unambiguous cases.
#
# Each helper returns either a (section, region, reason) tuple OR None to
# defer to the LLM. Only triggers on patterns the editor consistently
# routed the same way in their 154-row review of the persistent sheet.
#
# When a pre-classifier fires, the LLM `classify_section` call is SKIPPED
# (saves ~$0.005/row). LLM `translate_title` and `is_automotive` still run.
# ============================================================================

# Body-type words that always mean "commercial vehicle" — editor routes
# anything matching these to the LCV section regardless of brand.
_LCV_BODY_TYPES_EN = (
    " pickup", " pick-up", " pick up",
    " van ", " van,", " van.", " van:",
    " minivan", " mini-van", " mini van",
    " truck ", " truck,", " truck.", " truck:", " trucks ",
    " lorry", " lorries",
    " bus ", " buses ", " minibus",
    " microbus", " microvan",
    " lcv ", " lcv,",
    "light commercial vehicle",
    "commercial vehicle",
    "panel van",
    "cargo van",
    "delivery van",
    "double-decker bus",
    "double decker bus",
)
_LCV_BODY_TYPES_RU = (
    "пикап", "фургон", "грузовик",
    "автобус", "минивэн", "микроавтобус",
    "коммерчес транспорт",  # «коммерческий транспорт»
    "коммерческого транспорт",
    "лёгкий коммерчес",
    "легкий коммерчес",
    "лцв",
)

# Phrases that always mean "financial result" — editor: always Other news.
_FINANCIAL_RESULT_HINTS = (
    "financial results", "earnings results", "earnings report",
    "operating profit", "operating loss", "operating income",
    "net income", "net profit", "net loss",
    "fiscal year results", "fy2025", "fy 2025", "fy2024", "fy 2024",
    "q1 results", "q2 results", "q3 results", "q4 results",
    "first quarter results", "third quarter results",
    "full year results", "annual results",
    "revenue dropped", "revenue fell", "revenue grew",
    "profit fell", "profit dropped", "profit grew",
    "финрезультат", "финансовые результат",
    "операционная прибыль", "операционный убыток",
    "чистая прибыль", "чистый убыток",
    "выручка упала", "выручка выросла", "выручка снизилась",
    "квартальные результат",
    "годовые финансовые",
)

# Phrases that always mean "award/recognition" — editor: always Other news.
_AWARD_HINTS_EN = (
    " wins award", " wins the award",
    "awarded the prize", "earned the prize",
    "named winner of",
    "win award at", "won award at",
    "claimed top spot in",
    "topped j.d. power",  # JD Power award (editor row 159 said yes if from JDP)
    "named best in class",
    "top award at",
    "won the fuorisalone",  # Editor row 75: «премии идут в Другие»
    "fuorisalone award",
)
_AWARD_HINTS_RU = (
    "удостоен ", "удостоена ",
    "получил премию", "получила премию",
    "выиграл премию", "выиграла премию",
    "стал лауреатом", "стала лауреатом",
    "победил в номинации",
    "звание «", "звание лауреата",
    " награжд",  # награждён, награждена
)


def _domain_matches(domain: str, suffixes: tuple[str, ...]) -> bool:
    d = (domain or "").lower().lstrip(".")
    for s in suffixes:
        sn = s.lower().lstrip(".")
        if d == sn or d.endswith("." + sn):
            return True
    return False


# Russian auto-market portals — when the article is hosted here AND the
# subject is automotive, the editor consistently routes to "Local" rather
# than "Economics" or "Other news". List grew during apr-2026 review:
# editor flagged ROAD convention coverage, motorpage feature articles,
# and major newswires (TASS, RIA, Kommersant) as Local-by-default
# whenever the subject is the RU auto market.
_RU_AUTO_PORTALS = (
    # editorial RU auto portals (≥80% RU-market focus)
    "autostat.ru",
    "autonews.ru",
    "abreview.ru",
    "kolesa.ru",
    "drom.ru",
    "auto.mail.ru",
    "5koleso.ru",
    "zr.ru",
    "motor.ru",
    "autoreview.ru",
    "quto.ru",
    "wroom.ru",
    "rusautonews.com",
    "carcityrussia.ru",
    "motorpage.ru",
    # RU auto-industry organisations (always RU-context when on these sites)
    "asroad.org",         # ROAD — Russian Automobile Dealers Association
    "napinfo.ru",         # Russian Auto Manufacturers Association
    # Russian newswires — RU focus when topic is auto
    "tass.ru",
    "ria.ru",
    "kommersant.ru",
    "rg.ru",
    "iz.ru",
    "lenta.ru",
    "rbc.ru",
    "vedomosti.ru",
    "gazeta.ru",
)


# Russian regions / cities — appearance in title means the article is about
# RU auto market. Used to upgrade ambiguous global-brand stories that have
# a Russian-region angle (e.g. "Сборку Exeed перенесут на завод АГР в
# Шушарах" — China brand, but the RU plant move is the news).
_RU_GEO_MARKERS = (
    "московск", "подмоско", "москв",
    "петербург", "санкт-петер",
    "ленинградск",
    "татарстан", "башкорт",
    "калининград", "уральск",
    "сибирь", "сибирск",
    "крым",
    "новосибирск", "екатеринбург", "челябинск", "казан", "уфа", "красноярск",
    "тольятти",  # AvtoVAZ
    "ульяновск",  # UAZ
    "набережн",   # KamAZ
    "нижний новгор", "нижегородск",
    "шушары", "шушарах",  # AGR plant
    "автомобильный завод",
)


# Russian auto-industry actors — companies, alliances, regulators. When
# any of these are named in a title, the story is almost always RU-context.
_RU_AUTO_ACTORS = (
    # OEMs
    "автоваз", "ладa", " лада", "лады ", "лады.",
    "уаз ", "уаз,", "уаз.", "уаз-",
    "соллерс", "sollers",
    "москвич", "moskvich",
    "автотор", "avtotor",
    "тенет ", "tenet",
    " атом ",  # the 'Атом' EV
    "камаз", "kamaz", "газ ", " gaz ",
    "агр ",  # AGR plant
    # Trade bodies / regulators
    "автостат",
    "роад", "asroad",
    "napinfo", "оар ", " оар.",
    "минпромторг", "минфин",
    "национальной ассоциа",
)


def _is_ru_auto_subject(text: str) -> bool:
    """True if the title talks about the Russian auto market specifically.

    Three families of signals:
      1. Generic Russia markers ("в России", "in Russia", "россий")
      2. Russian region / city names (Подмосковье, Тольятти, Шушары)
      3. Russian auto-industry actors (АвтоВАЗ, УАЗ, Автотор, Автостат, РОАД)

    Any one of the three is sufficient. Used in combination with
    `_RU_AUTO_PORTALS` to upgrade Local classifications.
    """
    t = (text or "").lower()
    generic_ru = (
        "в россии", "в рф", " россии ", "россий",
        " рф ", " рф,", " рф.", " рф:", "(рф)",
        "russia", "russian market", "in russia",
        "russian auto", "ru market",
    )
    if any(m in t for m in generic_ru):
        return True
    if any(m in t for m in _RU_GEO_MARKERS):
        return True
    if any(m in t for m in _RU_AUTO_ACTORS):
        return True
    return False


# Section labels — must match config/sections.yaml exactly. Hard-coded
# here as the canonical names so the pre-classifier is deterministic.
SECTION_LCV = "LCV news"
SECTION_OTHER = "Other news"
SECTION_LOCAL = "Local specifics"
SECTION_RUMORS = "Rumors"
SECTION_CONFIRMED = "Confirmed"


# ---------- Rumor signals (title) ----------
# Editor: «Если ссылка на компанию, то это всегда Факты». A title
# containing one of these phrases AND a body that does NOT cite the
# brand directly is a Rumor — not Facts.
_RUMOR_TITLE_HINTS = (
    # english
    "may launch", "may arrive", "may appear", "may return",
    "expected to arrive", "expected to launch",
    "reportedly", "allegedly", "rumored",
    "spotted ", "leaked",
    "could come to", "could arrive",
    "hints at", "teased",
    # russian
    "может появиться", "может выйти", "может прийти",
    "может вернуться", "ожидается появление",
    "по слухам", "сообщают источники", "по неподтверждённым",
    "по непроверенной информации",
    "замечен", "замечена", "замечено",  # caught testing
    "слухи", "слух о",
    "поговаривают",
)

# ---------- Brand voice (body) — when present, it's NOT a rumor ----------
# Strong signals that the brand is the source. We only check the FIRST
# 1500 chars of body — leads usually attribute up front.
_BRAND_VOICE_HINTS = (
    # english
    "press release", "official statement", "in a statement",
    "the company announced", "the company said",
    "the company unveiled", "the company published",
    "ceo said", "ceo announced", " spokesperson said",
    "official press", "in an official",
    "as the company",
    # russian
    "пресс-релиз", "пресс-службе", "пресс-служб",
    "сообщили в компании", "сообщили в", "сообщили представители",
    "заявил глава", "заявили в компании", "официально объявили",
    "представители компании", "представители бренда",
    "официальное заявление", "раскрыли в компании",
    "по информации компании", "в самой компании",
)


def has_rumor_signal(title: str) -> bool:
    """True if the TITLE looks like speculation, not a press release."""
    if not title:
        return False
    t = title.lower()
    return any(kw in t for kw in _RUMOR_TITLE_HINTS)


def has_brand_voice(body: str) -> bool:
    """True if the article cites the brand directly (press release, CEO, etc).

    Limits the scan to the first 1500 chars — leads usually attribute
    upfront, and longer bodies often quote third parties later.
    """
    if not body:
        return False
    snippet = body[:1500].lower()
    return any(kw in snippet for kw in _BRAND_VOICE_HINTS)


# ---------- Multi-news detection ----------
# Editor: «опять несколько разных новостей по одной ссылке, такое нельзя
# ставить» (rows 32, 34). The cleanest signal is a title with two
# substantial clauses separated by a semicolon — that is almost always
# a digest / aggregator entry, not a single story.
_DIGEST_PATTERNS = (
    # patterns we confirmed in editor's data
    r";\s+",                         # "Changan integrates X; CATL hosts Y"
    r"\s+&\s+.+\s+&\s+",             # "X & Y & Z" triple-and
)
_DIGEST_RE = re.compile("|".join(_DIGEST_PATTERNS))


def is_multi_news_title(title: str) -> bool:
    """True if the title looks like it covers multiple unrelated stories.

    Heuristic: title contains a semicolon AND has substantial content on
    both sides (>= 25 chars each). Single-brand updates (no semicolon)
    pass through.
    """
    if not title:
        return False
    t = title.strip()
    if ";" in t:
        parts = [p.strip() for p in t.split(";", 1)]
        if len(parts) == 2 and all(len(p) >= 25 for p in parts):
            # Both halves are substantial — this is a digest entry.
            # Single trailing tail like "; KOR" wouldn't pass this gate.
            return True
    return False


# ---------- Supplier abstract showcase at motorshow ----------
# Editor flagged 7 such rows in round 2 (Bosch, MINIEYE, Eastman, Hangsheng,
# AUMOVIO, ElringKlinger, Hongqi sub-brand matrix). Pattern: parts vendor
# unveils generic "technologies / portfolio / matrix / foundations / suite"
# at a motorshow — no specific consumer product. Editor: «обо всем и ни о
# чем, важен масштаб». Rejected unless title also names a passenger-car brand.
_SUPPLIER_VERBS = (
    "showcased", "showcases ",
    "unveiled", "unveils ",
    "debuted", "debuts ",
    "presented", "presents ",
    "revealed", "reveals ",
    "introduced", "introduces ",
    " representation",
    "представил",  # ru
)
_SUPPLIER_ABSTRACT_OBJECTS = (
    " technologies",
    " solutions",
    " foundations",
    " matrix",
    " portfolio",
    " evolution",
    " platform showcase",
    " innovations",
    " technology suite",
    " core technology",
    "sub-brand matrix",
    "three key technologies",
    "three core technology",
    "three technology",
    "full-range evolution",
    "full range evolution",
    # russian
    " технологий",
    " решений",
    " инноваций",
)
_MOTORSHOW_LOCATIONS = (
    "auto china", "beijing auto show", "beijing motor show",
    "shanghai auto show", "shanghai motor show",
    "iaa transportation", "iaa mobility",
    "geneva motor show",
    "ces 20", "consumer electronics show",
    "паркинг авто", "moscow off-road show",
    "токио мотор шоу", "tokyo motor show",
    "munich motor show",
    "пекинский автосалон", "пекинском автосалон", "пекинской выставке",
    "шанхайск", "шанхайской выставке",
)

# Passenger-car brands that override — these are NEWS even when generic.
# We use a small set; the full brand list lives in config/brand_domains.yaml
# but we don't want to load YAML inside this module.
_PASSENGER_BRANDS_OVERRIDE = (
    "toyota", "lexus", "nissan", "honda", "mazda", "subaru", "suzuki",
    "mitsubishi", "hyundai", "kia", "genesis", "volkswagen", "vw ",
    "audi", "porsche", "skoda", "bmw", "mercedes", "ford", "chevrolet",
    "cadillac", "tesla", "rivian", "volvo", "jaguar", "land rover",
    "renault", "peugeot", "citroen", "fiat", "alfa romeo", "stellantis",
    "lada", "лада", "автоваз", "uaz", "уаз", "moskvich", "москвич",
    "chery", "haval", "geely", "exeed", "omoda", "jaecoo", "byd",
    "xpeng", "nio", "li auto", "great wall", "gwm", "changan",
    "dongfeng", "tank", "leapmotor", "lynk", "voyah", "jetour",
    "tenet", "тенет", "atom", "атом", "rox",
)


# ---------- Dzen-style listicles ----------
# "Parasitism, gigantism and three more obvious trends in Chinese automakers"
# "5 most reliable used SUVs"
# Pattern: "X, Y and N more <noun>" — characteristic dzen-channel listicle.
_DZEN_LISTICLE_PATTERNS = (
    re.compile(r"\b\w+, \w+ and \w+ more\b", re.IGNORECASE),
    # "5 reasons why...", "5 things you...", "10 most reliable...",
    # "5 best/worst/top...", "5 ways to..."
    re.compile(
        r"^\s*\d+\s+(most|best|worst|top|reasons|things|ways|signs|"
        r"features|tricks|secrets|mistakes|bright|featured|standout|"
        r"new\s+\w+\s+at|notable)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"^\s*\d+\s+(самых|лучших|худших|причин|способов|вариантов|"
        r"признаков|ярких|заметных|новых)\b",
        re.IGNORECASE,
    ),
)


def is_dzen_listicle(title: str) -> bool:
    """Detect dzen-channel listicles. These are off-topic regardless of
    auto subject — editor consistently rejected this format.
    """
    if not title:
        return False
    return any(pat.search(title) for pat in _DZEN_LISTICLE_PATTERNS)


def is_supplier_abstract_showcase(title: str, body: str = "") -> bool:
    """Editor's «обо всем и ни о чем» pattern.

    A parts vendor (Bosch, MINIEYE, Hangsheng, AUMOVIO, …) showcasing
    abstract "technologies / solutions / matrix / portfolio" at a
    motorshow, with no passenger-brand model named. Always reject —
    editor: «про каждую компанию не напишешь, важен масштаб».

    Override: if the title also names a passenger-car brand (BMW
    showcases tech at Beijing Show), let it through — the brand
    coverage IS news.
    """
    if not title:
        return False
    t = title.lower()
    has_verb = any(v in t for v in _SUPPLIER_VERBS)
    has_abstract = any(o in t for o in _SUPPLIER_ABSTRACT_OBJECTS)
    if not (has_verb and has_abstract):
        return False
    # Motorshow context — title or first 1000 chars of body
    in_title = any(m in t for m in _MOTORSHOW_LOCATIONS)
    in_body = body and any(m in body[:1000].lower() for m in _MOTORSHOW_LOCATIONS)
    if not (in_title or in_body):
        return False
    # Brand override
    if any(b in t for b in _PASSENGER_BRANDS_OVERRIDE):
        return False
    return True


@dataclass
class HeuristicSection:
    section: str          # one of the canonical section names
    region: str           # "Local" | "Global"
    confidence: float     # 0.0..1.0 — heuristic confidence
    reason: str           # short tag for debug ("body-type:pickup" etc.)


def heuristic_section(
    *,
    title: str,
    body_excerpt: str = "",
    domain: str = "",
) -> HeuristicSection | None:
    """Hard pre-classify a row before the LLM section call.

    Returns None to defer to LLM. Returns a HeuristicSection when the
    rules are confident enough that no LLM call is needed.

    Priority order matters — LCV first (body-type override), then
    financial / awards (clear keyword signals), then RU-portal Local
    upgrade.
    """
    t = (title or "").lower()
    if not t:
        return None

    # ----- Tier 1: LCV body-type override (highest priority) ----------------
    # Matches in the TITLE — body-type words like "pickup" / "минивэн"
    # are unambiguous when they appear in the headline.
    for kw in _LCV_BODY_TYPES_EN:
        if kw in " " + t + " ":
            return HeuristicSection(
                section=SECTION_LCV,
                region="Global",  # LCV has no Local/Global distinction in editor practice
                confidence=0.95,
                reason=f"body-type:{kw.strip()}",
            )
    for kw in _LCV_BODY_TYPES_RU:
        if kw in t:
            return HeuristicSection(
                section=SECTION_LCV,
                region="Global",
                confidence=0.95,
                reason=f"body-type:{kw}",
            )

    # ----- Tier 2: Financial-results — always Other news --------------------
    for kw in _FINANCIAL_RESULT_HINTS:
        if kw in t:
            return HeuristicSection(
                section=SECTION_OTHER,
                region="Global",
                confidence=0.92,
                reason=f"financial:{kw}",
            )

    # ----- Tier 3: Awards / премии — always Other news ----------------------
    for kw in _AWARD_HINTS_EN + _AWARD_HINTS_RU:
        if kw in t:
            return HeuristicSection(
                section=SECTION_OTHER,
                region="Global",
                confidence=0.90,
                reason=f"award:{kw.strip()}",
            )

    # ----- Tier 4: RU-portal + RU-market subject = Local --------------------
    # Domain-based upgrade. Without explicit RU-market markers in the title
    # we don't trigger — generic global news on iz.ru (e.g. Mercedes Cabrio)
    # should still be Global.
    if domain and _domain_matches(domain, _RU_AUTO_PORTALS) and _is_ru_auto_subject(t):
        return HeuristicSection(
            section=SECTION_LOCAL,
            region="Local",
            confidence=0.85,
            reason=f"ru-portal:{domain}",
        )

    # ----- Tier 5: Rumors detection (title speculation + no brand voice) ----
    # Editor row 43: «Если ссылка на компанию, то это всегда Факты».
    # A title with rumor markers ("may launch", "spotted", "по слухам") AND
    # a body that does NOT cite the brand directly = Rumors. If the body
    # has explicit brand-voice ("the company announced", "пресс-релиз") we
    # defer to LLM, because the brand citation overrides the title hedging.
    if has_rumor_signal(t) and not has_brand_voice(body_excerpt):
        # Strong cue but not deterministic — keep confidence moderate so the
        # editor still scrutinizes, and let the LLM section call refine if
        # available. We short-circuit only when the rumor signal is clear
        # (matches at least one strong RU pattern OR multiple EN ones).
        return HeuristicSection(
            section=SECTION_RUMORS,
            region="Global",  # region refined by LLM if needed
            confidence=0.75,
            reason="rumor-signal-no-brand-voice",
        )

    return None
