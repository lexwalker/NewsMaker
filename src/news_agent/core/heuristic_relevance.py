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


# Pre-compile keyword regexes with word-boundary semantics so that German
# "marke" doesn't match English "market" and French "usine" doesn't match
# "business". A keyword that already contains a leading or trailing space
# in its source string keeps that explicit boundary; otherwise we wrap
# with \b on both sides.
def _compile_keyword(kw: str) -> re.Pattern[str]:
    # Keywords that contain non-word characters (apostrophes, hyphens,
    # spaces) need to be matched literally — wrapping in \b is fine for
    # word-character boundaries on the ends.
    escaped = re.escape(kw.lower())
    # If the keyword already starts/ends with whitespace, keep it as-is.
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


def blacklist_hit(
    raw: RawArticle,
    bl: Blacklist,
    brands: list[Any] | None = None,
) -> BlacklistVerdict:
    """Check article against the editor-supplied hard-reject list.

    Rules:
      • Whole-domain blocks (e.g. benchmarkminerals.com) — always reject.
      • Phrases — matched in the TITLE only (case-insensitive).
      • BUT: if a blacklist phrase is found AND the title also contains
        a car brand name (from ``brands``) OR a strong auto-signal marker,
        we do NOT reject — the article is presumed to be about the car
        market as a whole, with buses / tractors / battery minerals
        mentioned incidentally. Let the LLM decide.
    """
    if not bl:
        return BlacklistVerdict(False)
    dom = domain_of(raw.url or "")
    for blocked in bl.domains:
        if dom == blocked or dom.endswith("." + blocked):
            return BlacklistVerdict(True, f"blacklisted domain: {blocked}")

    title = (raw.title or "").lower()
    for phrase in bl.all_phrases():
        if not phrase or phrase not in title:
            continue
        # Blacklist phrase is present — check for override signals.
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
