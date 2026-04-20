"""Claude's verdict pass on ТЕСТ статьи v4.

Writes two new columns:
  R  'Вердикт Claude'       — my opinion on each row
  S  'Обоснование Claude'   — one-line reason

Verdict taxonomy (7 buckets):
  Новость            — fits the editor's "news about cars" profile
  Новость (погран.)  — real news but borderline topic / low editorial value
  Уточнить           — plausible but I'm not sure; worth an LLM check
  Аналитика          — is an article, but analytical / market report, not news
  Не по теме         — real article, but not about automotive/economy
  Не новость         — index, archive, search, product, form, PDF, evergreen
  Дубль              — already seen the same final URL
  Ошибка             — fetch/parse failed
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
load_dotenv(ROOT / ".env", override=True)

SHEET_ID = os.environ["SPREADSHEET_ID"]
SA_PATH = ROOT / os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
TAB = "ТЕСТ статьи v4"

# row_number → (verdict, reason). Rows not in the dict get ("", "").
VERDICTS: dict[int, tuple[str, str]] = {
    # --- Telegram: chinamashina_news
    2:  ("Уточнить", "китайский бренд IM, оценка интереса редактора"),
    3:  ("Новость", "премьера концепт-кара FAW-VW Jetta X"),
    4:  ("Новость", "новая комплектация Omoda C5 для РФ"),
    5:  ("Новость", "Li Auto i6 завоз в Беларусь"),
    6:  ("Дубль", "повтор №3 (FAW-VW Jetta X)"),
    # --- Telegram: sergtselikov
    7:  ("Новость", "статистика продаж Geely/Belgee в РФ"),
    8:  ("Новость", "рост спроса Land Rover +41%"),
    9:  ("Новость", "рыночная статистика Made-in-China"),
    10: ("Новость", "регистрации Москвич серии М"),
    11: ("Новость", "рост рынка PHEV +127%"),
    # --- Telegram: autopotoknews
    12: ("Новость", "автокомпоненты — дистрибуция Кольчуга/НПК Автоприбор"),
    13: ("Новость", "портфель автокредитов в РФ"),
    14: ("Новость", "рынок Турции Q1 — статистика"),
    15: ("Новость", "140k авто в Калужской обл. — пром.статистика (бот ошибочно отсёк)"),
    16: ("Новость", "Omoda C5 4WD (дубль 4 по смыслу, но другой источник)"),
    # --- SPARK-Интерфакс — справочные пояснения
    17: ("Не новость", "help-page SPARK"),
    18: ("Не новость", "help-page SPARK"),
    19: ("Не новость", "help-page SPARK"),
    20: ("Не новость", "help-page SPARK"),
    21: ("Не новость", "help-page SPARK"),
    # --- raexpert
    22: ("Не новость", "index /news/"),
    23: ("Аналитика", "исследование рынка лизинга"),
    # --- RM Sotheby's
    24: ("Не новость", "Sotheby's — about-page"),
    25: ("Не новость", "Estate Planning guide"),
    26: ("Не новость", "evergreen — best auctions"),
    27: ("Не новость", "Careers page"),
    # --- energy.gov — timelines
    28: ("Не новость", "energy.gov timeline (не авто)"),
    29: ("Не новость", "energy.gov timeline"),
    30: ("Не новость", "RTG timeline (космос)"),
    31: ("Не новость", "Weatherization timeline"),
    32: ("Не новость", "Savannah River history"),
    # --- ratings.ru — финансовые рейтинги
    33: ("Не по теме", "рейтинг страховой — не авто"),
    34: ("Не по теме", "финансы — рассрочка"),
    35: ("Не по теме", "облигации МТС"),
    36: ("Не по теме", "зарплаты — не авто"),
    37: ("Не по теме", "ЦФА регулирование"),
    # --- Transport Canada recalls
    38: ("Уточнить", "канадский отзыв авто — короткий повод"),
    39: ("Уточнить", "канадский отзыв — ABS/ESC"),
    40: ("Уточнить", "канадский отзыв — шины"),
    41: ("Уточнить", "канадский отзыв — топливный насос"),
    42: ("Уточнить", "канадский отзыв — лейбл веса"),
    # --- motortrend
    43: ("Новость (погран.)", "обзор Mustang RTR — тяготеет к Test-drive"),
    44: ("Новость", "Ford F-150 EREV — индустриальная новость"),
    45: ("Новость (погран.)", "yearlong Honda Passport — longform-review"),
    46: ("Новость", "Mustang GTD Nürburgring рекорд"),
    47: ("Новость", "Ford restructuring EV strategy"),
    # --- semiconductors.org — 101-series
    48: ("Не новость", "evergreen explainer"),
    49: ("Не новость", "evergreen explainer"),
    50: ("Не новость", "evergreen explainer"),
    51: ("Не новость", "evergreen explainer"),
    52: ("Не новость", "policy page"),
    # --- Cox Automotive insights
    53: ("Аналитика", "Days' supply — аналитика рынка"),
    54: ("Аналитика", "EV Market Monitor — ежемес. аналитика"),
    55: ("Не новость", "подкаст-эпизод"),
    56: ("Новость", "назначение Jeff Jones — релиз HR"),
    57: ("Аналитика", "Weekly summary"),
    # --- Geotab
    58: ("Не новость", "industry landing page"),
    59: ("Не новость", "ROI calculator — инструмент"),
    60: ("Аналитика", "whitepaper-blog"),
    61: ("Аналитика", "whitepaper-blog"),
    62: ("Не новость", "PDF document"),
    # --- TrendForce
    63: ("Не новость", "index страницы news"),
    64: ("Не по теме", "Apple foldable — не авто"),
    65: ("Не по теме", "NVIDIA Rubin — не авто"),
    66: ("Новость", "EV cell prices Q2 forecast"),
    67: ("Не новость", "press center landing"),
    # --- Benchmark camefrom referral — дубли
    68: ("Дубль", "рефферал-лендинг Benchmark"),
    69: ("Дубль", "повтор 68"),
    70: ("Дубль", "повтор 68"),
    71: ("Дубль", "повтор 68"),
    72: ("Дубль", "повтор 68"),
    # --- SMMT
    73: ("Новость", "SMMT UK-EU trade — значимый индустриальный сигнал"),
    74: ("Новость", "Record Q1 zero-emission bus — рыночная статистика"),
    75: ("Не новость", "интервью-серия 'Five minutes with'"),
    76: ("Аналитика", "feature про Van dealerships"),
    77: ("Новость", "Foton выходит на рынок UK"),
    # --- naamsa
    78: ("Аналитика", "op-ed / paradox SA industry"),
    79: ("Новость", "BMW локализует PHEV в ЮАР"),
    80: ("Новость", "Suzuki sales ЮАР"),
    81: ("Новость", "Stellantis награда Accelerator"),
    82: ("Аналитика", "consumer strain report"),
    # --- VDA
    83: ("Не новость", "events index"),
    84: ("Не новость", "facts-and-figures landing"),
    85: ("Аналитика", "Israel monthly review"),
    # --- ancap
    86: ("Не новость", "evergreen 'что делает авто безопасным'"),
    87: ("Не новость", "evergreen 'как тестируют'"),
    88: ("Не новость", "evergreen 'что значат звёзды'"),
    89: ("Дубль", "повтор 87 с якорем"),
    90: ("Не новость", "Terms of Use"),
    91: ("Дубль", "повтор 90"),
    # --- AEB пресс-релизы
    92: ("Новость", "AEB продажи LCV март 2026"),
    93: ("Новость", "AEB продажи август 2013 — старо, но статья"),
    # --- EV.com
    94: ("Новость", "Stellantis+Dongfeng Europe"),
    95: ("Новость", "Cadillac Escalade IQ Middle East"),
    96: ("Новость", "Ford EV Chief уход"),
    # --- autonews.gasgoo
    97: ("Новость", "BASF+Transfar партнёрство"),
    98: ("Новость", "китайский рынок март +72.7% экспорт"),
    99: ("Ошибка", "gasgoo — пустой ответ"),
    100: ("Ошибка", "gasgoo — пустой ответ"),
    101: ("Ошибка", "gasgoo — пустой ответ"),
    # --- ACEA
    102: ("Не новость", "board of directors list"),
    103: ("Новость", "HDV CO2 amendment"),
    104: ("Аналитика", "годовой отчёт рынка ЕС"),
    105: ("Аналитика", "op-ed про zero-emission грузовики"),
    106: ("Новость", "регистрации новых авто февраль ЕС"),
    # --- Euro NCAP — evergreen
    107: ("Не новость", "how-to guide"),
    108: ("Не новость", "how-to guide"),
    109: ("Не новость", "evergreen"),
    110: ("Не новость", "how-to guide"),
    111: ("Не новость", "about-page"),
    # --- Global NCAP — realnews
    112: ("Новость", "NCAP критикует Chery"),
    113: ("Новость", "Toyota Corolla Cross 2-star"),
    114: ("Новость", "Maruti Suzuki mixed results"),
    115: ("Новость", "Hyundai zero-star Africa"),
    116: ("Новость", "Shanghai Declaration"),
    # --- Green NCAP
    117: ("Не новость", "how-to guide"),
    118: ("Аналитика", "legislative vs consumer testing"),
    119: ("Новость", "2024 category winners"),
    120: ("Новость", "2025 category winners"),
    121: ("Новость", "2023 category winners"),
    # --- IIHS — все реальные релизы
    122: ("Новость", "IIHS — driver assistance safety"),
    123: ("Новость", "IIHS 2026 awards"),
    124: ("Новость", "IIHS коммерческий транспорт"),
    125: ("Новость", "IIHS anti-speeding tech"),
    126: ("Новость", "Jeep Wrangler modifications"),
    # --- asroad.org — отличные русские дилерские новости
    127: ("Новость", "Новак совещание по автопрому"),
    128: ("Новость", "Джейлэнд формирование сети"),
    129: ("Новость", "OMODA|JAECOO АВТОДОМ награды"),
    130: ("Новость", "перспективы Belgee X50+"),
    131: ("Новость", "обучение сотрудников дилеров"),
    132: ("Дубль", "повтор 127"),
    133: ("Дубль", "повтор 128"),
    134: ("Дубль", "повтор 129"),
    135: ("Дубль", "повтор 130"),
    136: ("Дубль", "повтор 131"),
    # --- ksonline — общая новостная лента не про авто
    137: ("Не по теме", "вице-мэр в СИЗО"),
    138: ("Не по теме", "взятка ЖД"),
    139: ("Не по теме", "поликлиника"),
    140: ("Не по теме", "арест ЖД начальника"),
    141: ("Не по теме", "региональные кухни"),
    # --- banki.ru
    142: ("Не новость", "all-news index"),
    143: ("Не новость", "all-news index"),
    144: ("Не новость", "news-lenta index"),
    145: ("Не новость", "daytheme index"),
    146: ("Не новость", "events index"),
    # --- РИА новости
    147: ("Не по теме", "школьники Индонезия"),
    148: ("Не по теме", "гепатит С"),
    149: ("Не по теме", "Мурашко — здравоохр."),
    150: ("Не по теме", "футбол"),
    151: ("Не по теме", "самолёт-разведчик"),
    # --- techinsider
    152: ("Не по теме", "пчёлы"),
    153: ("Не по теме", "OnePlus планшет"),
    154: ("Не по теме", "Adata SSD"),
    155: ("Не по теме", "Марс океан"),
    156: ("Не по теме", "Digital Breakfast event"),
    # --- YouTube shorts
    157: ("Не новость", "YouTube short (Минский мотозавод)"),
    158: ("Не новость", "YouTube short Haval Jolion"),
    159: ("Не новость", "YouTube howto колодки"),
    160: ("Не новость", "YouTube howto колодки"),
    161: ("Не новость", "YouTube short Deepal"),
    # --- CNN
    162: ("Не по теме", "Elections 2026 landing"),
    163: ("Не новость", "политик профиль"),
    164: ("Не по теме", "redistricting maps"),
    165: ("Не новость", "Olympics landing"),
    166: ("Не по теме", "Anderson Cooper podcast"),
    # --- Forbes.ru
    167: ("Не по теме", "цифровые платформы"),
    168: ("Не по теме", "благотворители"),
    169: ("Не по теме", "жизнь в родительском доме"),
    170: ("Не по теме", "финансисты <30"),
    171: ("Не по теме", "Forbes Woman Mercury Awards"),
    # --- BusinessKorea
    172: ("Не по теме", "Naver AI encoder"),
    173: ("Не по теме", "Samsung union strike"),
    174: ("Не по теме", "Korean oil tanker Red Sea"),
    175: ("Не по теме", "dollar dividend payments"),
    176: ("Не по теме", "Dom Pérignon event"),
    # --- The Verge
    177: ("Не по теме", "YouTube timestamp share"),
    178: ("Не по теме", "Google+Gucci glasses"),
    179: ("Не по теме", "Ballmer NPR"),
    180: ("Не по теме", "Netflix vertical video"),
    181: ("Не по теме", "Netflix Reed Hastings"),
    # --- TechCrunch
    182: ("Не по теме", "Sequoia $7B fund"),
    183: ("Не по теме", "Factory AI coding"),
    184: ("Не по теме", "Luma AI production"),
    185: ("Не по теме", "Netflix Reed Hastings board"),
    186: ("Не по теме", "Upscale AI raise"),
    # --- Continental
    187: ("Не новость", "career page"),
    188: ("Не новость", "career page"),
    189: ("Не новость", "career subpage"),
    190: ("Не новость", "Capital Market Day 2025"),
    191: ("Не новость", "Capital Market Day 2023"),
    # --- avtonovostidnya
    192: ("Новость", "УМО + Яндекс голос"),
    193: ("Новость", "Mercedes EQS 1000 км"),
    194: ("Новость", "Kia — откуда везут"),
    195: ("Новость", "Xpeng GX электронная тонировка"),
    196: ("Новость", "Geely отзыв кроссоверов"),
    # --- АВТОВАЗ музей и магазин запчастей
    197: ("Новость (погран.)", "музей АВТОВАЗа — на грани новости"),
    198: ("Не новость", "товар — накладки Largus"),
    199: ("Не новость", "товар Largus"),
    200: ("Не новость", "товар Granta"),
    201: ("Не новость", "товар Niva Travel"),
    # --- 3dnews
    202: ("Не по теме", "крипто Grinex"),
    203: ("Не по теме", "OnePlus Китай"),
    204: ("Не по теме", "Байкал-Т1 Linux"),
    205: ("Не по теме", "Metro 2039 игра"),
    206: ("Не по теме", "Street Fighter трейлер"),
    # --- truesharing
    207: ("Новость", "продажи корейских авто +78%"),
    208: ("Не по теме", "зарплаты каршеринг"),
    209: ("Не по теме", "Telegram VPN"),
    210: ("Не по теме", "Делимобиль тариф"),
    211: ("Новость", "Volga 110k авто/год"),
    # --- naavtotrasse
    212: ("Новость", "ГТК Беларуси ввоз авто"),
    213: ("Новость", "Renault Kiger/Triber RGEP"),
    214: ("Новость", "Lada Vesta Sport МКП"),
    215: ("Аналитика", "эксперт о Changan UNI-K"),
    216: ("Не по теме", "Европол хакеры"),
    # --- ixbt
    217: ("Не по теме", "Max мессенджер"),
    218: ("Не по теме", "Amazon Fire TV"),
    219: ("Не по теме", "Telegram блокировки"),
    220: ("Новость", "Nissan Juke EV новое поколение"),
    221: ("Не по теме", "NASA солнечный парус"),
    222: ("Дубль", "повтор 217"),
    223: ("Дубль", "повтор 218"),
    224: ("Дубль", "повтор 219"),
    225: ("Дубль", "повтор 220"),
    226: ("Дубль", "повтор 221"),
    # --- motorpage (http://)
    227: ("Новость", "Audi субренд sedan"),
    228: ("Новость", "Mercedes C-Class тачскрин"),
    229: ("Аналитика", "обзор рынка — минимум"),
    230: ("Новость", "Infiniti перерождение"),
    231: ("Новость", "Госдума выделенные полосы"),
    # --- regnum
    232: ("Не по теме", "налоги магазины"),
    233: ("Не по теме", "21 апреля выходной"),
    234: ("Не по теме", "масоны-убийцы"),
    235: ("Не по теме", "ЦРУ Куба"),
    236: ("Не по теме", "Иран военная мысль"),
    # --- thedrive
    237: ("Новость", "Mustang GTD Nürburgring"),
    238: ("Новость", "VW из озера 1982"),
    239: ("Новость", "State bill collector cars"),
    240: ("Новость", "Porsche Apple livery Long Beach"),
    241: ("Новость", "Lamborghini Temerario Spyder"),
    # --- CNBC
    242: ("Не новость", "Pro news index"),
    243: ("Не новость", "Analyst calls landing"),
    244: ("Не по теме", "Wall Street 2026 outlook"),
    245: ("Не новость", "Chart Investing landing"),
    246: ("Новость", "foreign automakers sedans"),
    # --- CarNewsChina
    247: ("Не новость", "glossary"),
    248: ("Новость", "BYD 16M NEV milestone"),
    249: ("Дубль", "повтор 248"),
    250: ("Новость", "BYD Yuan Plus flash charging"),
    251: ("Новость", "BYD Sealion 05"),
    # --- Autocar India
    252: ("Не новость", "EMI calculator"),
    253: ("Не новость", "best scooters list"),
    254: ("Не новость", "EMI calculator"),
    255: ("Новость", "Chinese AUDI sedan 2027"),
    256: ("Новость", "Mercedes C-Class EV interior"),
    # --- Telegram дубли
    257: ("Дубль", "t.me chinamashina (повтор)"),
    258: ("Дубль", "t.me chinamashina"),
    259: ("Дубль", "t.me chinamashina"),
    260: ("Дубль", "t.me chinamashina"),
    261: ("Дубль", "t.me chinamashina"),
    # --- Китайские-автомобили.рф
    262: ("Новость", "Haval Raptor Plus у дилеров"),
    263: ("Новость", "Jeland первый кроссовер в РФ"),
    264: ("Новость", "Geely Galaxy Starshine 7"),
    265: ("Аналитика", "Geely Monjaro — обзор"),
    266: ("Новость", "Astana Motors Chery Словакия"),
    267: ("Дубль", "повтор 262"),
    268: ("Дубль", "повтор 263"),
    269: ("Дубль", "повтор 264"),
    270: ("Дубль", "повтор 265"),
    271: ("Дубль", "повтор 266"),
    # --- BMWBlog
    272: ("Новость", "ALPINA новые владельцы"),
    273: ("Новость", "BMW/MINI Plug & Charge Germany"),
    274: ("Новость", "Mercedes C-Class EV дашборд"),
    275: ("Новость", "BMW M2 Track Kit"),
    276: ("Новость", "BMW Steyr вторая линия"),
    277: ("Дубль", "повтор 272"),
    278: ("Дубль", "повтор 273"),
    279: ("Дубль", "повтор 274"),
    280: ("Дубль", "повтор 275"),
    281: ("Дубль", "повтор 276"),
    # --- CNEVPost
    282: ("Новость", "Geely Galaxy Starshine 7 pre-sales"),
    283: ("Новость", "Seres+BMW+Mercedes JV"),
    284: ("Новость", "Audi+SAIC China partnership"),
    285: ("Новость", "BYD 16M NEV milestone"),
    286: ("Новость", "VW Jetta X concept"),
    # --- CarBuzz
    287: ("Новость", "Porsche GT3 RS Nürburgring"),
    288: ("Новость", "Mustang GTD vs Corvette ZR1X"),
    289: ("Новость", "Used Hyundai $15k"),
    290: ("Новость", "Classic Nissan Xterra"),
    291: ("Новость", "Used Honda Prius alt"),
    # --- Ingosstrakh
    292: ("Не новость", "product page"),
    293: ("Ошибка", "empty response"),
    294: ("Новость", "Ингосстрах 3.8млрд автострахование"),
    295: ("Ошибка", "empty response"),
    296: ("Не по теме", "HR-премия Team Awards"),
    # --- 1prime
    297: ("Не по теме", "самозанятые"),
    298: ("Не по теме", "рынок акций"),
    299: ("Не по теме", "шоколад в Узбекистан"),
    300: ("Не по теме", "ЕС women workers"),
    301: ("Не по теме", "ЕС нефть из России"),
    # --- iz.ru
    302: ("Не по теме", "Бельгия ЕС Украина"),
    303: ("Не по теме", "Певкур НАТО"),
    304: ("Не по теме", "Израиль удар по Хияму"),
    305: ("Новость", "корейские авто +78% в РФ"),
    306: ("Не по теме", "Буданов Африка"),
    307: ("Не новость", "story/theme landing"),
    308: ("Не новость", "izvestia o vazhnom"),
    309: ("Не по теме", "США Израиль Иран"),
    310: ("Не новость", "izvestia pro dengi landing"),
    311: ("Не новость", "Terms of Service"),
    312: ("Дубль", "повтор 307"),
    313: ("Дубль", "повтор 308"),
    314: ("Дубль", "повтор 309"),
    315: ("Дубль", "повтор 310"),
    316: ("Дубль", "повтор 311"),
    # --- napinfo
    317: ("Аналитика", "доля китайских авто — аналитика"),
    318: ("Аналитика", "подорожания/падения — обзор"),
    319: ("Новость", "лизинг легковых +20%"),
    320: ("Аналитика", "растущие сегменты — обзор"),
    321: ("Новость", "транспортный налог на грузовики"),
    322: ("Аналитика", "инфографика путешествие"),
    323: ("Аналитика", "инфографика корпоративные"),
    324: ("Новость", "лизинг грузовых +25%"),
    325: ("Новость", "рынок автобусов"),
    326: ("Аналитика", "прогноз грузовых"),
    # --- autogpbl
    327: ("Не новость", "product page — лизинг"),
    328: ("Не новость", "product page — скидки"),
    329: ("Новость", "дилерская сеть 4.2k шоурумов"),
    330: ("Новость", "траты на новые авто"),
    331: ("Уточнить", "новые редакции условий лизинга — спорно"),
    # --- frankrg
    332: ("Не по теме", "рынок подписок"),
    333: ("Не по теме", "маркетинг брокеров"),
    334: ("Не по теме", "ипотечный клиент"),
    335: ("Не по теме", "выдачи кредитов март"),
    336: ("Не по теме", "подписки экосистемы"),
    # --- autostat archives
    337: ("Не новость", "archive /news/"),
    338: ("Не новость", "archive infographics 2026"),
    339: ("Не новость", "archive Jan 2026"),
    340: ("Не новость", "archive Feb 2026"),
    341: ("Не новость", "archive Mar 2026"),
    # --- t.me sergtselikov дубли
    342: ("Дубль", "повтор 7"),
    343: ("Дубль", "повтор 8"),
    344: ("Дубль", "повтор 9"),
    345: ("Дубль", "повтор 10"),
    346: ("Дубль", "повтор 11"),
    347: ("Дубль", "повтор 337"),
    # --- autostat interview archives
    348: ("Не новость", "archive interview 2026"),
    349: ("Не новость", "archive Jan"),
    350: ("Не новость", "archive Feb"),
    351: ("Не новость", "archive 2025"),
    # --- cbr, kommersant
    352: ("Не новость", "ЦБ news landing"),
    353: ("Не новость", "Kommersant theme landing"),
    354: ("Не новость", "Kommersant theme landing"),
    355: ("Не новость", "Kommersant archive theme/week"),
    356: ("Не новость", "Kommersant archive theme/month"),
    # --- interfax
    357: ("Не по теме", "металлы LME — Иран"),
    358: ("Не по теме", "БПЛА Белгород"),
    359: ("Не по теме", "губернатор Чуб иск"),
    360: ("Не по теме", "Ленобласть дроны"),
    361: ("Не по теме", "Ливан эвакуация"),
}


def main() -> int:
    creds = Credentials.from_service_account_file(str(SA_PATH), scopes=SCOPES)
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

    # 1. Write header in R1 and S1
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{TAB}'!R1:S1",
        valueInputOption="USER_ENTERED",
        body={"values": [["Вердикт Claude", "Обоснование Claude"]]},
    ).execute()

    # 2. Bulk write all verdicts using one update
    # Build a compact range starting at R2 going down, filling rows in order.
    max_row = max(VERDICTS)
    values: list[list[str]] = []
    for row_num in range(2, max_row + 1):
        v, r = VERDICTS.get(row_num, ("", ""))
        values.append([v, r])

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"'{TAB}'!R2:S{max_row}",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()

    print(f"Wrote {len(VERDICTS)} Claude verdicts into {TAB!r} columns R+S.")

    # 3. Distribution
    from collections import Counter
    dist = Counter(v for v, _ in VERDICTS.values())
    print("\nClaude verdict distribution:")
    for v, n in dist.most_common():
        print(f"  {v:25} {n:4}  ({n/len(VERDICTS):.0%})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
