"""Eval-gate v14 (jul-27): STEP-2b rescue list (6 classes the editor marked
«да, нужна» in the review tab). Baseline = prompt minus the block; Sonnet.

FOCUSED item set on purpose: the full 94-item regression suite costs ~$1.2
per side. Here we run the 6 target classes + the guards that could plausibly
flip (the rejects each rescue borders on), ~30 items => ~$0.35 total.
"""
from __future__ import annotations
import json, sys, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=True)
import news_agent.adapters.llm.base as base
from news_agent.adapters.llm import make_llm_client
from news_agent.core.config_loader import load_sections
from news_agent.settings import get_settings

CAND = base.EDITORIAL_REVIEW_SYSTEM
_start = CAND.index("------ STEP 2b")
_end = CAND.index("STEP 3 - Which section?")
BLOCK = CAND[_start:_end]
BASE = CAND.replace(BLOCK, "", 1)
assert len(BASE) < len(CAND) and "STEP 2b" not in BASE, "block not stripped"

ITEMS = [
 # ---- targets: the editor's own «да» rows ----
 {"key": "T1 CATL storage system", "exp_pub": True, "exp_sec": "",
  "title": "EN: CATL launched Tener Sodium energy storage system (EN)",
  "lid": "CATL представила систему накопления энергии Tener Sodium на натрий-ионных элементах для промышленных объектов."},
 {"key": "T2 electrolyte research", "exp_pub": True, "exp_sec": "",
  "title": "EN: CAS electrolyte extends solid-state battery cycles to 350 (EN)",
  "lid": "Учёные Китайской академии наук разработали электролит, увеличивающий ресурс твердотельных батарей до 350 циклов при сохранении 84,2% ёмкости."},
 {"key": "T3 Toyota hydrogen initiative", "exp_pub": True, "exp_sec": "",
  "title": "EN: Toyota Mobility Foundation launches European Hydrogen Regions Initiative (EN)",
  "lid": "Фонд Toyota Mobility Foundation запустил инициативу European Hydrogen Regions и объявил приём заявок от регионов-партнёров."},
 {"key": "T4 Ferrari patent no model", "exp_pub": True, "exp_sec": "",
  "title": "EN: Ferrari developed adaptive rear wing with bending capability (EN)",
  "lid": "В патентной заявке Ferrari описано адаптивное заднее крыло, способное изгибаться и скручиваться. Конкретная модель не названа."},
 {"key": "T5 Tata patent images", "exp_pub": True, "exp_sec": "",
  "title": "EN: Tata Tigor facelift design revealed via patent filing (EN)",
  "lid": "Патентные изображения раскрыли дизайн рестайлинга Tata Tigor. Официального анонса модели пока не было."},
 {"key": "T6 VW Uzbekistan presence", "exp_pub": True, "exp_sec": "",
  "title": "EN: Volkswagen expanded its presence in Uzbekistan (EN)",
  "lid": "Volkswagen расширил присутствие в Узбекистане: подписано соглашение о расширении дилерской сети и локальной сборки."},
 {"key": "T7 Jeland line-up expansion", "exp_pub": True, "exp_sec": "",
  "title": "EN: Jeland to expand line-up to 5 models (RU)",
  "lid": "Марка Jeland расширит модельный ряд до пяти моделей: помимо кроссоверов появятся седаны."},
 {"key": "T8 BYD China sales start", "exp_pub": True, "exp_sec": "",
  "title": "EN: BYD Seal 08 sedan sales to begin in China on July 2 (RU)",
  "lid": "Продажи седана BYD Seal 08 в Китае стартуют 2 июля, объявил производитель. Названы комплектации."},
 {"key": "T9 BMW pre-order edition", "exp_pub": True, "exp_sec": "",
  "title": "EN: BMW iX3 M Sport First Edition coming to single market in 2026 (EN)",
  "lid": "BMW открыла предзаказ на iX3 M Sport First Edition — версия выйдет на один рынок в 2026 году."},
 {"key": "T10 Kushaq 100k milestone", "exp_pub": True, "exp_sec": "",
  "title": "EN: Skoda Kushaq reached 100,000 sales milestone (EN)",
  "lid": "Кроссовер Skoda Kushaq преодолел отметку в 100 000 проданных автомобилей в Индии за четыре года."},
 {"key": "T11 Jeep special edition", "exp_pub": True, "exp_sec": "",
  "title": "EN: Jeep honors a military icon with new Sarge Editions (EN)",
  "lid": "Jeep представил пакет опций Sarge Editions для Wrangler и Gladiator: особая окраска, эмблемы и внедорожное оснащение."},
 {"key": "T12 McLaren race special", "exp_pub": True, "exp_sec": "",
  "title": "EN: Artura 1000GP celebrates McLaren's 1000th Formula 1 race (RU)",
  "lid": "McLaren выпустил спецверсию суперкара Artura 1000GP, приуроченную к тысячной гонке команды в Формуле-1. Тираж ограничен."},
 {"key": "T13 RF insurance calc", "exp_pub": True, "exp_sec": "Local specifics",
  "title": "EN: Ingosstrakh calculated insurance costs for new Volga models (RU)",
  "lid": "«Ингосстрах» рассчитал стоимость полисов каско и ОСАГО для новых моделей Volga: приведены суммы по каждой версии."},
 # ---- guards: neighbouring REJECTS that must stay rejected ----
 {"key": "G1 foreign periodic results", "exp_pub": False, "exp_sec": "",
  "title": "EN: Mercedes-Benz USA reported 84,500 Q2 retail sales (EN)",
  "lid": "Mercedes-Benz USA отчиталась о 84 500 розничных продаж во втором квартале на американском рынке."},
 {"key": "G2 GM US sales lead", "exp_pub": False, "exp_sec": "",
  "title": "EN: GM retains US sales lead in the first half (EN)",
  "lid": "General Motors сохранила лидерство по продажам на рынке США по итогам первого полугодия."},
 {"key": "G3 Waymo territory", "exp_pub": False, "exp_sec": "",
  "title": "EN: Waymo launches robotaxi service in Las Vegas (EN)",
  "lid": "Waymo запустила сервис роботакси в Лас-Вегасе — это четвёртый город присутствия оператора."},
 {"key": "G4 motorsport result", "exp_pub": False, "exp_sec": "",
  "title": "EN: Ferrari won the Le Mans 24 Hours (EN)",
  "lid": "Экипаж Ferrari выиграл гонку «24 часа Ле-Мана», опередив Toyota на две минуты."},
 {"key": "G5 forecast", "exp_pub": False, "exp_sec": "",
  "title": "EN: Analysts expect EV sales to rise 20% next year (EN)",
  "lid": "Аналитики прогнозируют рост продаж электромобилей на 20% в следующем году."},
 {"key": "G6 per-model discount", "exp_pub": False, "exp_sec": "",
  "title": "EN: Dealers cut prices on Haval Jolion by 200,000 RUB (RU)",
  "lid": "Дилеры снизили цены на Haval Jolion на 200 тысяч рублей в рамках июльской акции."},
 {"key": "G7 oil pipeline", "exp_pub": False, "exp_sec": "",
  "title": "EN: Gazprom commissioned a new gas pipeline section (RU)",
  "lid": "«Газпром» ввёл в эксплуатацию новый участок газопровода мощностью 15 млрд кубометров."},
 {"key": "G8 heavy truck", "exp_pub": False, "exp_sec": "",
  "title": "EN: KamAZ opened a new service centre for heavy trucks (RU)",
  "lid": "КамАЗ открыл сервисный центр для тяжёлых грузовиков в Новосибирске."},
 {"key": "G9 tyre survey", "exp_pub": False, "exp_sec": "",
  "title": "EN: Survey named the most popular tyre brands in Russia (RU)",
  "lid": "Опрос показал, какие бренды шин чаще всего покупают россияне этим летом."},
 {"key": "G10 new-regions law", "exp_pub": False, "exp_sec": "",
  "title": "EN: Federation Council approved law simplifying vehicle registration in new regions (RU)",
  "lid": "Совет Федерации одобрил закон, упрощающий регистрацию транспортных средств в новых регионах России."},
 # ---- guards: normal PUBLISH routes that must not shift ----
 {"key": "G11 RF sales start", "exp_pub": True, "exp_sec": "Confirmed",
  "title": "EN: Renault Koleos sales started in Russia (RU)",
  "lid": "В России официально стартовали продажи кроссовера Renault Koleos, цены объявлены дилерами."},
 {"key": "G12 RF market stats", "exp_pub": True, "exp_sec": "Local specifics",
  "title": "EN: Russian new-car market grew 12% in June 2026 (RU)",
  "lid": "Рынок новых легковых автомобилей в России вырос на 12% в июне 2026 года, подсчитали в АВТОСТАТе."},
 {"key": "G13 spy shot", "exp_pub": True, "exp_sec": "Rumors",
  "title": "EN: New Skoda Kodiaq spied during tests (RU)",
  "lid": "Прототип нового Skoda Kodiaq замечен на дорожных испытаниях в камуфляже."},
 {"key": "G14 recall", "exp_pub": True, "exp_sec": "",
  "title": "EN: Land Rover recalls over 15,000 Discovery vehicles (RU)",
  "lid": "Land Rover отзывает более 15 тысяч Discovery из-за попадания воды в камеру заднего вида."},
 {"key": "G15 model reveal", "exp_pub": True, "exp_sec": "Confirmed",
  "title": "EN: Mercedes-Benz unveiled the new G-Class cabriolet (RU)",
  "lid": "Mercedes-Benz представил новый кабриолет G-Class мощностью почти 600 л.с. Продажи начнутся осенью."},
]
WATCH = [it["key"] for it in ITEMS]


def matches(pub, sec, it):
    if pub != it["exp_pub"]:
        return False
    if not it["exp_pub"] or not it["exp_sec"]:
        return True
    return (sec or "") == it["exp_sec"]


def run_all(client, secs, prompt, label):
    base.EDITORIAL_REVIEW_SYSTEM = prompt
    out = {}
    for it in ITEMS:
        try:
            r, _ = client.editorial_review(title=it["title"], body=it["lid"],
                                           sections=secs, portal_country="Russia")
            out[it["key"]] = (r.should_publish, r.section or "")
        except Exception as e:
            print(f"  ! {label} {it['key'][:22]}: {type(e).__name__} {str(e)[:40]}")
            out[it["key"]] = (None, "")
    base.EDITORIAL_REVIEW_SYSTEM = CAND
    return out


def main():
    client = make_llm_client(get_settings()); client.model = "claude-sonnet-4-6"
    secs = load_sections()
    print(f"items {len(ITEMS)} model {client.model}")
    bl = run_all(client, secs, BASE, "base")
    b = sum(1 for it in ITEMS if matches(*bl[it["key"]], it))
    cd = run_all(client, secs, CAND, "cand")
    c = sum(1 for it in ITEMS if matches(*cd[it["key"]], it))
    fixes = [it["key"] for it in ITEMS if matches(*cd[it["key"]], it) and not matches(*bl[it["key"]], it)]
    regs = [it["key"] for it in ITEMS if matches(*bl[it["key"]], it) and not matches(*cd[it["key"]], it)]
    print(f"baseline {b}/{len(ITEMS)}  candidate {c}/{len(ITEMS)}  net {len(fixes)-len(regs):+d}")
    print("fixes:", fixes)
    print("regs :", regs)
    print("\nпострочно:")
    for k in WATCH:
        mark = "  " if bl.get(k) == cd.get(k) else "->"
        print(f" {mark} {k[:30]:30} {str(bl.get(k)):32} {str(cd.get(k))}")
    (ROOT / "data/_const_eval14_out.json").write_text(json.dumps(
        {"baseline": b, "cand": c, "net": len(fixes)-len(regs), "fixes": fixes,
         "regs": regs, "n": len(ITEMS)}, ensure_ascii=False, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
