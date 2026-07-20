"""Eval-gate v12 (jul-20): 5-class «ненужные» batch from the 16-19.07 feedback
(52 non-dup rejects mined): track-only specials, interest surveys, carsharing
user-mileage PR, lab experiments, golf-cart LSV. Baseline = prompt minus the
block; Sonnet (prod editorial model)."""
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
START = " - TRACK-ONLY specials and one-off niche performance builds"
END = " - already covered / stale"
i0 = CAND.index(START)
i1 = CAND.index(END)
BASE = CAND[:i0] + CAND[i1:]
assert len(BASE) < len(CAND) and START not in BASE, "block strip failed"

ITEMS = json.loads((ROOT / "data/_const_testset.json").read_text(encoding="utf-8"))
ITEMS += json.loads((ROOT / "data/_const_fresh_cases.json").read_text(encoding="utf-8"))
ITEMS += [
 {"key": "T1 Jensen track-only", "title": "EN: Jensen unveiled track-only Interceptor GTX prototype (EN)",
  "lid": "Компания Jensen представила трековый прототип Interceptor GTX с механической коробкой и компрессорным V8. Автомобиль предназначен только для гоночного трека, дорожная версия не планируется.",
  "exp_pub": False, "exp_sec": ""},
 {"key": "T2 VW interest survey", "title": "EN: Interest in Volkswagen vehicles increased in Russia among buyers (RU)",
  "lid": "Опрос сервиса показал: интерес россиян к автомобилям Volkswagen вырос и на первичном, и на вторичном рынке. Данных о продажах не приводится.",
  "exp_pub": False, "exp_sec": ""},
 {"key": "T3 hybrids survey", "title": "EN: Moscow residents switch to hybrids twice as often as other regions (RU)",
  "lid": "Москвичи в два раза чаще жителей других регионов пересаживаются на гибриды, показал опрос онлайн-сервиса объявлений.",
  "exp_pub": False, "exp_sec": ""},
 {"key": "T4 carsharing mileage PR", "title": "EN: Moscow carsharing users drove approximately 500 mln km in 2026 (RU)",
  "lid": "Пользователи московского каршеринга проехали около 500 млн километров с начала 2026 года, сообщил оператор сервиса. Это более 12 тысяч кругосветных путешествий.",
  "exp_pub": False, "exp_sec": ""},
 {"key": "T5 dandelion fuel lab", "title": "EN: Engineers developed system enabling vehicles to run on renewable fuel",
  "lid": "Инженеры испытали топливо из растительного масла: эксперимент показал работоспособность технологии на тестовом стенде. О серийном продукте речи пока не идёт.",
  "exp_pub": False, "exp_sec": ""},
 {"key": "T6 golf-cart LSV", "title": "EN: Chip is a $15,000 EV with 25-mph top speed (EN)",
  "lid": "Стартап Chip Motors представил в США электромобиль за 15 000 долларов с максимальной скоростью 25 миль в час и встроенным телевизором. Машина относится к классу низкоскоростных (LSV) и не допущена на хайвеи.",
  "exp_pub": False, "exp_sec": ""},
 {"key": "G1 M3 CS road special", "title": "EN: BMW unveiled road-legal M3 CS with manual gearbox (EN)",
  "lid": "BMW представила дорожную спецверсию M3 CS с механической коробкой передач. Продажи на основных рынках начнутся осенью, цены объявлены.",
  "exp_pub": True, "exp_sec": ""},
 {"key": "G2 track record Other", "title": "EN: Porsche Taycan set a new Nurburgring lap record (EN)",
  "lid": "Серийный Porsche Taycan установил новый рекорд круга Нюрбургринга среди электромобилей. Результат подтверждён официальным хронометражем.",
  "exp_pub": True, "exp_sec": ""},
 {"key": "G3 carsharing market stats", "title": "EN: Car-sharing demand drops as prices rise (RU)",
  "lid": "Спрос на каршеринг снизился из-за роста тарифов: оборот операторов упал на 17% в первом полугодии, подсчитали аналитики рынка.",
  "exp_pub": True, "exp_sec": ""},
 {"key": "G4 loan transactions", "title": "EN: Number of auto loans issued in June rose 14% year-on-year (RU)",
  "lid": "Число выданных автокредитов в июне выросло на 14% год к году, подсчитало ОКБ. Средний размер кредита также увеличился.",
  "exp_pub": True, "exp_sec": ""},
 {"key": "G5 solid-state to series", "title": "EN: Toyota starts series production of solid-state batteries for EVs (EN)",
  "lid": "Toyota начала серийное производство твердотельных батарей для электромобилей на заводе в Японии. Первые модели с новыми АКБ выйдут в следующем году.",
  "exp_pub": True, "exp_sec": ""},
 {"key": "G6 kei car homologated", "title": "EN: BYD plans to enter Japanese kei-car market (EN)",
  "lid": "BYD готовит выход на японский рынок кей-каров: компактная модель получит омологацию для дорог общего пользования и поступит в продажу в Японии.",
  "exp_pub": True, "exp_sec": ""},
]
WATCH = [it["key"] for it in ITEMS if it["key"][:2] in
         ("T1","T2","T3","T4","T5","T6","G1","G2","G3","G4","G5","G6")]

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
            r, _ = client.editorial_review(title=it["title"], body=it["lid"] or it["title"],
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
    print(f"baseline {b}/{len(ITEMS)}")
    cd = run_all(client, secs, CAND, "cand")
    c = sum(1 for it in ITEMS if matches(*cd[it["key"]], it))
    fixes = [it["key"] for it in ITEMS if matches(*cd[it["key"]], it) and not matches(*bl[it["key"]], it)]
    regs = [it["key"] for it in ITEMS if matches(*bl[it["key"]], it) and not matches(*cd[it["key"]], it)]
    print(f"candidate {c}/{len(ITEMS)}  net {len(fixes)-len(regs):+d}")
    print("fixes:", fixes)
    print("regs:", regs)
    print()
    print("WATCH:")
    for k in WATCH:
        print(f"  {k[:34]:34} {bl.get(k)} -> {cd.get(k)}")
    (ROOT / "data/_const_eval12_out.json").write_text(json.dumps(
        {"baseline": b, "cand": c, "net": len(fixes)-len(regs), "fixes": fixes, "regs": regs},
        ensure_ascii=False, indent=1), encoding="utf-8")
    return 0

if __name__ == "__main__":
    sys.exit(main())
