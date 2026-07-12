"""Eval-gate v9 (jul-10): dealer-minutiae + carsharing-promo classes (miner
top-2, both low-маятник). Baseline = prompt minus the block; Sonnet."""
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
BLOCK = ''' - single-model RF availability MINUTIAE: a model spotted in the registration
   database («встал на учёт в РФ»), "first buyers found", dealers CLAIM a
   shortage/deficit, a car appearing on / vanishing from a dealer or brand
   site, arriving at showrooms ahead of launch -> REJECT. Editor
   (18.06-06.07): "Tenet T9 встал на учет", "SKM нашли первых покупателей",
   "дилеры заявили о дефиците Changan Uni-S", "BYD Linghui M9 прибыл в салоны
   до официального запуска" -> нет. This is RETAIL-availability noise ONLY —
   it does not reroute anything else: official market launches, sales starts,
   production events, model/concept reveals and teasers all keep whatever
   route the other rules give them.
 - CARSHARING operator promos: discounts, loyalty points / bonuses, fuel
   cashback, drop-off-zone tweaks -> REJECT even for major operators. Editor
   (06.07): «VORON новые зоны», «Ситидрайв баллы за заправку», «BelkaCar
   повысил бонусы», «скидка 20% у Яндекс Драйва» -> не нужно. A city LAUNCH,
   fleet addition (Delimobil added Vesta -> Local) and carsharing MARKET
   stats stay Local specifics; an AUTOMAKER's promo/cashback stays Dealer
   news/Promo.
'''
BASE = CAND.replace("\n" + BLOCK, "\n", 1)
assert len(BASE) < len(CAND), "block not found"

ITEMS = json.loads((ROOT/"data/_const_testset.json").read_text(encoding="utf-8"))
ITEMS += json.loads((ROOT/"data/_const_fresh_cases.json").read_text(encoding="utf-8"))
ITEMS += [
 {"key":"T1 Tenet T9 registered","title":"EN: Tenet T9 crossover spotted in Russian registration database (RU)","lid":"Кроссовер Tenet T9 встал на учет в РФ, следует из данных регистрационной базы. Официальный старт продаж не объявлен.","exp_pub":False,"exp_sec":""},
 {"key":"T2 SKM first buyers","title":"EN: SKM cars found their first buyers in Russia (RU)","lid":"Автомобили SKM нашли первых покупателей в России, сообщили дилеры марки.","exp_pub":False,"exp_sec":""},
 {"key":"T3 Uni-S dealer deficit","title":"EN: Russian dealers report shortage of Changan Uni-S (RU)","lid":"Дилеры в РФ заявили о дефиците кроссовера Changan Uni-S. Поставки ожидаются в следующем месяце.","exp_pub":False,"exp_sec":""},
 {"key":"T4 VORON drop-off zones","title":"EN: VORON carsharing added new rental drop-off zones (RU)","lid":"Каршеринг VORON добавил новые зоны завершения аренды в нескольких районах Москвы.","exp_pub":False,"exp_sec":""},
 {"key":"T5 BelkaCar fuel bonuses","title":"EN: BelkaCar raised fuel bonuses to 3,000 RUB (RU)","lid":"Каршеринг BelkaCar повысил бонусы за заправку автомобилей до 3 000 рублей.","exp_pub":False,"exp_sec":""},
 # guards — neighbouring PUBLISH routes that must not flip
 {"key":"G1 Koleos RF sales start","title":"EN: Renault Koleos sales started in Russia (RU)","lid":"В России официально стартовали продажи кроссовера Renault Koleos. Цены объявлены дилерами марки, первые машины уже в салонах.","exp_pub":True,"exp_sec":""},
 {"key":"G2 Delimobil Vesta fleet","title":"EN: Delimobil added Lada Vesta to its carsharing fleet (RU)","lid":"Делимобиль добавил Lada Vesta в свой каршеринговый парк в Москве и Петербурге.","exp_pub":True,"exp_sec":"Local specifics"},
 {"key":"G3 Delimobil city launch","title":"EN: Delimobil launched service in Naberezhnye Chelny (RU)","lid":"Делимобиль начал работу в Набережных Челнах — это 15-й город присутствия сервиса.","exp_pub":True,"exp_sec":"Local specifics"},
 {"key":"G4 carsharing market stats","title":"EN: Russian carsharing turnover fell 17.7% in H1 (RU)","lid":"Оборот российского каршеринга упал на 17,7% в первом полугодии, подсчитали аналитики рынка.","exp_pub":True,"exp_sec":""},
]
WATCH={k:"" for k in ["T1 Tenet T9 registered","T2 SKM first buyers","T3 Uni-S dealer deficit","T4 VORON drop-off zones","T5 BelkaCar fuel bonuses","G1 Koleos RF sales start","G2 Delimobil Vesta fleet","G3 Delimobil city launch","G4 carsharing market stats"]}

def matches(pub, sec, it):
    if pub != it["exp_pub"]: return False
    if not it["exp_pub"] or not it["exp_sec"]: return True
    return (sec or "") == it["exp_sec"]

def run_all(client, secs, prompt, label):
    base.EDITORIAL_REVIEW_SYSTEM = prompt
    out={}
    for it in ITEMS:
        try:
            r,_=client.editorial_review(title=it["title"], body=it["lid"] or it["title"],
                                        sections=secs, portal_country="Russia")
            out[it["key"]]=(r.should_publish, r.section or "")
        except Exception as e:
            print(f"  ! {label} {it['key'][:22]}: {type(e).__name__} {str(e)[:40]}")
            out[it["key"]]=(None,"")
    base.EDITORIAL_REVIEW_SYSTEM = CAND
    return out

def main():
    client=make_llm_client(get_settings()); client.model="claude-sonnet-4-6"
    secs=load_sections()
    print(f"items {len(ITEMS)} model {client.model}")
    bl=run_all(client,secs,BASE,"base")
    b=sum(1 for it in ITEMS if matches(*bl[it["key"]],it))
    print(f"baseline {b}/{len(ITEMS)}")
    cd=run_all(client,secs,CAND,"cand")
    c=sum(1 for it in ITEMS if matches(*cd[it["key"]],it))
    fixes=[it["key"] for it in ITEMS if matches(*cd[it["key"]],it) and not matches(*bl[it["key"]],it)]
    regs=[it["key"] for it in ITEMS if matches(*bl[it["key"]],it) and not matches(*cd[it["key"]],it)]
    print(f"candidate {c}/{len(ITEMS)}  net {len(fixes)-len(regs):+d}")
    print("fixes:",fixes); print("regs:",regs)
    print("\nWATCH:")
    for k in WATCH: print(f"  {k[:32]:32} {bl.get(k)} -> {cd.get(k)}")
    (ROOT/"data/_const_eval9_out.json").write_text(json.dumps(
        {"baseline":b,"cand":c,"net":len(fixes)-len(regs),"fixes":fixes,"regs":regs},
        ensure_ascii=False, indent=1), encoding="utf-8")
    return 0

if __name__=="__main__": sys.exit(main())
