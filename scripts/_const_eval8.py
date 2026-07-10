"""Eval-gate v8 (jul-10): US-domestic classes from the red-mark audit.
Baseline = prompt minus the new block; candidate = as shipped. Sonnet."""
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
BLOCK = ''' - a US (or other single foreign country) regulator's PROPOSAL / consideration /
   data demand - the investigations rule applies to foreign agencies too.
   Editor (10.07): "U.S. regulators consider removing steering wheel
   requirement" -> REJECT (proposal, not a decision); "U.S. regulators demand
   AV companies report data" -> REJECT (administrative step). Actual US
   RECALLS remain wanted (Other news); an ENACTED law/decree with market
   effect stays (Other news, Global - the Trump right-to-repair memo was
   published).
 - single-FOREIGN-country sales results - «только глобальные продажи»: a
   brand's US/German/etc. market figures -> REJECT. Editor (07.07/10.07):
   "GM retains US sales lead" -> REJECT; "Mercedes-Benz USA reported 84,500
   Q2 retail sales" -> REJECT. Brand GLOBAL totals, China-market OFFICIAL
   stats, and RF-market figures keep their existing routes.
 - a robotaxi/ride-service OPERATOR's territory expansion - "Waymo launches
   in Las Vegas", "Waymo adds 4 new markets" -> REJECT (operator territory
   news, not an automotive product event). A robotaxi VEHICLE reveal / tech
   milestone stays (Other news).
'''
BASE = CAND.replace("\n" + BLOCK, "\n", 1)
assert len(BASE) < len(CAND), "block not found"

ITEMS = json.loads((ROOT/"data/_const_testset.json").read_text(encoding="utf-8"))
ITEMS += json.loads((ROOT/"data/_const_fresh_cases.json").read_text(encoding="utf-8"))
ITEMS += [
 {"key":"T1 US steering-wheel proposal","title":"EN: U.S. regulators consider removing steering wheel requirement for AVs (EN)","lid":"NHTSA is considering updating FMVSS rules to remove the steering wheel requirement for fully autonomous vehicles, according to a regulatory filing.","exp_pub":False,"exp_sec":""},
 {"key":"T2 US AV data demand","title":"EN: U.S. regulators demand autonomous vehicle companies report crash data (EN)","lid":"US regulators demanded that AV companies submit expanded incident data under a new administrative order.","exp_pub":False,"exp_sec":""},
 {"key":"T3 Mercedes USA Q2","title":"EN: Mercedes-Benz USA reported 84,500 retail sales in Q2 2026 (EN)","lid":"Mercedes-Benz USA announced retail sales of 84,500 vehicles for the second quarter, up 3% year-over-year in the US market.","exp_pub":False,"exp_sec":""},
 {"key":"T4 Waymo Vegas","title":"EN: Waymo launches driverless service in Las Vegas, plans 4 more markets (EN)","lid":"Waymo opened its robotaxi service to the public in Las Vegas and said it plans to expand to four more US cities next year.","exp_pub":False,"exp_sec":""},
 {"key":"T5 Ram Hellcat unit","title":"EN: Ram's 777-HP Hellcat V8 engine is a Redeye unit (EN)","lid":"The 777-horsepower Hellcat V8 in the new Ram is actually a Redeye-spec unit, according to parts documentation.","exp_pub":False,"exp_sec":""},
 # guards
 {"key":"G1 NHTSA recall stays","title":"EN: Ford recalls 67,842 vehicles due to windshield defect (EN)","lid":"NHTSA recall campaign: Ford is recalling 67,842 vehicles over a windshield bonding defect. Report 26V-410.","exp_pub":True,"exp_sec":"Other news"},
 {"key":"G2 Trump memo stays","title":"EN: Trump signed a right-to-repair memorandum (EN)","lid":"The US president signed a memorandum directing agencies to guarantee vehicle right-to-repair, with direct effect on automakers' service policies.","exp_pub":True,"exp_sec":"Other news"},
 {"key":"G3 China NEV monthly stays","title":"EN: China NEV retail sales fall for sixth consecutive month, CPCA reports (EN)","lid":"CPCA reported actual China NEV retail sales for June: 1.02 million units, down 4% — the sixth consecutive monthly decline.","exp_pub":True,"exp_sec":""},
 {"key":"G4 RU sales stay Local","title":"EN: Russian car market grew 5% in June, AEB reports (RU)","lid":"Продажи новых автомобилей в России в июне выросли на 5%, сообщает АЕБ. Лидеры - Lada, Haval, Chery.","exp_pub":True,"exp_sec":""},
]
WATCH={k:"" for k in ["T1 US steering-wheel proposal","T2 US AV data demand","T3 Mercedes USA Q2","T4 Waymo Vegas","T5 Ram Hellcat unit","G1 NHTSA recall stays","G2 Trump memo stays","G3 China NEV monthly stays","G4 RU sales stay Local"]}

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
    for k in WATCH: print(f"  {k[:34]:34} {bl.get(k)} -> {cd.get(k)}")
    (ROOT/"data/_const_eval8_out.json").write_text(json.dumps(
        {"baseline":b,"cand":c,"net":len(fixes)-len(regs),"fixes":fixes,"regs":regs},
        ensure_ascii=False, indent=1), encoding="utf-8")
    return 0

if __name__=="__main__": sys.exit(main())
