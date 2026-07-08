"""Eval-gate v7 (jul-08): 3 recurring-complaint additions, user-directed.

C1 stock-price moves -> REJECT (8 editor mentions since May)
C2 executive-defense / no-concretes -> REJECT (10 mentions)
C3 Motorshow only for MAJOR shows (2 mentions)

Baseline = current prompt with the three inserted blocks removed in-memory.
Candidate = prompt as shipped. Sonnet (prod editorial model).
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
B1 = ''' - routine share-PRICE movement (stock rises / falls N%, hits a low/high,
   market-cap swings) - «акции не постим»: reject even for major brands.
   Editor (21.05/08.07): "Leapmotor shares plunge to 2-month low" -> REJECT;
   "Rivian stock falls 18%, little detail" -> REJECT. BUT a brand actually
   SELLING a stake / issuing shares as a concrete strategic or refinancing
   deal WITH details stays publishable (financial).
 - an executive DEFENDING / explaining a past decision, "opens the door to" /
   possibility-talk, or an overview piece with no new concrete fact -
   «нет конкретики, ни о чем». Editor (10.06/14.06): "Ford defends sedan
   discontinuation" -> REJECT; "BMW opens door to more US wagons" -> REJECT;
   "AvtoVAZ CEO speaks at Expert Council" (speech, no figures) -> REJECT.
'''
B2_NEW = '''5) Motorshow - a MULTI-model OEM line-up at a MAJOR auto show (an
   Innoprom / Shanghai / Munich-scale salon). A single model at a show ->
   Confirmed. A brand's own event / small venue showing 1-2 concepts is NOT
   Motorshow -> route as Confirmed/Facts («в выставки постим только крупные
   автосалоны», 30.06; Peugeot two concepts at a brand event 08.07 -> Факты).'''
B2_OLD = '''5) Motorshow - a MULTI-model OEM line-up at a show. A single model at a show ->
   Confirmed.'''
BASE = CAND.replace("\n" + B1, "\n", 1).replace(B2_NEW, B2_OLD, 1)
assert len(BASE) < len(CAND), "blocks not found!"

ITEMS = json.loads((ROOT/"data/_const_testset.json").read_text(encoding="utf-8"))
ITEMS += json.loads((ROOT/"data/_const_fresh_cases.json").read_text(encoding="utf-8"))
ITEMS += [
 {"key":"T1 Leapmotor shares plunge","title":"EN: Leapmotor shares plunge to 2-month low (EN)","lid":"Leapmotor stock fell sharply to a two-month low on the Hong Kong exchange amid a broader selloff in Chinese EV makers.","exp_pub":False,"exp_sec":""},
 {"key":"T2 Rivian stock falls 18","title":"EN: Rivian stock falls 18% after company sells $1.3B in shares (EN)","lid":"Rivian shares dropped 18% after the automaker announced a share sale. Few additional details were disclosed.","exp_pub":False,"exp_sec":""},
 {"key":"T3 Ford defends sedan exit","title":"EN: Ford defends sedan discontinuation amid criticism (EN)","lid":"Ford executives defended the decision to exit the sedan segment, saying the company remains focused on trucks and SUVs. No new products were announced.","exp_pub":False,"exp_sec":""},
 {"key":"T4 AvtoVAZ CEO speech","title":"EN: AvtoVAZ CEO speaks at Expert Council on industry support (RU)","lid":"Глава АвтоВАЗа выступил на экспертном совете о поддержке отрасли. Конкретных цифр и решений в выступлении не прозвучало.","exp_pub":False,"exp_sec":""},
 {"key":"T5 Peugeot 2 concepts small event","title":"EN: Peugeot confirmed two new radical concepts at brand design event (EN)","lid":"Peugeot confirmed it will show two radical new concept cars, previewed at the brand's own design event in Paris. Both concepts preview future production models.","exp_pub":True,"exp_sec":"Confirmed"},
 # guards
 {"key":"G1 BYD quarterly results","title":"EN: BYD reports Q2 revenue up 20%, net profit 9.2B yuan (EN)","lid":"BYD posted quarterly revenue growth of 20% year-on-year with net profit of 9.2 billion yuan for the finished quarter.","exp_pub":True,"exp_sec":""},
 {"key":"G2 VW stake sale detailed","title":"EN: Volkswagen sells 51% of Everllence unit for 7.4B euro (RU)","lid":"Volkswagen продаёт 51% акций подразделения Everllence инвесткомпании за €7,4 млрд в рамках реструктуризации, сообщает Der Spiegel.","exp_pub":True,"exp_sec":"Rumors"},
]
WATCH = {"T1 Leapmotor shares plunge":"target C1a","T2 Rivian stock falls 18":"target C1b",
         "T3 Ford defends sedan exit":"target C2a","T4 AvtoVAZ CEO speech":"target C2b",
         "T5 Peugeot 2 concepts small event":"target C3 (sec=Confirmed)",
         "G1 BYD quarterly results":"GUARD fin-results stay","G2 VW stake sale detailed":"GUARD stake-sale stays"}

def matches(pub, sec, it):
    if pub != it["exp_pub"]: return False
    if not it["exp_pub"] or not it["exp_sec"]: return True
    return (sec or "") == it["exp_sec"]

def run_all(client, secs, prompt, label):
    base.EDITORIAL_REVIEW_SYSTEM = prompt
    out={}
    for it in ITEMS:
        try:
            r,_ = client.editorial_review(title=it["title"], body=it["lid"] or it["title"],
                                          sections=secs, portal_country="Russia")
            out[it["key"]]=(r.should_publish, r.section or "")
        except Exception as e:
            print(f"  ! {label} {it['key'][:24]}: {type(e).__name__} {str(e)[:50]}")
            out[it["key"]]=(None,"")
    base.EDITORIAL_REVIEW_SYSTEM = CAND
    return out

def main():
    client = make_llm_client(get_settings()); client.model="claude-sonnet-4-6"
    secs = load_sections()
    print(f"items {len(ITEMS)} model {client.model}")
    bl = run_all(client, secs, BASE, "base")
    b_ok = sum(1 for it in ITEMS if matches(*bl[it["key"]], it))
    print(f"baseline {b_ok}/{len(ITEMS)}")
    cd = run_all(client, secs, CAND, "cand")
    c_ok = sum(1 for it in ITEMS if matches(*cd[it["key"]], it))
    fixes=[it["key"] for it in ITEMS if matches(*cd[it["key"]],it) and not matches(*bl[it["key"]],it)]
    regs=[it["key"] for it in ITEMS if matches(*bl[it["key"]],it) and not matches(*cd[it["key"]],it)]
    print(f"candidate {c_ok}/{len(ITEMS)}  net {len(fixes)-len(regs):+d}")
    print("fixes:", fixes); print("regs:", regs)
    print("\nWATCH:")
    for k,d in WATCH.items(): print(f"  {d:28} {k[:30]!r}: {bl.get(k)} -> {cd.get(k)}")
    (ROOT/"data/_const_eval7_out.json").write_text(json.dumps(
        {"baseline":b_ok,"cand":c_ok,"net":len(fixes)-len(regs),"fixes":fixes,"regs":regs,
         "watch":{k:[bl.get(k),cd.get(k)] for k in WATCH}}, ensure_ascii=False, indent=1), encoding="utf-8")
    print("saved data/_const_eval7_out.json")
    return 0

if __name__=="__main__": sys.exit(main())
