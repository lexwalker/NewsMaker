"""Eval-gate v10 (jul-14): ОСАГО/procedure-mechanics + factory-tour rejects."""
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
START = " - RF driver-procedure / insurance MECHANICS changes"
END = " - already covered / stale"
i0 = CAND.index(START); i1 = CAND.index(END)
BLOCK = CAND[i0:i1]
BASE = CAND[:i0] + CAND[i1:]

ITEMS = json.loads((ROOT/"data/_const_testset.json").read_text(encoding="utf-8"))
ITEMS += json.loads((ROOT/"data/_const_fresh_cases.json").read_text(encoding="utf-8"))
ITEMS += [
 {"key":"T1 CTP repair rules","title":"EN: New CTP repair expense rules take effect on July 11 (RU)","lid":"С 11 июля в России начнут действовать новые правила определения расходов на восстановительный ремонт по ОСАГО.","exp_pub":False,"exp_sec":""},
 {"key":"T2 double penalty abolished","title":"EN: Russia abolished double punishment for drivers (RU)","lid":"В России отменили двойное наказание водителей за одно нарушение — соответствующая процедура вступила в силу.","exp_pub":False,"exp_sec":""},
 {"key":"T3 July-1 digest","title":"EN: CTP and licenses: what changes for Russian drivers from July 1 (RU)","lid":"ОСАГО, водительские права и техосмотр: обзор всех изменений для российских водителей с 1 июля.","exp_pub":False,"exp_sec":""},
 {"key":"T4 Jeland plant tour","title":"EN: Inside the Jeland plant in Shushary: Drom reportage (RU)","lid":"Автопроизводство вернулось в Шушары. Репортаж с завода Jeland: как устроены цеха и что происходит на конвейере.","exp_pub":False,"exp_sec":""},
 {"key":"T5 AvtoVAZ QA step","title":"EN: AvtoVAZ introduced engine pressure leak testing at 1 atm (RU)","lid":"На АвтоВАЗе внедрили проверку герметичности моторов под давлением 1 атмосфера на конвейере.","exp_pub":False,"exp_sec":""},
 # guards
 {"key":"G1 Duma fine increase","title":"EN: State Duma proposes 1.5x increase in traffic fines (RU)","lid":"В Госдуме предложили в 1,5 раза увеличить штрафы за опасное вождение — законопроект внесён.","exp_pub":True,"exp_sec":""},
 {"key":"G2 fine statistics","title":"EN: How much Russian drivers spend on fines: statistics (RU)","lid":"Статистика: сколько российские водители тратят на штрафы в год, данные по регионам.","exp_pub":True,"exp_sec":""},
 {"key":"G3 scrappage fee market-wide","title":"EN: Russia raises утильсбор rates for imported cars from October (RU)","lid":"Правительство утвердило повышение ставок утильсбора на импортируемые автомобили с октября — затронет весь рынок.","exp_pub":True,"exp_sec":""},
 {"key":"G4 Avtotor new line","title":"EN: Avtotor launched welding line for SWM vehicle production (RU)","lid":"Автотор запустил сварочную линию для производства автомобилей SWM — старт производства новой марки.","exp_pub":True,"exp_sec":""},
]
WATCH={k:"" for k in ["T1 CTP repair rules","T2 double penalty abolished","T3 July-1 digest","T4 Jeland plant tour","T5 AvtoVAZ QA step","G1 Duma fine increase","G2 fine statistics","G3 scrappage fee market-wide","G4 Avtotor new line"]}

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
    print(f"items {len(ITEMS)} model {client.model}  block_chars={len(BLOCK)}")
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
    for k in WATCH: print(f"  {k[:30]:30} {bl.get(k)} -> {cd.get(k)}")
    (ROOT/"data/_const_eval10_out.json").write_text(json.dumps(
        {"baseline":b,"cand":c,"net":len(fixes)-len(regs),"fixes":fixes,"regs":regs},
        ensure_ascii=False, indent=1), encoding="utf-8")
    return 0

if __name__=="__main__": sys.exit(main())
