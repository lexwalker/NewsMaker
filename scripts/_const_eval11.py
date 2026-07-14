"""Eval-gate v11 (jul-14): event_signature stability nudge.
(a) Direct signature checks on multi-model/show cases (the MG-Goodwood class);
(b) verdict-drift gate (pub/sec) on the full regression set."""
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
O1='''  • model: canonical model, lowercase, no brand prefix ("type 01",
    "skm m7", "coolray"). Several models in ONE event -> list them ALL,
    space-separated, alphabetical ("cyber go!") — never "" when specific
    models are named; "" only when the news names no specific model at all.'''
N1='''  • model: canonical model, lowercase, no brand prefix ("type 01",
    "skm m7", "coolray"). "" if the news is not about one model.'''
O2="""      motorshow       a show's line-up as such; a specific model's
                      debut AT a show is 'reveal', not motorshow"""
N2="""      motorshow       multi-model line-up at a show"""
BASE = CAND.replace(O1,N1,1).replace(O2,N2,1)
assert BASE != CAND

SIG_CASES = [
 {"key":"MG two concepts at Goodwood","title":"EN: MG revealed GO! and Cyber electric concepts at Goodwood Festival of Speed (EN)","lid":"MG unveiled two electric concepts, the GO! and the Cyber, at the Goodwood Festival of Speed.","want_brand":"mg","want_models":("go","cyber"),"want_type":("reveal","motorshow")},
 {"key":"MG Go single angle","title":"EN: MG revealed the Go! concept as rival to Mini and Renault 5 (EN)","lid":"MG revealed the Go! concept, a future rival to the Mini and Renault 5, with production planned for 2027.","want_brand":"mg","want_models":("go",),"want_type":("reveal","motorshow")},
 {"key":"BMW two cars at festival","title":"EN: New BMW X5 and 7 Series show up together at film festival (EN)","lid":"BMW brought the new X5 and the 7 Series to a film festival appearance.","want_brand":"bmw","want_models":("x5",),"want_type":None},
 {"key":"solid-state standards","title":"EN: China publishes new solid-state battery standards: 120 kWh (EN)","lid":"China published new national solid-state battery standards taking effect in December.","want_brand":None,"want_models":None,"want_type":("regulation","tech")},
]

ITEMS = json.loads((ROOT/"data/_const_testset.json").read_text(encoding="utf-8"))
ITEMS += json.loads((ROOT/"data/_const_fresh_cases.json").read_text(encoding="utf-8"))

def matches(pub, sec, it):
    if pub != it["exp_pub"]: return False
    if not it["exp_pub"] or not it["exp_sec"]: return True
    return (sec or "") == it["exp_sec"]

def main():
    client=make_llm_client(get_settings()); client.model="claude-sonnet-4-6"
    secs=load_sections()
    print("=== (a) signature stability, CANDIDATE, 2 samples each ===")
    base.EDITORIAL_REVIEW_SYSTEM = CAND
    sig_ok=0; sig_n=0
    for c in SIG_CASES:
        for s in range(2):
            r,_=client.editorial_review(title=c["title"], body=c["lid"], sections=secs, portal_country="Russia")
            es=r.event_signature
            b=(es.brand if es else "") or ""; m=(es.model if es else "") or ""; ty=(es.event_type if es else "") or ""
            checks=[]
            if c["want_brand"]: checks.append(c["want_brand"] in b)
            if c["want_models"]: checks.append(all(w in m for w in c["want_models"]) and m.strip()!="")
            if c["want_type"]: checks.append(ty in c["want_type"])
            ok=all(checks) if checks else True
            sig_n+=1; sig_ok+=ok
            print(f"  [{'OK ' if ok else 'BAD'}] {c['key'][:30]:30} -> brand={b!r} model={m!r} type={ty!r}")
    print(f"signature checks: {sig_ok}/{sig_n}")

    print("\n=== (b) verdict drift gate ===")
    def run_all(prompt,label):
        base.EDITORIAL_REVIEW_SYSTEM=prompt
        out={}
        for it in ITEMS:
            try:
                r,_=client.editorial_review(title=it["title"], body=it["lid"] or it["title"], sections=secs, portal_country="Russia")
                out[it["key"]]=(r.should_publish, r.section or "")
            except Exception as e:
                print(f"  ! {label} {it['key'][:20]}: {type(e).__name__}")
                out[it["key"]]=(None,"")
        base.EDITORIAL_REVIEW_SYSTEM=CAND
        return out
    bl=run_all(BASE,"base"); b=sum(1 for it in ITEMS if matches(*bl[it["key"]],it))
    cd=run_all(CAND,"cand"); c=sum(1 for it in ITEMS if matches(*cd[it["key"]],it))
    fixes=[it["key"] for it in ITEMS if matches(*cd[it["key"]],it) and not matches(*bl[it["key"]],it)]
    regs=[it["key"] for it in ITEMS if matches(*bl[it["key"]],it) and not matches(*cd[it["key"]],it)]
    print(f"baseline {b}/{len(ITEMS)}  candidate {c}/{len(ITEMS)}  net {len(fixes)-len(regs):+d}")
    print("fixes:",fixes); print("regs:",regs)
    return 0

if __name__=="__main__": sys.exit(main())
