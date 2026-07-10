"""Re-probe the 5 eval8 'regs': stable block-caused or coin-flip noise?"""
import json, sys, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src"))
from dotenv import load_dotenv; load_dotenv(ROOT/".env", override=True)
import news_agent.adapters.llm.base as base
from news_agent.adapters.llm import make_llm_client
from news_agent.core.config_loader import load_sections
from news_agent.settings import get_settings

CAND = base.EDITORIAL_REVIEW_SYSTEM
import importlib
spec = Path("scripts/_const_eval8.py").read_text(encoding="utf-8")
# reuse BLOCK from eval8
BLOCK = spec.split("BLOCK = '''")[1].split("'''")[0]
BASE = CAND.replace("\n" + BLOCK, "\n", 1)
assert len(BASE) < len(CAND)

ALL = json.loads((ROOT/"data/_const_testset.json").read_text(encoding="utf-8"))
ALL += json.loads((ROOT/"data/_const_fresh_cases.json").read_text(encoding="utf-8"))
REGS = ['AvtoVAZ designed T-134 crossover with On','AvtoVAZ expanded LADA Iskra wagon line-u',
        'Atom electric vehicle passed final crash','Exeed LX and TXL SUVs exit Russian marke',
        'AGR plant in Shushary capable of produci']
items=[it for it in ALL if any(it['key'].startswith(k[:38]) for k in REGS)]
print("probing", len(items), "items x2 per prompt")
client=make_llm_client(get_settings()); client.model="claude-sonnet-4-6"
secs=load_sections()
def one(prompt, it):
    base.EDITORIAL_REVIEW_SYSTEM=prompt
    r,_=client.editorial_review(title=it["title"], body=it["lid"] or it["title"], sections=secs, portal_country="Russia")
    base.EDITORIAL_REVIEW_SYSTEM=CAND
    return (r.should_publish, r.section or "")
for it in items:
    exp=(it["exp_pub"], it.get("exp_sec",""))
    b=[one(BASE,it) for _ in range(2)]
    c=[one(CAND,it) for _ in range(2)]
    print(f"\n  {it['key'][:44]} exp={exp}")
    print(f"    base: {b}")
    print(f"    cand: {c}")
