"""Stability re-probe of eval9's 3 regs (2x per prompt)."""
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
spec = Path("scripts/_const_eval9.py").read_text(encoding="utf-8")
BLOCK = spec.split("BLOCK = '''")[1].split("'''")[0]
BASE = CAND.replace("\n" + BLOCK, "\n", 1); assert len(BASE)<len(CAND)
ALL = json.loads((ROOT/"data/_const_testset.json").read_text(encoding="utf-8"))
ALL += json.loads((ROOT/"data/_const_fresh_cases.json").read_text(encoding="utf-8"))
REGS=['AGR halted Solaris production at former','UAZ launches galvanization process (RU)']
items=[it for it in ALL if any(it['key'].startswith(k[:40]) for k in REGS)]
client=make_llm_client(get_settings()); client.model="claude-sonnet-4-6"
secs=load_sections()
def one(prompt,it):
    base.EDITORIAL_REVIEW_SYSTEM=prompt
    r,_=client.editorial_review(title=it["title"],body=it["lid"] or it["title"],sections=secs,portal_country="Russia")
    base.EDITORIAL_REVIEW_SYSTEM=CAND
    return (r.should_publish, r.section or "")
for it in items:
    print(f"\n{it['key'][:52]} exp=({it['exp_pub']},{it.get('exp_sec','')!r})")
    print("  base:", [one(BASE,it) for _ in range(2)])
    print("  cand:", [one(CAND,it) for _ in range(2)])
