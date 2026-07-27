"""Eval-gate v13 (jul-27): RF new-regions block. Baseline = prompt minus the
block; Sonnet. Single rule, editor-verbatim, per маятник discipline."""
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
BLOCK = ''' - RF NEW-REGIONS administration: any measure, law, registration procedure or
   programme scoped to the new Russian regions (ДНР/ЛНР, Запорожская и
   Херсонская области, «новые регионы РФ») -> REJECT whatever the subject.
   Editor (27.07): "Совфед одобрил закон об упрощённой регистрации ТС в новых
   регионах" -> «такое не нужно, не пишем про новые регионы РФ». This rejects
   ONLY items scoped to those regions and reroutes nothing else — every other
   story keeps whatever route the remaining rules give it.
'''
BASE = CAND.replace("\n" + BLOCK, "\n", 1)
assert len(BASE) < len(CAND), "block not found"

ITEMS = json.loads((ROOT / "data/_const_testset.json").read_text(encoding="utf-8"))
ITEMS += json.loads((ROOT / "data/_const_fresh_cases.json").read_text(encoding="utf-8"))
ITEMS += [
 # targets — the editor's own case + near variants of the same class
 {"key": "T1 new-regions registration law",
  "title": "EN: Federation Council approved law simplifying vehicle registration in new regions (RU)",
  "lid": "Совет Федерации одобрил закон, упрощающий регистрацию транспортных средств в новых регионах России. Документ вступит в силу с сентября.",
  "exp_pub": False, "exp_sec": ""},
 {"key": "T2 new-regions plates programme",
  "title": "EN: Vehicle re-registration programme extended in DPR and LPR (RU)",
  "lid": "Программу перерегистрации автомобилей в ДНР и ЛНР продлили до конца года, сообщили в профильном ведомстве.",
  "exp_pub": False, "exp_sec": ""},
 {"key": "T3 new-regions subsidy",
  "title": "EN: Car-loan subsidy launched for residents of Zaporizhzhia region (RU)",
  "lid": "Для жителей Запорожской области запустили программу льготного автокредитования на отечественные модели.",
  "exp_pub": False, "exp_sec": ""},
 # guards — federal RF rules and normal Local stories must NOT flip
 {"key": "G1 federal registration rule",
  "title": "EN: Russia simplified vehicle registration procedure nationwide (RU)",
  "lid": "В России упростили процедуру регистрации транспортных средств: теперь поставить машину на учёт можно через портал госуслуг за один визит.",
  "exp_pub": True, "exp_sec": "Local specifics"},
 {"key": "G2 RF market stats",
  "title": "EN: Russian new-car market grew 12% in June 2026 (RU)",
  "lid": "Рынок новых легковых автомобилей в России вырос на 12% в июне 2026 года, подсчитали в АВТОСТАТе.",
  "exp_pub": True, "exp_sec": "Local specifics"},
 {"key": "G3 RF local production start",
  "title": "EN: Production of the new Tenet A8 sedan started in Russia (RU)",
  "lid": "На заводе в Санкт-Петербурге стартовало производство седана Tenet A8 — первые машины уже отгружены дилерам.",
  "exp_pub": True, "exp_sec": "Local specifics"},
 {"key": "G4 regional EV stats",
  "title": "EN: EV sales grew in several Russian regions in H1 2026 (RU)",
  "lid": "Продажи электромобилей выросли в ряде регионов России в первом полугодии 2026 года, свидетельствуют данные аналитиков.",
  "exp_pub": True, "exp_sec": "Local specifics"},
]
WATCH = {k: "" for k in ["T1 new-regions registration law", "T2 new-regions plates programme",
                          "T3 new-regions subsidy", "G1 federal registration rule",
                          "G2 RF market stats", "G3 RF local production start",
                          "G4 regional EV stats"]}


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
    print("fixes:", fixes); print("regs:", regs)
    print("\nWATCH:")
    for k in WATCH:
        print(f"  {k[:34]:34} {bl.get(k)} -> {cd.get(k)}")
    (ROOT / "data/_const_eval13_out.json").write_text(json.dumps(
        {"baseline": b, "cand": c, "net": len(fixes)-len(regs), "fixes": fixes, "regs": regs},
        ensure_ascii=False, indent=1), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
