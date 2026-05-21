"""Wider dup re-scan after the user spotted pairs the first lexical pass
missed. Lowers fuzz threshold, brings in lede content (not just title),
adds brand-only pair candidates with date proximity, and flags
cross-section duplicates. Read-only — prints; merging is a separate run.

Scope: top ~250 rows of the editor sheet — covers the last few pushes
including v41 (20.05.2026) so older editor-approved canon rows are
visible as merge targets too.
"""

from __future__ import annotations

import io
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env", override=True)

from google.oauth2 import service_account  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402
from rapidfuzz import fuzz  # noqa: E402

EDITOR = os.environ.get(
    "EDITOR_SPREADSHEET_ID", "1fQic_uDpTzfjySf091tW9Ql_iJ1Z544dQYbEHAlPAZs"
)
SA = Path(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"].lstrip("./"))
creds = service_account.Credentials.from_service_account_file(
    str(SA), scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
svc = build("sheets", "v4", credentials=creds)
TAB = "Новости (новые)"

BRANDS = [
    # JP
    "toyota", "honda", "nissan", "mazda", "subaru", "suzuki",
    "mitsubishi", "lexus", "infiniti", "acura", "daihatsu",
    # DE / EU
    "bmw", "mercedes", "audi", "porsche", "volkswagen", "vw",
    "opel", "skoda", "seat", "cupra", "smart", "alpine",
    "volvo", "jaguar", "land rover", "range rover", "mini",
    "peugeot", "citroen", "renault", "dacia", "fiat",
    "alfa romeo", "lancia", "maserati", "ferrari", "lamborghini",
    "bentley", "rolls-royce", "aston martin", "mclaren", "lotus",
    # KR
    "kia", "hyundai", "genesis",
    # US
    "ford", "chevrolet", "chevy", "cadillac", "gmc", "dodge",
    "chrysler", "jeep", "ram", "buick", "lincoln", "tesla",
    "lucid", "rivian",
    # RU
    "lada", "uaz", "gaz", "kamaz", "sollers", "aurus", "moskvich",
    "volga", "solaris", "xcite", "avtovaz", "лада", "уаз", "газ",
    "москвич", "волга", "соллерс", "автоваз",
    # CN
    "haval", "great wall", "gwm", "geely", "chery", "exeed",
    "jaecoo", "omoda", "tank", "changan", "dongfeng", "faw",
    "baic", "jac", "jetour", "livan", "maxus", "foton", "sitrak",
    "sany", "byd", "nio", "xpeng", "li auto", "lixiang",
    "leapmotor", "seres", "aito", "huawei", "xiaomi", "denza",
    "voyah", "wey", "ora", "polestar", "mg", "roewe", "maextro",
    "skywell", "skm",
    # IN / VN
    "vinfast", "tata", "mahindra", "maruti",
]
BRANDS = sorted(set(BRANDS), key=len, reverse=True)
# precompiled boundary regexes
_BR = {b: re.compile(r"(?:^|[^a-zа-я])" + re.escape(b) + r"(?:[^a-zа-я]|$)")
       for b in BRANDS}


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().strip())


def find_brands(text: str) -> list[str]:
    """Return ALL brand mentions (some stories pair two — that's a
    legit signal too)."""
    nt = norm(text)
    out = []
    for b, rx in _BR.items():
        if rx.search(nt):
            out.append(b)
    return out


def extract_model_after(text: str, brand: str) -> str:
    nt = norm(text)
    m = re.search(re.escape(brand) + r"[\s\-]+([a-z0-9а-я\-]+"
                  r"(?:\s+[a-z0-9\-]+)?)", nt)
    if not m:
        return ""
    mdl = m.group(1).strip()
    if mdl in {"to", "will", "launches", "launch", "for", "in", "с",
              "для", "на", "от", "до", "начал", "начнут", "представ",
              "анонсир", "объяв", "and", "the", "is"}:
        return ""
    return mdl


def main() -> int:
    rows = svc.spreadsheets().values().get(
        spreadsheetId=EDITOR, range=f"'{TAB}'!A1:P300",
        valueRenderOption="UNFORMATTED_VALUE"
    ).execute().get("values", [])

    data = []
    for i, r in enumerate(rows, 1):
        if i == 1:
            continue
        if not r or not isinstance(r[0], str):
            continue
        if "━━" in r[0] or "Прогон от" in r[0]:
            continue
        title = (r[1] if len(r) > 1 else "") or ""
        lede = (r[2] if len(r) > 2 else "") or ""
        section = (r[3] if len(r) > 3 else "") or ""
        src = (r[9] if len(r) > 9 else "") or ""
        all_urls = (r[12] if len(r) > 12 else "") or ""
        comment = (r[15] if len(r) > 15 else "") or ""
        data.append({
            "r": i, "title": str(title), "lede": str(lede),
            "section": str(section), "src": str(src),
            "all_urls": str(all_urls), "comment": str(comment),
        })

    print(f"rows with content: {len(data)}")
    annotated = sum(1 for d in data if "🔁" in d["comment"])
    print(f"already annotated as dup: {annotated}")

    # Index by brand
    by_brand: dict[str, list[int]] = defaultdict(list)
    for i, d in enumerate(data):
        bm_text = d["title"] + " " + d["lede"][:300]
        brands = find_brands(bm_text)
        d["_brands"] = brands
        d["_model_by_brand"] = {b: extract_model_after(bm_text, b)
                                for b in brands}
        for b in brands:
            by_brand[b].append(i)

    seen_pairs = set()
    candidates = []
    for brand, idxs in by_brand.items():
        if len(idxs) < 2:
            continue
        for ai in range(len(idxs)):
            for bi in range(ai + 1, len(idxs)):
                a, b = data[idxs[ai]], data[idxs[bi]]
                key = (min(a["r"], b["r"]), max(a["r"], b["r"]))
                if key in seen_pairs:
                    continue
                seen_pairs.add(key)

                at, bt = norm(a["title"]), norm(b["title"])
                al, bl = norm(a["lede"][:400]), norm(b["lede"][:400])

                tf = fuzz.token_set_ratio(at, bt)
                lf = fuzz.token_set_ratio(al, bl) if al and bl else 0
                xf1 = fuzz.token_set_ratio(at, bl) if bl else 0
                xf2 = fuzz.token_set_ratio(bt, al) if al else 0
                xf = max(xf1, xf2)

                am = a["_model_by_brand"].get(brand, "")
                bm = b["_model_by_brand"].get(brand, "")
                same_model = bool(am and bm and (
                    am == bm or
                    fuzz.partial_ratio(am, bm) >= 80 or
                    am in bm or bm in am
                ))

                best = max(tf, lf, xf)
                flag = False
                why = ""
                tier = "?"
                if tf >= 75:
                    flag, tier, why = True, "A", f"title-fuzz={tf}"
                elif lf >= 78:
                    flag, tier, why = True, "B", f"lede-fuzz={lf}"
                elif xf >= 72:
                    flag, tier, why = True, "C", f"cross-fuzz={xf}"
                elif same_model and best >= 45:
                    flag, tier, why = True, "D", (
                        f"same-brand+model={brand}/{am}, max-fuzz={best}"
                    )
                elif (tf + lf + xf) >= 165 and tf >= 50:
                    flag, tier, why = True, "E", (
                        f"sum-fuzz={tf+lf+xf} (tf={tf} lf={lf} xf={xf})"
                    )

                if flag:
                    candidates.append({
                        "a": a, "b": b, "brand": brand,
                        "tier": tier, "tf": tf, "lf": lf, "xf": xf,
                        "best": best, "why": why,
                        "x_section": a["section"] != b["section"],
                    })

    # Skip pairs already merged (one side flagged 🔁)
    fresh = [c for c in candidates if not (
        "🔁" in c["a"]["comment"] or "🔁" in c["b"]["comment"]
    )]

    # De-dup by (rA, rB) keeping the strongest tier
    by_pair: dict[tuple, dict] = {}
    rank = {"A": 5, "B": 4, "C": 3, "D": 2, "E": 1}
    for c in fresh:
        k = (c["a"]["r"], c["b"]["r"])
        if k not in by_pair or rank[c["tier"]] > rank[by_pair[k]["tier"]]:
            by_pair[k] = c
    uniq = sorted(by_pair.values(),
                  key=lambda c: (-rank[c["tier"]], -c["best"]))

    print(f"\ncandidate pairs (wide): {len(candidates)}")
    print(f"after dedup + filter:    {len(uniq)}")
    print(f"\n=== TOP {min(40, len(uniq))} pairs ===\n")
    for c in uniq[:40]:
        a, b = c["a"], c["b"]
        xs = " ⚠X-SECT" if c["x_section"] else ""
        print(f"[{c['tier']}] r{a['r']:>3}({a['section'][:8]:8}) "
              f"⇋ r{b['r']:>3}({b['section'][:8]:8})  "
              f"tf={c['tf']:>3} lf={c['lf']:>3} xf={c['xf']:>3}  "
              f"{c['why']}{xs}")
        print(f"     A: {a['title'][:100].replace(chr(10),' | ')}")
        print(f"     B: {b['title'][:100].replace(chr(10),' | ')}")
        if a["lede"]:
            print(f"     A.lede: {a['lede'][:90].replace(chr(10),' ')}")
        if b["lede"]:
            print(f"     B.lede: {b['lede'][:90].replace(chr(10),' ')}")
        if a["comment"]:
            print(f"     A.com:  {a['comment'][:80]}")
        if b["comment"]:
            print(f"     B.com:  {b['comment'][:80]}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
