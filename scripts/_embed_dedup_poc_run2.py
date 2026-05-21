"""PoC iteration #2: brand-prefilter + calibrated thresholds.

Findings from run #1:
  • MiniLM on short titles (no lede) yields cosines 0.5-0.75, never >0.85
  • Best matches are often WRONG brand (topic-similar, not story-similar)
  • → both threshold AND candidate filtering need work

Run #2 changes:
  • Brand prefilter: only consider history with same brand as candidate.
    This kills 90%+ of the noise.
  • Calibrate thresholds for THIS model (MiniLM) by sweeping.
  • Also try: URL canonical-host match (cross-domain re-host detection).

If brand-prefiltered match still doesn't work, we need either:
  (a) richer text (re-crawl lede), or
  (b) a stronger encoder (BGE-M3 ~2GB, takes 2 GB RAM but multilingual
      and known to beat MiniLM by ~10pp on short text).
"""

from __future__ import annotations

import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

V41_FILE = DATA / "embed_poc_v41.jsonl"
HIST_FILE = DATA / "embed_poc_history.jsonl"
EMB_CACHE = DATA / "embed_poc_vectors.npz"

# Brand list (same as run1)
_BRANDS = [
    "toyota", "honda", "nissan", "mazda", "subaru", "suzuki",
    "mitsubishi", "lexus", "infiniti", "bmw", "mercedes",
    "mercedes-benz", "mercedes-amg", "audi", "porsche", "volkswagen",
    "vw", "opel", "skoda", "seat", "cupra", "ford", "chevrolet",
    "cadillac", "gmc", "dodge", "chrysler", "jeep", "ram", "buick",
    "lincoln", "fiat", "alfa romeo", "lancia", "maserati", "ferrari",
    "lamborghini", "bentley", "rolls-royce", "aston martin",
    "mclaren", "volvo", "jaguar", "land rover", "range rover", "mini",
    "peugeot", "citroen", "renault", "dacia", "kia", "hyundai",
    "genesis", "ssangyong", "kgm", "lada", "uaz", "gaz", "kamaz",
    "sollers", "aurus", "moskvich", "volga", "solaris", "xcite",
    "avtovaz", "haval", "great wall", "gwm", "geely", "chery",
    "exeed", "jaecoo", "omoda", "tank", "changan", "dongfeng", "faw",
    "baic", "jac", "jetour", "livan", "maxus", "foton", "sitrak",
    "sany", "byd", "nio", "xpeng", "li auto", "leapmotor", "seres",
    "aito", "huawei", "xiaomi", "denza", "voyah", "wey", "ora",
    "polestar", "mg", "roewe", "maextro", "vinfast", "tata",
    "mahindra", "tesla", "lucid", "rivian", "stellantis", "lotus",
    "alpina", "alpine",
]
_BRANDS = sorted(set(_BRANDS), key=len, reverse=True)
_BR_RX = {b: re.compile(r"(?:^|[^a-zа-я])" + re.escape(b) +
                        r"(?:[^a-zа-я]|$)") for b in _BRANDS}


def find_brand(text: str) -> str:
    nt = re.sub(r"\s+", " ", text.lower())
    for b, rx in _BR_RX.items():
        if rx.search(nt):
            return b
    return ""


def parse_ts(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _load_jsonl(p: Path) -> list[dict]:
    out = []
    for ln in p.read_text("utf-8").splitlines():
        if not ln.strip():
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    return out


def main() -> int:
    v41 = _load_jsonl(V41_FILE)
    hist = _load_jsonl(HIST_FILE)
    print(f"v41={len(v41)}, hist={len(hist)}")

    # Load cached vectors (no re-encoding needed)
    if not EMB_CACHE.exists():
        print("vectors not cached — run _embed_dedup_poc_run.py first")
        return 1
    d = np.load(EMB_CACHE)
    v41_vec = d["v41"]
    hist_vec = d["hist"]
    print(f"loaded vectors v41={v41_vec.shape}, hist={hist_vec.shape}")

    # Per-row brand
    for r in v41:
        r["_brand"] = find_brand(r["title"] + " " + r["lede"][:200])
    for r in hist:
        r["_brand"] = (r.get("event_brand") or find_brand(r["title"]))

    # Time window mask
    v41_anchor = datetime(2026, 5, 20, 21, 23, tzinfo=timezone.utc)
    window_lo = v41_anchor.timestamp() - 14 * 86400
    hist_ts = np.array([
        (parse_ts(r["ts"]).timestamp() if parse_ts(r["ts"]) else 0.0)
        for r in hist
    ])
    in_window = hist_ts >= window_lo

    # Index history by brand for fast prefilter
    by_brand: dict[str, list[int]] = {}
    for i, r in enumerate(hist):
        b = r["_brand"]
        if b and in_window[i]:
            by_brand.setdefault(b, []).append(i)
    print(f"history brands with >=1 entry in window: {len(by_brand)}")
    print(f"top-10 brand sizes:", sorted(
        {b: len(idx) for b, idx in by_brand.items()}.items(),
        key=lambda x: -x[1])[:10])

    # For each v41 row, brand-filter then top cosine
    out = []
    for i, r in enumerate(v41):
        b = r["_brand"]
        candidate_ix = by_brand.get(b, [])
        # Also expand to brand aliases ("vw" ↔ "volkswagen", etc.)
        alias_map = {"vw": "volkswagen", "volkswagen": "vw",
                     "mercedes": "mercedes-benz", "mercedes-benz": "mercedes"}
        if b in alias_map:
            candidate_ix = list(set(candidate_ix +
                                     by_brand.get(alias_map[b], [])))
        if not candidate_ix:
            out.append({**r, "best_cos": 0.0, "best_idx": -1,
                        "n_cands": 0})
            continue
        cand_vec = hist_vec[candidate_ix]
        sims = cand_vec @ v41_vec[i]
        order = np.argsort(sims)[::-1]
        top = []
        for k in order[:3]:
            j = candidate_ix[int(k)]
            top.append({"cos": float(sims[int(k)]), "idx": j,
                        "title": hist[j]["title"][:100],
                        "domain": hist[j]["domain"],
                        "ts": hist[j]["ts"]})
        out.append({**r, "best_cos": top[0]["cos"],
                    "best_idx": top[0]["idx"],
                    "matches": top, "n_cands": len(candidate_ix)})

    # SCORECARD with brand-prefilter
    print("\n=== SCORECARD (brand-prefilter + cosine) ===\n")
    print(f"{'thresh':>7} {'TP':>4} {'FP':>4} {'TN':>4} {'FN':>4} "
          f"{'prec':>6} {'rec':>6} {'F1':>6}")
    best = None
    for th in (0.50, 0.55, 0.60, 0.625, 0.65, 0.68, 0.70, 0.72, 0.75):
        tp = fp = tn = fn = 0
        for r in out:
            if r["editor_label"] == "dup_amb":
                continue
            pred_dup = (r.get("n_cands", 0) > 0 and r["best_cos"] >= th)
            true_dup = r["editor_label"] == "dup_yes"
            if pred_dup and true_dup: tp += 1
            elif pred_dup and not true_dup: fp += 1
            elif not pred_dup and not true_dup: tn += 1
            else: fn += 1
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        print(f"  {th:.3f} {tp:>4} {fp:>4} {tn:>4} {fn:>4} "
              f"{prec:>6.1%} {rec:>6.1%} {f1:>6.1%}")
        if best is None or f1 > best[1]:
            best = (th, f1, tp, fp, tn, fn)
    print(f"\nbest threshold: {best[0]:.3f} (F1={best[1]:.1%})")

    # Per-row diff at best threshold
    th_opt = best[0]
    print(f"\n=== EDITOR-LABELED DUPS — at threshold {th_opt:.2f} ===\n")
    for r in sorted(
        (x for x in out if x["editor_label"] == "dup_yes"),
        key=lambda x: -x["best_cos"],
    ):
        pred = "DUP" if r["best_cos"] >= th_opt else "miss"
        flag = "✓" if pred == "DUP" else "✗"
        print(f"{flag} r{r['row']:>3} cos={r['best_cos']:.3f} "
              f"({r['n_cands']:>3} cands) | {r['_brand']:14} | "
              f"{r['title'][:65].replace(chr(10),' | ')}")
        if r.get("matches"):
            m = r["matches"][0]
            print(f"     → {m['domain'][:25]:25} {m['ts'][:10]} | "
                  f"{m['title'][:70]}")

    # NOT-DUPS that fired (FPs)
    print(f"\n=== NOT-DUP rows that fired (false positives) ===\n")
    for r in sorted(
        (x for x in out if x["editor_label"] == "dup_no"
         and x["best_cos"] >= th_opt),
        key=lambda x: -x["best_cos"],
    )[:15]:
        print(f"  r{r['row']:>3} cos={r['best_cos']:.3f} | "
              f"{r['_brand']:14} | "
              f"{r['title'][:65].replace(chr(10),' | ')}")
        if r.get("matches"):
            m = r["matches"][0]
            print(f"     → {m['domain'][:25]:25} {m['ts'][:10]} | "
                  f"{m['title'][:70]}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
