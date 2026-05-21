"""PoC step 2 — actually run the embedding dedup and score it.

Model: paraphrase-multilingual-MiniLM-L12-v2  (~470MB, 50+ langs, fast)
       — handles RU + EN natively, deterministic cosine.

Pipeline:
  1. Embed v41 (n=52) and history (n=5798) titles+ledes.
  2. For each v41 row, find best cosine match in last 14 days of
     history (most relevant window per editor behaviour).
  3. Decide DUP / NOT DUP via tier rules.
  4. Confusion matrix vs editor verdict, ROC curve over thresholds.

Output: scorecard, per-row diff list, and a recommended threshold.

Why MiniLM and not BGE-M3 / e5-large: PoC. We want the WORST reasonable
multilingual encoder to demonstrate. If MiniLM already hits >80% recall
on editor's DUP labels, a better model only widens the margin.
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
RESULT_FILE = DATA / "embed_poc_result.jsonl"

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
WINDOW_DAYS = 14  # history lookup window for each v41 row


def _norm_text(s: str) -> str:
    s = re.sub(r"\s+", " ", s.lower().strip())
    return s


_BRANDS = [
    "toyota", "honda", "nissan", "mazda", "subaru", "suzuki",
    "mitsubishi", "lexus", "infiniti", "bmw", "mercedes", "audi",
    "porsche", "volkswagen", "vw", "opel", "skoda", "seat", "cupra",
    "ford", "chevrolet", "cadillac", "gmc", "dodge", "chrysler", "jeep",
    "ram", "buick", "lincoln", "fiat", "alfa romeo", "lancia",
    "maserati", "ferrari", "lamborghini", "bentley", "rolls-royce",
    "aston martin", "mclaren", "volvo", "jaguar", "land rover",
    "range rover", "mini", "peugeot", "citroen", "renault", "dacia",
    "kia", "hyundai", "genesis", "ssangyong", "kgm",
    "lada", "uaz", "gaz", "kamaz", "sollers", "aurus", "moskvich",
    "volga", "solaris", "xcite", "avtovaz",
    "haval", "great wall", "gwm", "geely", "chery", "exeed", "jaecoo",
    "omoda", "tank", "changan", "dongfeng", "faw", "baic", "jac",
    "jetour", "livan", "maxus", "foton", "sitrak", "sany", "byd",
    "nio", "xpeng", "li auto", "leapmotor", "seres", "aito", "huawei",
    "xiaomi", "denza", "voyah", "wey", "ora", "polestar", "mg",
    "roewe", "maextro", "vinfast", "tata", "mahindra", "tesla",
    "lucid", "rivian", "stellantis", "lotus", "alpina", "alpine",
]
_BRANDS = sorted(set(_BRANDS), key=len, reverse=True)
_BR_RX = {b: re.compile(r"(?:^|[^a-zа-я])" + re.escape(b) +
                        r"(?:[^a-zа-я]|$)") for b in _BRANDS}


def find_brand(text: str) -> str:
    nt = _norm_text(text)
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
    out, bad = [], 0
    for ln in p.read_text("utf-8").splitlines():
        if not ln.strip():
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            bad += 1
    if bad:
        print(f"  ({p.name}: skipped {bad} malformed line(s))")
    return out


def main() -> int:
    v41 = _load_jsonl(V41_FILE)
    hist = _load_jsonl(HIST_FILE)
    print(f"loaded v41={len(v41)}, hist={len(hist)}")

    # Build embedding input text (RU + EN combined, since v41 titles
    # come in "EN: ...\nRU: ..." form already).
    v41_texts = [
        ((r["title"] or "") + " " + (r["lede"] or "")[:300]).strip()[:600]
        for r in v41
    ]
    hist_texts = [
        ((r["title"] or "") + " " + (r["lede"] or "")[:300]).strip()[:600]
        for r in hist
    ]

    print(f"\nloading model {MODEL_NAME} …")
    t0 = time.time()
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(MODEL_NAME)
    print(f"  loaded in {time.time()-t0:.1f}s")

    # Encode (cache to disk so reruns are instant)
    if EMB_CACHE.exists():
        d = np.load(EMB_CACHE)
        if d["v41"].shape[0] == len(v41) and d["hist"].shape[0] == len(hist):
            v41_vec = d["v41"]
            hist_vec = d["hist"]
            print("loaded cached vectors")
        else:
            v41_vec = hist_vec = None
    else:
        v41_vec = hist_vec = None

    if v41_vec is None:
        t0 = time.time()
        v41_vec = model.encode(v41_texts, batch_size=32,
                               show_progress_bar=False,
                               convert_to_numpy=True,
                               normalize_embeddings=True)
        print(f"  v41 encoded in {time.time()-t0:.1f}s "
              f"({v41_vec.shape})")
        t0 = time.time()
        hist_vec = model.encode(hist_texts, batch_size=64,
                                show_progress_bar=False,
                                convert_to_numpy=True,
                                normalize_embeddings=True)
        print(f"  hist encoded in {time.time()-t0:.1f}s "
              f"({hist_vec.shape})")
        np.savez_compressed(EMB_CACHE, v41=v41_vec, hist=hist_vec)

    # Per-row brand extraction
    for r in v41:
        r["_brand"] = find_brand(r["title"] + " " + r["lede"][:200])
    for r in hist:
        r["_brand"] = (r.get("event_brand") or
                       find_brand(r["title"]))

    # v41 ran at 2026-05-20 21:23 UTC; only match history within
    # WINDOW_DAYS prior.
    v41_anchor = datetime(2026, 5, 20, 21, 23, tzinfo=timezone.utc)
    window_lo = v41_anchor.timestamp() - WINDOW_DAYS * 86400

    hist_ts = np.array([
        (parse_ts(r["ts"]).timestamp() if parse_ts(r["ts"]) else 0.0)
        for r in hist
    ])
    in_window = hist_ts >= window_lo

    # For each v41 row, top-3 cosine matches (within window).
    out_rows = []
    for i, r in enumerate(v41):
        sims = hist_vec @ v41_vec[i]  # cosine since both normalized
        # Mask out-of-window
        sims_w = np.where(in_window, sims, -1.0)
        top3_idx = np.argsort(sims_w)[::-1][:3]
        matches = []
        for j in top3_idx:
            j = int(j)
            if sims_w[j] < 0.3:
                break
            matches.append({
                "cos": float(sims_w[j]),
                "title": hist[j]["title"][:120],
                "url": hist[j]["url"],
                "domain": hist[j]["domain"],
                "ts": hist[j]["ts"],
                "brand": hist[j]["_brand"],
            })
        best = matches[0] if matches else None
        verdict = "no_match"
        if best:
            cb = r["_brand"]
            hb = best["brand"]
            if best["cos"] >= 0.85 and cb and hb and (cb == hb or
                                                       cb in hb or hb in cb):
                verdict = "DUP (T1 brand+sim)"
            elif best["cos"] >= 0.90:  # high cosine even without brand
                verdict = "DUP (T1 high-sim)"
            elif best["cos"] >= 0.80:
                verdict = "MAYBE (below threshold)"
        out_rows.append({
            "row": r["row"],
            "editor_label": r["editor_label"],
            "editor_comment": r["editor_comment"][:120],
            "title": r["title"][:120],
            "brand": r["_brand"],
            "section": r["section"],
            "best_cos": best["cos"] if best else 0.0,
            "verdict": verdict,
            "matches": matches,
        })

    with RESULT_FILE.open("w", encoding="utf-8") as f:
        for r in out_rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Scorecard at multiple thresholds
    print("\n=== SCORECARD: predicted-DUP vs editor verdict ===\n")
    print(f"{'thresh':>7} {'TP':>4} {'FP':>4} {'TN':>4} {'FN':>4} "
          f"{'prec':>6} {'rec':>6} {'F1':>6}")
    for th in (0.70, 0.75, 0.80, 0.825, 0.85, 0.875, 0.90, 0.92, 0.95):
        tp = fp = tn = fn = 0
        for r in out_rows:
            if r["editor_label"] == "dup_amb":
                continue
            best_cos = r["best_cos"]
            cb = r["brand"]
            best = r["matches"][0] if r["matches"] else None
            hb = best["brand"] if best else ""
            brand_match = bool(cb and hb and (cb == hb or cb in hb
                                              or hb in cb))
            # Apply tiered rule at this threshold
            if best_cos >= th and (brand_match or best_cos >= th + 0.05):
                pred = "dup"
            else:
                pred = "no"
            true = "dup" if r["editor_label"] == "dup_yes" else "no"
            if pred == "dup" and true == "dup":
                tp += 1
            elif pred == "dup" and true == "no":
                fp += 1
            elif pred == "no" and true == "no":
                tn += 1
            else:
                fn += 1
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        print(f"  {th:.3f} {tp:>4} {fp:>4} {tn:>4} {fn:>4} "
              f"{prec:>6.1%} {rec:>6.1%} {f1:>6.1%}")

    # Per-row breakdown (DUP-labeled rows first)
    print("\n=== DUP-labeled rows (editor said DUP) ===\n")
    dup_rows = [r for r in out_rows if r["editor_label"] == "dup_yes"]
    for r in sorted(dup_rows, key=lambda x: -x["best_cos"]):
        print(f"r{r['row']:>3} cos={r['best_cos']:.3f} | "
              f"brand={r['brand'] or '—':14} | {r['title'][:78]}")
        if r["editor_comment"]:
            print(f"      ред: {r['editor_comment'][:100]}")
        for m in r["matches"][:1]:
            print(f"      match cos={m['cos']:.3f} | "
                  f"{m['domain'][:25]:25} | {m['title'][:70]}")
            print(f"           ts={m['ts'][:10]} brand={m['brand']}")
        print()

    print("\n=== NOT-DUP rows that fired (false positives) ===\n")
    fp_rows = [r for r in out_rows
               if r["editor_label"] == "dup_no" and r["best_cos"] >= 0.85]
    for r in sorted(fp_rows, key=lambda x: -x["best_cos"])[:20]:
        print(f"r{r['row']:>3} cos={r['best_cos']:.3f} | brand={r['brand']:14} | "
              f"{r['title'][:78]}")
        if r["editor_comment"]:
            print(f"      ред: {r['editor_comment'][:100]}")
        for m in r["matches"][:1]:
            print(f"      match cos={m['cos']:.3f} | "
                  f"{m['domain'][:25]:25} | {m['title'][:70]}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
