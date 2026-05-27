"""Production validation: did LLM-editor catch what the editor flagged?

For each new editor verdict on v42 (dup, wrong_primary) — check:
  • DUP complaint: would LLM-editor have merged these rows in its
    cluster? If yes, the complaint never reaches the editor.
  • WRONG_PRIMARY: would LLM-editor have picked the OEM URL the editor
    cited? Tier-based check on the new cluster's primary_url.

Compares editor's verdicts (synced today via sync_editor_feedback)
against the LLM-editor cluster output (data/clusters_*_v42.json).
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from news_agent.core.brand_canonical import canonicalize_brand  # noqa: E402
from news_agent.core.source_priority import domain_tier  # noqa: E402

V42_LLM = ROOT / "data" / "clusters_ТЕСТ_статьи_v42.json"
V42_BASELINE = ROOT / "data" / "clusters_ТЕСТ_статьи_v42_baseline.json"
EVAL_V2 = ROOT / "data" / "eval_set_v2.jsonl"

# Editor's verdicts are on rows in the sheet 'Новости (новые)' tab.
# That sheet has all pushes mixed; we filter v42 rows by sync date.
# v42 was pushed at "25.05.2026 09:22 UTC". Sync state captures all
# new entries since the previous sync.


def _load_recent_verdicts(min_synced_at: str = "2026-05-26") -> list[dict]:
    """Return v2 entries synced today (after morning sync) — these are
    the v42 reactions."""
    out = []
    for ln in EVAL_V2.read_text("utf-8").splitlines():
        if not ln.strip():
            continue
        try:
            r = json.loads(ln)
        except Exception:
            continue
        if r.get("synced_at", "") >= min_synced_at:
            out.append(r)
    return out


def _build_cluster_lookup(clusters: list[dict]) -> dict[str, dict]:
    """url → cluster dict. Speeds up "which cluster is this row in"."""
    m = {}
    for c in clusters:
        for member in c.get("members", []):
            m[member["url"]] = c
    return m


def main() -> int:
    new_verdicts = _load_recent_verdicts()
    print(f"New editor verdicts since morning sync: {len(new_verdicts)}\n")

    llm_clusters = json.loads(V42_LLM.read_text("utf-8"))
    baseline_clusters = json.loads(V42_BASELINE.read_text("utf-8"))
    llm_url = _build_cluster_lookup(llm_clusters)
    base_url = _build_cluster_lookup(baseline_clusters)

    # ── DUP analysis ────────────────────────────────────────────
    dup_complaints = [r for r in new_verdicts
                      if r.get("label_dup_within") or
                      r.get("label_dup_cross_run")]
    print(f"━━ DUP complaints: {len(dup_complaints)}\n")

    llm_caught_dup = 0
    llm_missed_dup = 0
    base_already_caught = 0
    for v in dup_complaints:
        url = v.get("url", "")
        title = v.get("title", "")[:60].replace("\n", " | ")
        # Was the article's cluster a multi-source one in LLM output?
        # I.e., did LLM-editor merge it with at least one other source?
        llm_c = llm_url.get(url)
        base_c = base_url.get(url)
        if llm_c is None:
            print(f"  ?  {title!s:60}  (not in v42 LLM clusters — older push?)")
            continue
        llm_merged = llm_c["size"] >= 2
        base_merged = base_c["size"] >= 2 if base_c else False
        if base_merged:
            tag = "✓both (lexical already had)"
            base_already_caught += 1
        elif llm_merged:
            tag = "✓LLM-editor caught (lexical missed)"
            llm_caught_dup += 1
        else:
            tag = "✗ still singleton in LLM output"
            llm_missed_dup += 1
        print(f"  {tag:42}  {title}")
        if v.get("editor_comment"):
            print(f"        ред: «{v['editor_comment'][:90]}»")

    print(f"\n  Dup-recall summary:")
    print(f"    lexical already caught:    {base_already_caught}")
    print(f"    LLM-editor caught extra:   {llm_caught_dup}")
    print(f"    still missed (no merge):   {llm_missed_dup}")
    total = base_already_caught + llm_caught_dup + llm_missed_dup
    if total:
        improved = base_already_caught + llm_caught_dup
        print(f"    coverage: lexical {base_already_caught}/{total} "
              f"= {base_already_caught*100/total:.0f}% → "
              f"LLM-editor {improved}/{total} = {improved*100/total:.0f}%")

    # ── WRONG_PRIMARY analysis ───────────────────────────────────
    wp_complaints = [r for r in new_verdicts if r.get("label_wrong_primary")]
    print(f"\n━━ WRONG_PRIMARY complaints: {len(wp_complaints)}\n")

    fixed = unfixed = no_cluster = 0
    for v in wp_complaints:
        url = v.get("url", "")
        title = v.get("title", "")[:60].replace("\n", " | ")
        editor_refs = v.get("referenced_urls", [])
        if not editor_refs:
            print(f"  -  no editor ref to compare: {title}")
            continue
        ed_dom = urlparse(editor_refs[0]).netloc.lower().lstrip("www.")

        llm_c = llm_url.get(url)
        if llm_c is None:
            print(f"  ?  {title}  (not in v42 LLM clusters)")
            no_cluster += 1
            continue

        llm_primary_dom = llm_c.get("primary_domain", "").lower()
        if not llm_primary_dom:
            llm_primary_dom = urlparse(
                llm_c.get("primary_url", "") or llm_c.get("canonical_url", "")
            ).netloc.lower()
        llm_primary_dom = llm_primary_dom.lstrip("www.")

        # Get the article's brand for tier computation
        brand = canonicalize_brand(title + " " + v.get("body", "")[:200])
        ed_tier = domain_tier(ed_dom, brand_canonical=brand)
        llm_tier = domain_tier(llm_primary_dom, brand_canonical=brand)

        # FIXED = LLM's primary is at same or better tier as editor's ref
        if ed_dom == llm_primary_dom:
            tag = "✓ exact match"
            fixed += 1
        elif llm_tier <= ed_tier:
            tag = f"✓ equal tier ({llm_tier} vs {ed_tier})"
            fixed += 1
        else:
            tag = f"✗ worse tier (llm={llm_tier} vs ed={ed_tier})"
            unfixed += 1
        print(f"  {tag:38}  {title}")
        print(f"        bot picked: {llm_primary_dom:30}  tier={llm_tier}")
        print(f"        editor ref: {ed_dom:30}  tier={ed_tier}")

    print(f"\n  Wrong-primary fix summary: "
          f"{fixed}/{fixed+unfixed} fixed ({fixed*100/(fixed+unfixed) if (fixed+unfixed) else 0:.0f}%)"
          f", {no_cluster} not in cluster set")

    return 0


if __name__ == "__main__":
    sys.exit(main())
