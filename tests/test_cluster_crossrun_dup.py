"""Package-2 dedup fixes (jul-2026 architecture review).

1. A cross-run dup verdict from the LLM-editor (is_cross_run_dup) must be
   CARRIED into the article's llm_reason as the standard "возможно дубль"
   hint — it was computed and paid for, then discarded, so a story the model
   recognised as already-pushed sailed into the clean feed.
2. The _unknown bucket must be sorted by normalised title before chunking so
   same-topic no-brand dups (the утильсбор case: vesti + kommersant on one
   FCS announcement, far apart in sheet order) land in the SAME chunk where
   the LLM can compare them.
"""

import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import news_agent.core.llm_editor as le  # noqa: E402
import build_news_clusters as bnc  # noqa: E402


def _art(row: int, title: str) -> dict:
    return {
        "sheet_row": row, "title": title, "lede": "l",
        "section": "Other news", "url": f"https://x.ru/{row}",
        "normalised": bnc._normalise(title), "llm_reason": "",
        "domain": "x.ru", "pub_dt": None, "launch_brand_model": "",
        "primary_url": "", "primary_conf": "",
        "event_brand": "", "event_model": "", "event_type": "",
    }


def _run_pass(monkeypatch):
    calls: list[tuple[str, list[int]]] = []

    def stub(*, brand, articles, history=None, confidence_threshold=0.7):
        calls.append((brand, [a["row"] for a in articles]))
        r = types.SimpleNamespace(cost_usd=0.0, elapsed_s=0.0, error=None,
                                  events=[])
        if 2 in [a["row"] for a in articles]:
            r.events = [types.SimpleNamespace(
                event_id="E1", member_rows=[2], primary_row=2, summary="s",
                section="", is_cross_run_dup=True,
                cross_run_match_url="https://old.example/prev-story")]
        return r

    # cluster_group is imported LOCALLY inside _apply_llm_editor_pass, so the
    # source module attribute is the one to patch.
    monkeypatch.setattr(le, "cluster_group", stub)

    fillers = [f"market stat number {i} rose again" for i in range(12)]
    arts = (
        [_art(2, "FCS clarified recycling fee mechanism for imported cars")]
        + [_art(10 + i, t) for i, t in enumerate(fillers[:6])]
        + [_art(30, "FCS explained recycling fee recheck for imported cars")]
        + [_art(40 + i, t) for i, t in enumerate(fillers[6:])]
    )
    groups = [[a] for a in arts]
    _, stats = bnc._apply_llm_editor_pass(groups, arts)
    return arts, calls, stats


def test_cross_run_dup_verdict_becomes_divert_hint(monkeypatch) -> None:
    arts, _, stats = _run_pass(monkeypatch)
    assert stats["cross_run_dups"] == 1
    target = next(a for a in arts if a["sheet_row"] == 2)
    # the push diverts on this exact substring (_LLM_DUP_RE)
    assert "возможно дуб" in target["llm_reason"].lower()
    assert "old.example" in target["llm_reason"]
    # PREPENDED — the [:400] cap can never amputate it
    assert target["llm_reason"].startswith("возможно дубль")


def test_unknown_bucket_sorted_so_same_topic_co_chunks(monkeypatch) -> None:
    _, calls, _ = _run_pass(monkeypatch)
    # rows 2 and 30 are far apart in sheet order but lexically adjacent
    # ("fcs …") — sorting must put them into the same LLM chunk.
    assert any(2 in rows and 30 in rows for _, rows in calls), \
        "утильсбор-pair still split across chunks"


def test_pair_passes_llm_merge_corroboration() -> None:
    a = _art(2, "FCS clarified recycling fee mechanism for imported cars")
    b = _art(30, "FCS explained recycling fee recheck for imported cars")
    assert bnc._llm_merge_corroborated(a, b)  # fuzz ~83 >= floor 50
