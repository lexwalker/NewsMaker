"""LLM-editor merge corroboration guard (jun-2026 prog-v2 review).

The LLM-as-editor over-merged unrelated no-brand items into one "event",
silently dropping publishable stories: a Genesis Magma GT3 concept and an
Avtodor M-11 traffic story were swallowed into an RF auto-loan cluster, and
an Exeed RX refresh was merged into an Exeed EX6 reveal. _llm_merge_corroborated
requires a real shared signal before an LLM merge is applied. These cases are
the exact rows from that run.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import build_news_clusters as bnc  # noqa: E402


def _a(title: str, *, url: str = "", purl: str = "", bm: str = "") -> dict:
    return {"normalised": bnc._normalise(title), "url": url,
            "primary_url": purl, "launch_brand_model": bm}


# Real members from the v2 over-merged clusters.
_LOAN_ASROAD = _a("Car loan statistics in Russia in May 2026 Russian Auto Dealers Association")
_LOAN_5KOLESO = _a("Auto-loan issuance in Russia surged 25% in May")
_GENESIS = _a("Genesis unveiled Magma GT3 concept")
_M11_A = _a("Avtodor denies traffic jams on M-11 Neva highway blames drivers")
_VOYAH = _a("Voyah Free driver saves thousands on Moscow rides weekly cost drops from 3500 to 600 RUB")
_M11_B = _a("No traffic jams on M-11 Avtodor officially denies panic rumors")
_EX6_A = _a("Exeed EX6 crossover unveiled as updated Omoda 9",
            url="https://carnewschina.com/2026/06/17/exeed-ex6-crossover-broke-cover")
_EX6_B = _a("Exeed revealed new EX6 SUV in official images",
            url="https://x.xn--p1ai/2026/06/16/novyj-exeed-ex6/")
_RX = _a("Exeed RX coupe-SUV refreshed design shown in official images",
         url="https://t.me/chinamashina_news/14248")

C = bnc._llm_merge_corroborated


def test_legit_same_story_merges_kept() -> None:
    # Two outlets on the same RF May auto-loan stats — must still merge.
    assert C(_LOAN_ASROAD, _LOAN_5KOLESO)
    # Two outlets on the same Exeed EX6 reveal — must still merge.
    assert C(_EX6_A, _EX6_B)


def test_unrelated_no_brand_items_split() -> None:
    # The over-merges that dropped publishable stories — must NOT merge.
    assert not C(_LOAN_ASROAD, _GENESIS)   # Genesis Magma GT3 concept survives
    assert not C(_LOAN_ASROAD, _M11_A)     # Avtodor M-11 story survives
    assert not C(_VOYAH, _M11_B)           # Voyah vs M-11 (same domain only)


def test_same_brand_different_model_split() -> None:
    # Exeed EX6 reveal vs Exeed RX refresh — different model, must NOT merge.
    assert not C(_EX6_A, _RX)


def test_same_primary_url_always_merges() -> None:
    # The LLM-editor's real value (cross-language / same press release) is
    # preserved: identical primary_url corroborates regardless of wording.
    x = _a("totally different headline X", purl="https://brand.example/press/1")
    y = _a("совсем другой заголовок Y", purl="https://brand.example/press/1")
    assert C(x, y)


def test_same_brand_model_pair_merges() -> None:
    x = _a("Geely Emgrand i-HEV launched in China", bm="geely emgrand")
    y = _a("Sales of the Geely Emgrand hybrid sedan started", bm="geely emgrand")
    assert C(x, y)
