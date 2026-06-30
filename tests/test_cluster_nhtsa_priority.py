"""NHTSA recall campaigns win canonical absolutely.

The editor designated NHTSA as THE source for US recalls. When a recall also
surfaces via blogs / OEM pages and clusters with them, the official campaign
must be the canonical (primary) member so the story is attributed to NHTSA
and not collapsed into a secondary source. _cluster_priority ranks nhtsa.gov
above tier-0 press-release hosts.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import build_news_clusters as bnc  # noqa: E402


def _a(domain: str, *, primary_dom: str = "", day: int = 24) -> dict:
    return {
        "domain": domain,
        "primary_dom": primary_dom,
        "pub_dt": datetime(2026, 6, day, tzinfo=timezone.utc),
        "title": "Hyundai recalls 96,310 vehicles",
        "launch_brand_model": "",
    }


def _prio(article: dict, *, press=frozenset(), wl=frozenset()):
    return bnc._cluster_priority(article, press_release_hosts=press, whitelist=wl)


def test_nhtsa_ranks_below_zero() -> None:
    assert _prio(_a("www.nhtsa.gov"))[0] == -1
    assert _prio(_a("nhtsa.gov"))[0] == -1
    # also recognised via the primary-source domain
    assert _prio(_a("motor1.com", primary_dom="nhtsa.gov"))[0] == -1


def test_nhtsa_beats_blog_and_press_release() -> None:
    nhtsa = _prio(_a("nhtsa.gov"))
    blog = _prio(_a("motor1.com"))
    oem_press = _prio(_a("media.hyundai.com"), press={"media.hyundai.com"})
    assert nhtsa < blog
    assert nhtsa < oem_press  # beats even tier-0 press-release hosts


def test_nhtsa_is_canonical_in_mixed_cluster() -> None:
    grp = [_a("motor1.com"), _a("nhtsa.gov"), _a("media.hyundai.com")]
    canonical = sorted(
        grp, key=lambda a: _prio(a, press={"media.hyundai.com"})
    )[0]
    assert canonical["domain"] == "nhtsa.gov"


def test_non_nhtsa_unaffected() -> None:
    # a plain blog still gets a normal positive tier (regression guard)
    assert _prio(_a("motor1.com"))[0] >= 0
