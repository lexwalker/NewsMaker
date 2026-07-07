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


# --- _cluster_official_primary: a member's authoritative primary is surfaced
#     as the cluster primary instead of the aggregator canonical article URL.
#     Regression for the Cadillac recall (we detected the NHTSA PDF but the
#     multi-source cluster showed thedrive.com).

def _m(domain, primary_dom="", primary_url="", primary_conf="", brand="") -> dict:
    return {
        "domain": domain,
        "primary_dom": primary_dom,
        "primary_url": primary_url,
        "primary_conf": primary_conf,
        "title": "Cadillac recalls Vistiq EVs",
        "launch_brand_model": brand,
    }


def _off(members, press=frozenset()):
    return bnc._cluster_official_primary(members, press_release_hosts=press)


def test_nhtsa_primary_rescued_from_aggregator_canonical() -> None:
    pdf = "https://static.nhtsa.gov/odi/rcl/2026/RCRIT-26V394-0938.pdf"
    grp = [
        _m("thedrive.com", primary_dom="static.nhtsa.gov",
           primary_url=pdf, primary_conf="high"),
        _m("electrek.co", primary_dom="electrek.co",
           primary_url="https://electrek.co/x", primary_conf="low"),
    ]
    assert _off(grp) == (pdf, "static.nhtsa.gov")


def test_oem_press_primary_rescued() -> None:
    url = "https://media.ford.com/content/x.html"
    grp = [_m("carscoops.com", primary_dom="media.ford.com",
              primary_url=url, primary_conf="high", brand="ford explorer")]
    assert _off(grp, press={"media.ford.com"}) == (url, "media.ford.com")


def test_junk_primary_not_rescued() -> None:
    # a linked tweet at HIGH conf must NOT override the canonical (the original
    # protection the multi-cluster rule relied on)
    grp = [_m("autonews.ru", primary_dom="twitter.com",
              primary_url="https://twitter.com/x/status/1", primary_conf="high")]
    assert _off(grp) is None


def test_journalistic_primary_not_rescued() -> None:
    # a preferred JOURNALISTIC primary (tier 3) is not what the editor asks for
    # — it must not override the canonical
    grp = [_m("naavtotrasse.ru", primary_dom="carscoops.com",
              primary_url="https://carscoops.com/2026/x", primary_conf="high")]
    assert _off(grp) is None


def test_low_conf_primary_not_rescued() -> None:
    # reveal aggregator ceiling: nobody resolved a high-conf official primary
    grp = [_m("motortrend.com", primary_dom="motortrend.com",
              primary_url="https://motortrend.com/z", primary_conf="low")]
    assert _off(grp) is None
