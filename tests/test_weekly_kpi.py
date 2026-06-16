"""Weekly KPI core — the single matcher + 4 honest metrics. Pins the
strict-matching contract so the weekly numbers can't silently inflate."""

from news_agent.core.weekly_kpi import (
    Item, build_index, match, coverage, precision_and_section,
    reject_right, _section_eq,
)


def test_url_exact_match() -> None:
    idx = build_index([Item("anything", url="https://moex.com/n/1", section="Economics")])
    m, method, _ = match(Item("totally different headline here",
                               url="https://moex.com/n/1?utm=x"), idx)
    assert m and method == "url"


def test_brand_gated_fuzzy_match() -> None:
    idx = build_index([Item("Geely Coolray facelift officially revealed", section="Confirmed")])
    m, method, sec = match(Item("Geely Coolray facelift revealed in China"), idx)
    assert m and method == "fuzzy" and sec == "Confirmed"


def test_cross_brand_blocked() -> None:
    idx = build_index([Item("Geely X5 facelift officially revealed today")])
    m, _method, _ = match(Item("BMW X5 facelift officially revealed today"), idx)
    assert not m   # same phrasing, different brand → no match


def test_short_title_not_matched() -> None:
    idx = build_index([Item("5", section="x")])         # degenerate
    m, _method, _ = match(Item("Central Bank raised the key rate to 21 percent"), idx)
    assert not m


def test_coverage() -> None:
    coll = build_index([
        Item("Geely Coolray facelift revealed", url="https://drom.ru/a"),
        Item("Lada Iskra sales started in Russia"),
    ])
    pubs = [
        Item("Geely Coolray facelift officially revealed"),   # fuzzy hit
        Item("Lada Iskra sales started", url="https://drom.ru/a"),  # url hit
        Item("BYD Han EV totally new unrelated launch story"),      # miss
    ]
    c = coverage(pubs, coll)
    assert c["hit"] == 2 and c["total"] == 3
    assert abs(c["rate"] - 2/3) < 1e-9


def test_precision_and_section() -> None:
    archive = build_index([
        Item("Geely Coolray facelift revealed", section="Confirmed"),
        Item("Voyah Free Sport debut in China", section="Other news"),
    ])
    accepted = [
        Item("Geely Coolray facelift revealed now", section="Confirmed"),   # match + sec ok
        Item("Voyah Free Sport debut in China today", section="Rumors"),    # match + sec wrong
        Item("Some unmatched brand-new story about nothing", section="Confirmed"),  # not published
    ]
    r = precision_and_section(accepted, archive, {})
    assert r["found_right"]["hit"] == 2 and r["found_right"]["total"] == 3
    assert r["section_right"]["hit"] == 1 and r["section_right"]["total"] == 2


def test_reject_right_proxy() -> None:
    archive = build_index([Item("Geely Coolray facelift revealed", section="Confirmed")])
    rejected = [
        Item("Geely Coolray facelift revealed again"),   # editor DID publish → false reject
        Item("Buy a tire cheap clickbait listicle thing"),  # not published → correct reject
        Item("How to wash your car in five easy steps"),     # not published → correct reject
    ]
    r = reject_right(rejected, archive)
    assert r["hit"] == 2 and r["false_rejects"] == 1
    assert r["is_proxy"] is True


def test_section_eq() -> None:
    assert _section_eq("Confirmed", "confirmed")
    assert _section_eq("Dealer news / Promo", "Dealer news/promo")
    assert not _section_eq("Confirmed", "Rumors")
    assert not _section_eq("", "Confirmed")
