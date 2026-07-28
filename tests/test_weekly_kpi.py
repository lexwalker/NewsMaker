"""Weekly KPI core — the single matcher + 4 honest metrics. Pins the
strict-matching contract so the weekly numbers can't silently inflate."""

from datetime import date

from news_agent.core.weekly_kpi import (
    Item, build_index, match, coverage, precision_and_section,
    reject_right, _section_eq, active_collection_days, coverage_by_day,
    coverage_day_aligned,
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


def test_active_collection_days() -> None:
    # June 9 had a real prog (25 articles), June 10 only a trickle (3) → not a
    # prog day; June 13 a prog (40).
    dates = ([date(2026, 6, 9)] * 25 + [date(2026, 6, 10)] * 3
             + [date(2026, 6, 13)] * 40)
    active = active_collection_days(dates, min_per_day=20)
    assert active == {date(2026, 6, 9), date(2026, 6, 13)}


def test_coverage_day_aligned_excludes_no_prog_days() -> None:
    coll = build_index([Item("Geely Coolray facelift revealed officially")])
    dated_pubs = [
        (date(2026, 6, 9), Item("Geely Coolray facelift revealed in China")),  # active, hit
        (date(2026, 6, 9), Item("BYD Han EV brand-new unrelated story here")),  # active, miss
        (date(2026, 6, 10), Item("Lada Iskra sales started somewhere today")),  # NO prog day
    ]
    active = {date(2026, 6, 9)}     # prog ran only the 9th
    cov = coverage_day_aligned(dated_pubs, active, coll)
    assert cov["total"] == 2          # only the two June-9 pubs counted
    assert cov["hit"] == 1
    assert cov["excluded_no_prog"] == 1   # the June-10 pub set aside (uptime)
    assert cov["active_days"] == 1


def test_coverage_by_day_marks_prog_gaps() -> None:
    coll = build_index([Item("Geely Coolray facelift revealed officially")])
    dated_pubs = [
        (date(2026, 6, 9), Item("Geely Coolray facelift revealed in China")),
        (date(2026, 6, 10), Item("Lada Iskra sales started somewhere today")),
    ]
    rows = coverage_by_day(dated_pubs, {date(2026, 6, 9)}, coll)
    assert {r["date"]: r["prog"] for r in rows} == {
        "2026-06-09": True, "2026-06-10": False}
    jun9 = next(r for r in rows if r["date"] == "2026-06-09")
    assert jun9["found"] == 1 and jun9["pubs"] == 1


def test_section_eq() -> None:
    assert _section_eq("Confirmed", "confirmed")
    assert _section_eq("Dealer news / Promo", "Dealer news/promo")
    assert not _section_eq("Confirmed", "Rumors")
    assert not _section_eq("", "Confirmed")


# --- jul-21 coverage-miss audit fixes ---------------------------------------

def test_numero_and_dash_fold_matches_ds_case() -> None:
    """«…№7 SUV - Elysee» vs «DS N°7 Elysee…»: the numero forms differed AND
    normalise_title's source-suffix peeler ate «- Elysee…». Both fixed."""
    from news_agent.core.weekly_kpi import Item, build_index, match
    ours = [Item(title="Президентский электрокроссовер DS N°7 Elysee: удлиненная колесная база, броня и гидропневматика")]
    pub = Item(title="DS introduced special version of electric №7 SUV - Elysee in France",
               title_alt="DS представила спецверсию электрокроссовера №7 - Elysee")
    m, how, _ = match(pub, build_index(ours))
    assert m and how == "fuzzy"


def test_family_bridge_matches_gwm_haval_greatwall() -> None:
    """Editor says «Haval … GWM H10», source says «Great Wall H10» — one
    family, metric-only bridge + shared model anchor h10."""
    from news_agent.core.weekly_kpi import Item, build_index, match
    ours = [Item(title="Новый внедорожник Great Wall H10 доступен к заказу: он больше и мощнее Dargo")]
    pub = Item(title="Haval announced start of collecting orders for GWM H10 SUV in China",
               title_alt="Хавейл открыл приём заказов на внедорожник GWM H10")
    m, how, _ = match(pub, build_index(ours))
    assert m


def test_anchor_does_not_glue_different_models() -> None:
    from news_agent.core.weekly_kpi import Item, build_index, match
    ours = [Item(title="Новый внедорожник Great Wall H10 доступен к заказу")]
    pub = Item(title="Haval certified the H9 SUV in Russia", title_alt="Хавейл сертифицировал H9")
    m, _, _ = match(pub, build_index(ours))
    assert not m


def test_denominator_exclusions() -> None:
    from news_agent.core.weekly_kpi import Item, is_editor_own_content, is_routine_brief
    assert is_editor_own_content(Item(title="Deepal S07 test-drive", section="Test-drive"))
    assert is_editor_own_content(Item(title="Toyota Camry test-drive (RU)"))
    assert not is_editor_own_content(Item(title="Camry crash test results", url="https://x/y"))
    assert is_routine_brief(Item(title="Oil prices (USD): Brent 86,08/ WTI 79,76 (RU)"))
    assert is_routine_brief(Item(title="Central Bank increased USD rate on July 15 to 77,49 RUB (RU)"))
    assert not is_routine_brief(Item(title="Central Bank of Brazil approved car loan reform"))


# --- event-type discriminator (jul-28 audit: anchor proved "same car",
# not "same story" — ~1/3 of anchored matches paired different events) ---

def test_event_conflict_separates_different_events_on_one_model() -> None:
    from news_agent.core.weekly_kpi import _event_conflict
    # editor ran the spy shots, we had the sales start — different stories
    assert _event_conflict(["refreshed hybrid gac s7 suv spied in china"],
                           "start prodazh obnovlennogo krossovera gac s7")
    # pre-orders vs an interior reveal
    assert _event_conflict(["leapmotor started collecting pre-orders for the a05"],
                           "leapmotor a05 pokazal salon")


def test_event_conflict_silent_on_same_event_reworded() -> None:
    from news_agent.core.weekly_kpi import _event_conflict
    assert not _event_conflict(["buick started collecting pre-orders for electra l7"],
                               "buick opens orders for electra l7 bev in china")
    assert not _event_conflict(["sollers s9 suv got vehicle type approval in russia"],
                               "vnedorozhniki sollers s9 i jac js9 poluchili otts")
    assert not _event_conflict(["nio sold 130 thousandth 3rd gen es8 suv"],
                               "v kitae prodali 130 tysyachnyi krossover nio es8")


def test_event_conflict_silent_when_a_side_has_no_marker() -> None:
    # The proven undercount fixes (H10, DS N°7) carry no type word — they
    # must keep matching.
    from news_agent.core.weekly_kpi import _event_conflict
    assert not _event_conflict(["haval gwm h10 suv"], "great wall h10 krossover")
    assert not _event_conflict(["ds n 7 elysee"], "ds n 7 elysee in france")
