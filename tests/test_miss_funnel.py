"""Miss-funnel matching + bucketing (peer-review §3, Week1-C).

Synthetic editor publications matched against a synthetic collected set,
pinning the S1/S2/S3/S4/accepted decomposition the recall diagnosis relies
on.
"""

from news_agent.core.miss_funnel import (
    ACCEPTED,
    S1,
    S2,
    S3,
    S4,
    Collected,
    EditorPub,
    build_funnel,
    summarise,
)


def _pub(title_en="", title_ru="", url="", date="2026-06-01", section="Confirmed"):
    return EditorPub(title_en, title_ru, url, date, section)


def _col(title, verdict, url="", domain="", seen="2026-06-01"):
    return Collected(url, domain, title, seen, verdict)


SOURCES = {"autonews.ru", "drom.ru", "kolesa.ru"}


def test_url_match_takes_verdict_stage() -> None:
    pub = _pub("Geely Coolray facelift revealed", url="https://autonews.ru/a/1")
    col = _col("Geely Coolray facelift", "Отклонено LLM",
               url="https://autonews.ru/a/1", domain="autonews.ru")
    rows = build_funnel([pub], [col], SOURCES)
    assert rows[0].match_method == "url"
    assert rows[0].stage == S4  # LLM reject


def test_fuzzy_match_by_title_and_brand() -> None:
    pub = _pub("Geely Coolray facelift officially revealed in China")
    col = _col("Geely Coolray facelift revealed", "Точно не новость (чёрный список)",
               domain="drom.ru")
    rows = build_funnel([pub], [col], SOURCES)
    assert rows[0].match_method == "fuzzy"
    assert rows[0].stage == S3
    assert rows[0].cause == "blacklist"


def test_brand_gate_blocks_cross_brand_match() -> None:
    # near-identical phrasing, different brand → must NOT match
    pub = _pub("BMW X5 facelift officially revealed")
    col = _col("Geely X5 facelift officially revealed", "Точно новость",
               domain="drom.ru")
    rows = build_funnel([pub], [col], SOURCES, brand_gate=True)
    # no same-brand collected → unmatched → S1/S2 not ACCEPTED
    assert rows[0].stage in (S1, S2)
    assert rows[0].match_method == "none"


def test_unmatched_source_not_in_list_is_s1() -> None:
    pub = _pub("Some obscure launch", url="https://unknown-blog.com/x")
    rows = build_funnel([pub], [], SOURCES)
    assert rows[0].stage == S1
    assert rows[0].cause == "source_not_in_list"


def test_unmatched_source_in_list_is_s2() -> None:
    pub = _pub("A story we should have caught", url="https://autonews.ru/missed")
    rows = build_funnel([pub], [], SOURCES)
    assert rows[0].stage == S2
    assert rows[0].cause == "not_collected"


def test_accepted_when_collected_and_published_verdict() -> None:
    pub = _pub("Voyah Free Sport plus debuts")
    col = _col("Voyah Free Sport+ debuts", "Точно новость", domain="kolesa.ru")
    rows = build_funnel([pub], [col], SOURCES)
    assert rows[0].stage == ACCEPTED
    assert rows[0].cause == "accepted"


def test_dedup_collapse_is_not_a_miss() -> None:
    pub = _pub("Lada Iskra sales start")
    col = _col("Lada Iskra sales start", "Отклонить (дубль финального URL)",
               domain="drom.ru")
    rows = build_funnel([pub], [col], SOURCES)
    assert rows[0].stage == ACCEPTED
    assert rows[0].cause == "dedup_collapse"


def test_no_url_pub_is_s2_no_source_url() -> None:
    pub = _pub("Civic news with no link", url="")
    rows = build_funnel([pub], [], SOURCES)
    assert rows[0].stage == S2
    assert rows[0].cause == "no_source_url"


def test_short_string_trap_rejected() -> None:
    """A long editor title must NOT match a degenerate 1-token collected
    title (emoji / bare word) just because token_set_ratio hits 100."""
    pub = _pub("Central Bank increased USD rate on June 5 to 74 RUB")
    junk = _col("5", "Точно не новость (чёрный список)", domain="drom.ru")
    sedan = _col("Седан", "Отклонено LLM", domain="auto.ru")
    rows = build_funnel([pub], [junk, sedan], SOURCES)
    assert rows[0].match_method == "none"
    assert rows[0].stage in (S1, S2)


def test_legit_short_title_still_matches() -> None:
    """A real 4-token title must still match (guard isn't over-tight)."""
    pub = _pub("Lada Iskra sales start in Russia")
    col = _col("Lada Iskra sales start", "Точно новость", domain="drom.ru")
    rows = build_funnel([pub], [col], SOURCES)
    assert rows[0].match_method == "fuzzy"
    assert rows[0].stage == ACCEPTED


def test_summarise_aggregates() -> None:
    pubs = [
        _pub("Geely Coolray revealed", url="https://unknown.com/1"),       # S1
        _pub("Missed on autonews", url="https://autonews.ru/2"),            # S2
    ]
    rows = build_funnel(pubs, [], SOURCES)
    s = summarise(rows)
    assert s["total"] == 2
    assert s["stages"].get(S1) == 1
    assert s["stages"].get(S2) == 1
    assert "unknown.com" in s["s1_domains"]
    assert "autonews.ru" in s["s2_domains"]
    assert s["methods"].get("none") == 2
