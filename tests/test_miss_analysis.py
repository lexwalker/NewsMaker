"""Miss-analysis sheet builder — funnel rows → 'Непокрытые (анализ)' table."""

from news_agent.core.miss_funnel import (
    S1, S2, S3, S4, ACCEPTED, EditorPub, FunnelRow,
)
from news_agent.core.miss_analysis import (
    misses, domains_to_analyse, deterministic_rec, build_sheet_rows, HEADER,
)


def _pub(title, url, section="Confirmed", date="2026-06-10"):
    return EditorPub(title_en=title, title_ru="", url=url, date=date, section=section)


def _row(stage, cause, pub, method="none", score=0.0, matched=""):
    return FunnelRow(pub=pub, stage=stage, cause=cause, match_method=method,
                     matched_title=matched, score=score)


def test_misses_drops_accepted() -> None:
    rows = [
        _row(S1, "source_not_in_list", _pub("A", "https://x.ru/1")),
        _row(ACCEPTED, "accepted", _pub("B", "https://y.ru/2")),
    ]
    assert len(misses(rows)) == 1
    assert misses(rows)[0].stage == S1


def test_domains_to_analyse_groups_s1_s2_only() -> None:
    rows = [
        _row(S1, "source_not_in_list", _pub("A1", "https://drom.ru/1")),
        _row(S1, "source_not_in_list", _pub("A2", "https://drom.ru/2")),
        _row(S2, "not_collected", _pub("B1", "https://motor.ru/1")),
        _row(S3, "blacklist", _pub("C1", "https://z.ru/1")),  # excluded
        _row(S4, "llm", _pub("D1", "https://w.ru/1")),         # excluded
    ]
    groups = domains_to_analyse(rows)
    keys = {(g["stage"], g["domain"]) for g in groups}
    assert keys == {(S1, "drom.ru"), (S2, "motor.ru")}
    drom = next(g for g in groups if g["domain"] == "drom.ru")
    assert drom["count"] == 2 and len(drom["titles"]) == 2


def test_deterministic_rec_per_stage() -> None:
    assert "нет в списке" in deterministic_rec(
        _row(S1, "source_not_in_list", _pub("A", "https://drom.ru/1")))
    assert "RSS" in deterministic_rec(
        _row(S2, "not_collected", _pub("B", "https://motor.ru/1")))
    assert "Эвристика" in deterministic_rec(
        _row(S3, "blacklist:колесо", _pub("C", "https://z.ru/1")))
    assert "разметку" in deterministic_rec(
        _row(S4, "llm", _pub("D", "https://w.ru/1")))


def test_build_sheet_rows_uses_ai_rec_when_present() -> None:
    rows = [
        _row(S1, "source_not_in_list", _pub("A", "https://drom.ru/1")),
        _row(S2, "not_collected", _pub("B", "https://motor.ru/1")),
    ]
    recs = {(S1, "drom.ru"): "ИИ: добавить, это крупный портал"}
    out = build_sheet_rows(rows, recs)
    # S1 row first (stage order), AI rec used; S2 falls back to deterministic
    assert out[0][3].startswith("S1") and out[0][8] == "ИИ: добавить, это крупный портал"
    assert out[1][3].startswith("S2") and "RSS" in out[1][8]
    assert len(out[0]) == len(HEADER)


def test_build_sheet_rows_fuzzy_score_column() -> None:
    rows = [_row(S3, "blacklist", _pub("A", "https://z.ru/1"),
                 method="fuzzy", score=82.4, matched="близкая статья")]
    out = build_sheet_rows(rows)
    assert out[0][6] == "82"   # score shown only for fuzzy matches
