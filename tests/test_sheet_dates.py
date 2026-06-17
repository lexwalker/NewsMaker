"""The one date parser must read all three encodings the archive uses."""

from datetime import timezone

from news_agent.core.sheet_dates import parse_sheet_date


def test_iso() -> None:
    d = parse_sheet_date("2026-06-04 17:50")
    assert d is not None and (d.year, d.month, d.day) == (2026, 6, 4)
    assert d.tzinfo is not None


def test_iso_date_only() -> None:
    d = parse_sheet_date("2026-06-15")
    assert d is not None and d.day == 15


def test_us_format() -> None:
    d = parse_sheet_date("05/26/2026 17:06:00")
    assert d is not None and (d.month, d.day, d.year) == (5, 26, 2026)


def test_excel_serial_float() -> None:
    # 46177.x falls in June 2026 — this is the case the old parser dropped.
    d = parse_sheet_date(46177.72)
    assert d is not None and d.year == 2026 and d.month == 6


def test_excel_serial_string() -> None:
    d = parse_sheet_date("46177")
    assert d is not None and d.year == 2026


def test_unparseable() -> None:
    assert parse_sheet_date("") is None
    assert parse_sheet_date(None) is None
    assert parse_sheet_date("not a date") is None
    assert parse_sheet_date(5) is None        # too small to be a serial


def test_excel_serial_matches_known_date() -> None:
    # Well-known anchor: Sheets/Excel serial 44197 == 2021-01-01.
    d = parse_sheet_date(44197)
    assert (d.year, d.month, d.day) == (2021, 1, 1)
