"""Unit tests for the NHTSA recalls adapter (pure functions + fetch filter).

No network: fetch_recent_recalls is exercised against a stub client that
returns canned Socrata rows, so the recall_type filter and field mapping
are tested deterministically.
"""

from datetime import datetime, timezone

from news_agent.adapters.fetchers import nhtsa_recalls as nr


# A Socrata row shaped like the real ODI dataset (6axg-epim).
HYUNDAI = {
    "manufacturer": "Hyundai Motor America",
    "subject": "Instrument Panel Display Failure/FMVSS 101",
    "recall_type": "Vehicle",
    "potentially_affected": "96310",
    "report_received_date": "2026-06-24T00:00:00.000",
    "defect_summary": "Hyundai is recalling certain 2025-2026 Tucson vehicles.",
    "consequence_summary": "An instrument panel display that fails may obscure the speedometer.",
    "corrective_action": "Dealers will update the software, free of charge.",
    "recall_link": {"url": "https://www.nhtsa.gov/recalls?nhtsaId=26V400000",
                    "description": "Go to Recall"},
    "nhtsa_id": "26V400000",
}


def test_is_recalls_source() -> None:
    assert nr.is_recalls_source(nr.RECALLS_ENDPOINT)
    assert nr.is_recalls_source("https://data.transportation.gov/resource/x.json")
    assert not nr.is_recalls_source("https://kolesa.ru")
    assert not nr.is_recalls_source("")


def test_format_title_with_units_and_subject_cleanup() -> None:
    # legal suffix trimmed, /FMVSS citation dropped, units thousands-grouped
    assert nr.format_title(HYUNDAI) == (
        "Hyundai Motor America recalls 96,310 vehicles: Instrument Panel Display Failure"
    )


def test_format_title_without_units() -> None:
    rec = {"manufacturer": "Acme, Inc.", "subject": "Brake Hose May Leak",
           "potentially_affected": None}
    assert nr.format_title(rec) == "Acme issues recall: Brake Hose May Leak"


def test_format_title_zero_units_treated_as_unknown() -> None:
    rec = {"manufacturer": "Acme", "subject": "X", "potentially_affected": "0"}
    assert nr.format_title(rec).startswith("Acme issues recall")


def test_format_body_concatenates_then_falls_back() -> None:
    body = nr.format_body(HYUNDAI)
    assert "Tucson" in body and "speedometer" in body and "software" in body
    # no summaries → fall back to the title so the LLM/cluster never see ""
    bare = {"manufacturer": "Acme", "subject": "X", "potentially_affected": "5"}
    assert nr.format_body(bare) == nr.format_title(bare)


def test_recall_url_prefers_link_then_id() -> None:
    assert nr.recall_url(HYUNDAI) == "https://www.nhtsa.gov/recalls?nhtsaId=26V400000"
    # no recall_link → build from nhtsa_id
    assert nr.recall_url({"nhtsa_id": "26V399000"}) == (
        "https://www.nhtsa.gov/recalls?nhtsaId=26V399000"
    )
    assert nr.recall_url({}) == ""


def test_recall_to_article_maps_all_fields() -> None:
    art = nr.recall_to_article(HYUNDAI)
    assert art is not None
    assert art.url == "https://www.nhtsa.gov/recalls?nhtsaId=26V400000"
    assert art.source_name == "NHTSA"
    assert art.source_language == "en"
    assert art.source_url == nr.RECALLS_ENDPOINT
    assert art.published_at == datetime(2026, 6, 24, tzinfo=timezone.utc)
    assert art.title.startswith("Hyundai Motor America recalls 96,310")


def test_recall_to_article_none_without_url() -> None:
    assert nr.recall_to_article({"manufacturer": "X", "subject": "Y"}) is None


class _StubResp:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def raise_for_status(self) -> None:  # pragma: no cover - trivial
        pass

    def json(self) -> list[dict]:
        return self._rows


class _StubClient:
    """Minimal httpx.Client stand-in capturing the request params."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.last_params: dict | None = None

    def get(self, url, *, params=None, timeout=None, follow_redirects=None):
        self.last_params = params
        return _StubResp(self._rows)


def test_fetch_filters_non_vehicle_recall_types() -> None:
    rows = [
        HYUNDAI,                                              # Vehicle → kept
        {**HYUNDAI, "recall_type": "Tire", "nhtsa_id": "26T001",
         "recall_link": {"url": "https://x/26T001"}},        # Tire → dropped
        {**HYUNDAI, "recall_type": "Equipment", "nhtsa_id": "26E001",
         "recall_link": {"url": "https://x/26E001"}},        # Equipment → dropped
    ]
    client = _StubClient(rows)
    arts = nr.fetch_recent_recalls(
        client, lookback_days=10, limit=50,
        now=datetime(2026, 6, 30, tzinfo=timezone.utc),
    )
    assert len(arts) == 1
    assert arts[0].title.startswith("Hyundai")


def test_fetch_builds_dated_where_clause() -> None:
    client = _StubClient([])
    nr.fetch_recent_recalls(
        client, lookback_days=10,
        now=datetime(2026, 6, 30, tzinfo=timezone.utc),
    )
    where = client.last_params["$where"]
    assert where == "report_received_date > '2026-06-20T00:00:00'"
    assert client.last_params["$order"] == "report_received_date DESC"


class _TextResp:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:  # pragma: no cover - trivial
        pass


class _ReportStubClient:
    """Returns a safetyIssues-like payload embedding the RCLRPT PDF url."""

    def __init__(self, text: str = "", raise_exc: bool = False) -> None:
        self._text = text
        self._raise = raise_exc

    def get(self, url, *, params=None, timeout=None, follow_redirects=None):
        if self._raise:
            raise RuntimeError("network down")
        return _TextResp(self._text)


def test_recall_report_url_extracted_from_safety_issues() -> None:
    # The editor-provided example: the -NNNN suffix is not derivable, only
    # discoverable via the safetyIssues payload.
    payload = ('{"results":[{"associatedDocuments":"'
               'https://static.nhtsa.gov/odi/rcl/2026/RCLRPT-26V403-8025.pdf"}]}')
    url = nr.fetch_recall_report_url(_ReportStubClient(payload), "26V403000")
    assert url == "https://static.nhtsa.gov/odi/rcl/2026/RCLRPT-26V403-8025.pdf"


def test_recall_report_url_degrades_to_empty() -> None:
    assert nr.fetch_recall_report_url(_ReportStubClient("{}"), "26V999000") == ""
    assert nr.fetch_recall_report_url(_ReportStubClient(raise_exc=True), "26V1") == ""
    assert nr.fetch_recall_report_url(_ReportStubClient("x"), "") == ""


def test_recall_to_article_carries_report_pdf_in_outbound_links() -> None:
    pdf = "https://static.nhtsa.gov/odi/rcl/2026/RCLRPT-26V400-5456.pdf"
    art = nr.recall_to_article(HYUNDAI, report_url=pdf)
    assert art is not None
    assert art.outbound_links == [pdf]
    # campaign URL stays the article/dedup URL
    assert art.url == "https://www.nhtsa.gov/recalls?nhtsaId=26V400000"
    # without a report the links stay empty (campaign URL remains primary)
    art2 = nr.recall_to_article(HYUNDAI)
    assert art2 is not None and art2.outbound_links == []
