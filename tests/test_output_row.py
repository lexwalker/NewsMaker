from news_agent.core.models import OutputRow


def test_header_matches_spec_columns_count() -> None:
    assert len(OutputRow.header()) == 13


def test_as_row_is_serialisable() -> None:
    row = OutputRow(
        start_date="2026-04-16T10:00:00+00:00",
        section="facts",
        name="Toyota unveils 2026 Camry",
        localized_title="Toyota unveils 2026 Camry / Toyota представила Camry (EN)",
        announcement_image="https://cdn.example.com/x.jpg",
        region="Global",
        country="Russia [7]",
        primary_source_url="https://pressroom.toyota.com/new",
        primary_source_domain="pressroom.toyota.com",
        aggregator_url="https://autoblog.example/camry",
        published_at="2026-04-10T08:30:00+00:00",
        llm_provider="anthropic",
        confidence=0.91,
    )
    assert len(row.as_row()) == 13
    assert row.as_row()[6] == "Russia [7]"
