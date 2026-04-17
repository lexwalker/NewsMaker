"""Smoke test: dump source counts per portal from the input spreadsheet."""

from __future__ import annotations

from rich.console import Console
from rich.table import Table

from news_agent.adapters.sheets import SheetsClient
from news_agent.core.config_loader import load_sections, load_sources_schema
from news_agent.settings import get_settings


def main() -> None:
    s = get_settings()
    if not s.spreadsheet_id:
        raise SystemExit("SPREADSHEET_ID not set in .env")
    client = SheetsClient(s.spreadsheet_id, s.google_service_account_json)
    schema = load_sources_schema()
    console = Console()

    t = Table(title="Sources per portal")
    t.add_column("Portal")
    t.add_column("Tab")
    t.add_column("Total", justify="right")
    t.add_column("Active", justify="right")
    t.add_column("RSS", justify="right")
    t.add_column("HTML", justify="right")

    for portal in ("RU", "UZ", "KZ"):
        tab = s.sources_tab_for(portal)  # type: ignore[arg-type]
        sources = client.read_sources(tab, schema)
        active = [x for x in sources if x.is_active]
        rss = [x for x in active if x.type == "rss"]
        html = [x for x in active if x.type == "html"]
        t.add_row(portal, tab, str(len(sources)), str(len(active)), str(len(rss)), str(len(html)))

    console.print(t)

    sections_from_sheet = client.read_sections(s.sections_tab)
    sections_from_yaml = load_sections()
    console.print(
        f"\nSections — sheet: {len(sections_from_sheet)}  yaml-fallback: {len(sections_from_yaml)}"
    )
    if sections_from_sheet:
        for sec in sections_from_sheet:
            console.print(f"  • {sec.name}")
    else:
        console.print("  (tab empty — yaml defaults will be used)")


if __name__ == "__main__":
    main()
