"""Google Sheets adapter: read sources/news/sections, append rows."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from news_agent.core.config_loader import SourcesSchema
from news_agent.core.models import FewShotExample, OutputRow, SectionDefinition, Source
from news_agent.logging_setup import get_logger

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
log = get_logger("sheets")


def _truthy(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    return str(val).strip().lower() in {"1", "true", "yes", "y", "да", "истина", "on"}


class SheetsClient:
    """Thin wrapper around the Google Sheets v4 API."""

    def __init__(self, spreadsheet_id: str, service_account_path: Path) -> None:
        if not service_account_path.exists():
            raise FileNotFoundError(
                f"Service account JSON not found at {service_account_path}. "
                "Set GOOGLE_SERVICE_ACCOUNT_JSON in .env."
            )
        creds = Credentials.from_service_account_file(
            str(service_account_path), scopes=SCOPES
        )
        self._svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
        self.spreadsheet_id = spreadsheet_id

    # ------------------------------------------------------------------ read
    def get_values(self, tab: str, range_suffix: str = "") -> list[list[str]]:
        a1 = f"'{tab}'{range_suffix}"
        try:
            resp = (
                self._svc.spreadsheets()
                .values()
                .get(spreadsheetId=self.spreadsheet_id, range=a1)
                .execute()
            )
        except HttpError as e:
            log.warning("sheets.read.failed", tab=tab, error=str(e))
            return []
        return cast(list[list[str]], resp.get("values", []))

    def read_sources(self, tab: str, schema: SourcesSchema) -> list[Source]:
        rows = self.get_values(tab)
        if not rows:
            log.warning("sheets.sources.empty", tab=tab)
            return []
        header = [h.strip() for h in rows[0]]
        idx = {name: i for i, name in enumerate(header)}

        def col(row: list[str], key: str) -> str:
            i = idx.get(key)
            if i is None or i >= len(row):
                return ""
            return row[i].strip()

        out: list[Source] = []
        for row in rows[1:]:
            if not any(row):
                continue
            name = col(row, schema.name)
            url = col(row, schema.url)
            if not url:
                continue
            raw_type = (col(row, schema.type) or "html").lower()
            stype = "rss" if raw_type == "rss" else "html"
            lang = col(row, schema.language) if schema.language else ""
            out.append(
                Source(
                    name=name or url,
                    url=url,
                    type=stype,
                    is_active=_truthy(col(row, schema.is_active) or "true"),
                    language=lang or None,
                )
            )
        return out

    def read_sections(self, tab: str) -> list[SectionDefinition]:
        rows = self.get_values(tab)
        if not rows or len(rows) < 2:
            return []
        header = [h.strip().lower() for h in rows[0]]
        name_idx = header.index("name") if "name" in header else 0
        desc_idx = header.index("description") if "description" in header else 1
        out: list[SectionDefinition] = []
        for row in rows[1:]:
            if not row or not row[name_idx].strip():
                continue
            desc = row[desc_idx].strip() if desc_idx < len(row) else ""
            out.append(SectionDefinition(name=row[name_idx].strip(), description=desc))
        return out

    def read_few_shots(self, tab: str, max_per_section: int = 3) -> list[FewShotExample]:
        """Pull (title, section) pairs from the curated `News` tab."""
        rows = self.get_values(tab)
        if not rows or len(rows) < 2:
            return []
        header = [h.strip().lower() for h in rows[0]]

        def find(*candidates: str) -> int | None:
            for c in candidates:
                if c in header:
                    return header.index(c)
            return None

        title_i = find("name", "title", "название")
        section_i = find("section", "раздел")
        region_i = find("region", "регион")
        if title_i is None or section_i is None:
            return []

        by_section: dict[str, list[FewShotExample]] = {}
        for row in rows[1:]:
            if not row:
                continue
            title = row[title_i].strip() if title_i < len(row) else ""
            section = row[section_i].strip() if section_i < len(row) else ""
            if not title or not section:
                continue
            region_val: Any = None
            if region_i is not None and region_i < len(row):
                rv = row[region_i].strip().capitalize()
                if rv in {"Local", "Global"}:
                    region_val = rv
            bucket = by_section.setdefault(section, [])
            if len(bucket) < max_per_section:
                bucket.append(FewShotExample(title=title, section=section, region=region_val))
        out: list[FewShotExample] = []
        for bucket in by_section.values():
            out.extend(bucket)
        return out

    def read_existing_titles(self, tabs: list[str]) -> list[str]:
        """Pull titles from any of the listed tabs (used for fuzzy dedup)."""
        titles: list[str] = []
        for tab in tabs:
            rows = self.get_values(tab)
            if not rows:
                continue
            header = [h.strip().lower() for h in rows[0]]
            candidates = [c for c in ("name", "title", "название") if c in header]
            if not candidates:
                continue
            i = header.index(candidates[0])
            for row in rows[1:]:
                if i < len(row) and row[i].strip():
                    titles.append(row[i].strip())
        return titles

    # ----------------------------------------------------------------- write
    def ensure_output_header(self, tab: str) -> None:
        rows = self.get_values(tab, range_suffix="!1:1")
        if rows and rows[0]:
            return
        self._svc.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [OutputRow.header()]},
        ).execute()
        log.info("sheets.output.header_written", tab=tab)

    def append_rows(self, tab: str, rows: list[OutputRow]) -> int:
        if not rows:
            return 0
        body = {"values": [r.as_row() for r in rows]}
        resp = (
            self._svc.spreadsheets()
            .values()
            .append(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{tab}'!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )
        updates = resp.get("updates", {})
        count = int(updates.get("updatedRows", len(rows)))
        log.info("sheets.output.appended", tab=tab, count=count)
        return count
