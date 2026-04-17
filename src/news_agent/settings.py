"""Runtime settings loaded from environment variables."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

Provider = Literal["anthropic", "openai"]
Portal = Literal["RU", "UZ", "KZ"]

PORTAL_COUNTRY: dict[Portal, tuple[str, int]] = {
    "RU": ("Russia", 7),
    "UZ": ("Uzbekistan", 608),
    "KZ": ("Kazakhstan", 14),
}


class Settings(BaseSettings):
    """All runtime config. Reads .env if present."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    llm_provider: Provider = Field(default="anthropic")
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-5"
    openai_model: str = "gpt-4o"

    google_service_account_json: Path = Path("./secrets/service_account.json")
    spreadsheet_id: str = ""

    sources_tab_ru: str = "Sources (RU)"
    sources_tab_uz: str = "Sources (UZ)"
    sources_tab_kz: str = "Sources (KZ)"
    news_tab: str = "News"
    published_news_tab: str = "Published news"
    sections_tab: str = "News sections"
    output_tab: str = "News"

    freshness_hours: int = 48
    fuzzy_title_threshold: float = 0.85
    default_rate_limit_rps: float = 1.0
    max_cost_usd: float = 5.0
    user_agent: str = "NewsMakerBot/0.1 (+https://example.com/bot)"

    sqlite_path: Path = Path("./data/news_agent.sqlite")

    log_level: str = "INFO"
    log_dir: Path = Path("./logs")

    def sources_tab_for(self, portal: Portal) -> str:
        return {
            "RU": self.sources_tab_ru,
            "UZ": self.sources_tab_uz,
            "KZ": self.sources_tab_kz,
        }[portal]

    def country_cell(self, portal: Portal) -> str:
        name, cid = PORTAL_COUNTRY[portal]
        return f"{name} [{cid}]"


_cached: Settings | None = None


def get_settings() -> Settings:
    global _cached
    if _cached is None:
        _cached = Settings()
    return _cached
