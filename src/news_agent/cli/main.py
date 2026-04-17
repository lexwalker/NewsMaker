"""`python -m news_agent` / `news_agent` CLI."""

from __future__ import annotations

import json
from typing import Annotated

import typer
from rich.console import Console

from news_agent.logging_setup import configure_logging, get_logger
from news_agent.pipeline.run import run_pipeline
from news_agent.settings import Portal, get_settings

app = typer.Typer(add_completion=False, help="Automotive news aggregator — Stage 1.")
console = Console()


@app.command()
def run(
    portal: Annotated[str, typer.Option("--portal", "-p", help="RU | UZ | KZ")],
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Do not write to Sheets")] = False,
    limit: Annotated[int, typer.Option("--limit", help="Max items per source")] = 20,
    since_hours: Annotated[
        int | None, typer.Option("--since-hours", help="Override FRESHNESS_HOURS")
    ] = None,
) -> None:
    """Fetch, classify and append fresh news for the given portal."""
    portal_u = portal.upper()
    if portal_u not in {"RU", "UZ", "KZ"}:
        raise typer.BadParameter("portal must be one of RU, UZ, KZ")
    s = get_settings()
    log_path = configure_logging(s.log_dir, s.log_level)
    log = get_logger("cli")
    log.info("run.start", portal=portal_u, dry_run=dry_run, limit=limit, log_file=str(log_path))

    summary = run_pipeline(
        s,
        portal_u,  # type: ignore[arg-type]
        dry_run=dry_run,
        limit_per_source=limit,
        since_hours_override=since_hours,
    )
    console.print_json(data=summary.model_dump())
    if summary.aborted:
        raise typer.Exit(code=2)


@app.command()
def show_config() -> None:
    """Print effective runtime settings (sans secrets)."""
    s = get_settings()
    payload = s.model_dump()
    for k in ("anthropic_api_key", "openai_api_key"):
        payload[k] = "***" if payload.get(k) else ""
    console.print_json(data=json.loads(json.dumps(payload, default=str)))


if __name__ == "__main__":
    app()
