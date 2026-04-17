"""Pipeline orchestration: fetch → filter → classify → extract → write."""

from __future__ import annotations

from datetime import datetime, timezone

from news_agent.adapters.fetchers import HTMLFetcher, RateLimiter, RobotsCache, RSSFetcher
from news_agent.adapters.fetchers.base import make_http_client
from news_agent.adapters.fetchers.playwright_fetch import PlaywrightFetcher
from news_agent.adapters.llm import LLMClient, make_llm_client
from news_agent.adapters.sheets import SheetsClient
from news_agent.adapters.storage import DedupStore
from news_agent.core.budget import BudgetExceeded, BudgetTracker
from news_agent.core.config_loader import (
    SourceOverride,
    load_brand_domains,
    load_primary_source_cues,
    load_sections,
    load_sources_overrides,
    load_sources_schema,
)
from news_agent.core.dedup import title_is_duplicate
from news_agent.core.freshness import is_fresh
from news_agent.core.models import (
    Candidate,
    ClassifiedNews,
    OutputRow,
    Portal,
    RawArticle,
    RunSummary,
    Source,
)
from news_agent.core.primary_source import detect_primary_source
from news_agent.core.urls import canonicalise, domain_of, url_hash
from news_agent.logging_setup import get_logger
from news_agent.settings import Settings

log = get_logger("pipeline")


def _apply_overrides(source: Source, overrides: list[SourceOverride]) -> Source:
    for o in overrides:
        if source.url.startswith(o.url):
            updates: dict[str, object] = {}
            if o.requires_js:
                updates["requires_js"] = True
            if o.rate_limit_rps is not None:
                updates["rate_limit_rps"] = o.rate_limit_rps
            if o.language:
                updates["language"] = o.language
            if o.rss_url:
                updates["url"] = o.rss_url
                updates["type"] = "rss"
            if updates:
                return source.model_copy(update=updates)
    return source


def run_pipeline(
    settings: Settings,
    portal: Portal,
    *,
    dry_run: bool = False,
    limit_per_source: int = 20,
    since_hours_override: int | None = None,
) -> RunSummary:
    summary = RunSummary(portal=portal)

    sheets = SheetsClient(settings.spreadsheet_id, settings.google_service_account_json)
    dedup = DedupStore(settings.sqlite_path)
    llm: LLMClient = make_llm_client(settings)
    budget = BudgetTracker(cap_usd=settings.max_cost_usd)

    schema = load_sources_schema()
    overrides = load_sources_overrides()
    brands = load_brand_domains()
    cues = load_primary_source_cues()

    sections = sheets.read_sections(settings.sections_tab) or load_sections()
    if not sections:
        raise RuntimeError("No sections available (sheet empty and sections.yaml missing)")

    few_shots = sheets.read_few_shots(settings.news_tab)
    existing_titles = sheets.read_existing_titles(
        [settings.news_tab, settings.published_news_tab]
    )

    sources_raw = sheets.read_sources(settings.sources_tab_for(portal), schema)
    sources = [_apply_overrides(s, overrides) for s in sources_raw if s.is_active]
    summary.sources_total = len(sources)

    http = make_http_client(settings.user_agent)
    rate = RateLimiter(default_rps=settings.default_rate_limit_rps)
    for src in sources:
        if src.rate_limit_rps is not None:
            rate.set_rate(domain_of(src.url), src.rate_limit_rps)
    robots = RobotsCache(settings.user_agent, http)
    html_fetcher = HTMLFetcher(http, rate, robots)
    rss_fetcher = RSSFetcher(http, rate, robots, html_fetcher)
    pw_fetcher: PlaywrightFetcher | None = None
    if any(s.requires_js for s in sources):
        pw_fetcher = PlaywrightFetcher(settings.user_agent)

    # -------------------------------------------------------- collect candidates
    candidates: list[Candidate] = []
    freshness_hours = since_hours_override or settings.freshness_hours
    now = datetime.now(timezone.utc)
    collected_hashes: set[str] = set()

    for src in sources:
        try:
            if src.requires_js and pw_fetcher is not None:
                articles = pw_fetcher.fetch(src, limit_per_source)
            elif src.type == "rss":
                articles = rss_fetcher.fetch(src, limit_per_source)
            else:
                articles = html_fetcher.fetch(src, limit_per_source)
        except Exception as e:  # noqa: BLE001
            log.warning("source.failed", source=src.name, url=src.url, error=str(e))
            summary.sources_failed += 1
            continue
        summary.sources_ok += 1
        summary.candidates_found += len(articles)
        for art in articles:
            c = _to_candidate(art)
            if c is None or c.url_hash in collected_hashes:
                continue
            if not is_fresh(art.published_at, hours=freshness_hours, now=now):
                log.debug("filter.stale", url=c.canonical_url)
                continue
            collected_hashes.add(c.url_hash)
            candidates.append(c)

    # ---------------------------------------------------------- batch dedup
    dedup_hits = dedup.has_any([c.url_hash for c in candidates])
    after_sqlite = [c for c in candidates if c.url_hash not in dedup_hits]

    after_fuzzy = [
        c
        for c in after_sqlite
        if not title_is_duplicate(
            c.raw.title, existing_titles, threshold=settings.fuzzy_title_threshold
        )
    ]
    log.info(
        "candidates.after_filters",
        collected=len(candidates),
        after_sqlite=len(after_sqlite),
        after_fuzzy=len(after_fuzzy),
    )

    # ------------------------------------------------------ LLM classification
    classified: list[ClassifiedNews] = []
    country_name = settings.country_cell(portal).split(" [")[0]
    try:
        for c in after_fuzzy:
            rel, u1 = llm.is_automotive(c.raw.title, c.raw.body[:500])
            budget.record(u1)
            if not rel.is_automotive_or_economy:
                log.info("filter.not_automotive", url=c.canonical_url, reason=rel.reason)
                continue

            cls, u2 = llm.classify_section(
                title=c.raw.title,
                body=c.raw.body,
                sections=sections,
                few_shots=few_shots,
                portal_country=country_name,
            )
            budget.record(u2)
            if cls.section not in {s.name for s in sections}:
                log.warning("classify.unknown_section", section=cls.section, url=c.canonical_url)
                cls.section = "other"

            titles, u3 = llm.translate_title(
                title=c.raw.title, source_language_hint=c.raw.source_language
            )
            budget.record(u3)

            p_url, p_domain, p_conf = detect_primary_source(
                article_url=c.canonical_url,
                body=c.raw.body,
                title=c.raw.title,
                outbound_links=c.raw.outbound_links,
                brands=brands,
                cues=cues,
            )
            classified.append(
                ClassifiedNews(
                    candidate=c,
                    titles=titles,
                    classification=cls,
                    primary_source_url=p_url,
                    primary_source_domain=p_domain,
                    primary_source_confidence=p_conf,
                    image_url=c.raw.image_url,
                    llm_provider=llm.provider_name,
                )
            )
    except BudgetExceeded as e:
        summary.aborted = True
        summary.abort_reason = str(e)
        log.error("pipeline.aborted", reason=str(e), snapshot=budget.snapshot())

    summary.candidates_after_filters = len(classified)

    # --------------------------------------------------------------- write
    rows = [_to_row(cn, settings=settings, portal=portal, now=now) for cn in classified]

    if dry_run:
        log.info("dry_run.rows", count=len(rows))
        for r in rows:
            log.info("dry_run.row", **{k: v for k, v in r.model_dump().items()})
    else:
        sheets.ensure_output_header(settings.output_tab)
        written = sheets.append_rows(settings.output_tab, rows)
        summary.rows_written = written
        # Commit dedup rows *only* after successful Sheets append.
        dedup_entries = [
            (
                cn.candidate.url_hash,
                cn.candidate.canonical_url,
                cn.candidate.raw.title,
                cn.candidate.raw.published_at.isoformat() if cn.candidate.raw.published_at else None,
                cn.candidate.source_domain,
                portal,
            )
            for cn in classified
        ]
        dedup.mark_many(dedup_entries)

    summary.total_cost_usd = budget.spent_usd
    dedup.log_run(portal, summary.model_dump_json())
    log.info("pipeline.done", **summary.model_dump())
    return summary


def _to_candidate(art: RawArticle) -> Candidate | None:
    if not art.url or not art.title:
        return None
    canonical = canonicalise(art.url)
    return Candidate(
        raw=art,
        url_hash=url_hash(canonical),
        canonical_url=canonical,
        source_domain=domain_of(canonical),
    )


def _to_row(cn: ClassifiedNews, *, settings: Settings, portal: Portal, now: datetime) -> OutputRow:
    published_iso = cn.candidate.raw.published_at.isoformat() if cn.candidate.raw.published_at else ""
    localized = f"{cn.titles.english} / {cn.titles.russian} ({cn.titles.source_language})"
    return OutputRow(
        start_date=now.isoformat(timespec="seconds"),
        section=cn.classification.section,
        name=cn.titles.english,
        localized_title=localized,
        announcement_image=cn.image_url or "",
        region=cn.classification.region,
        country=settings.country_cell(portal),
        primary_source_url=cn.primary_source_url,
        primary_source_domain=cn.primary_source_domain,
        aggregator_url=cn.candidate.canonical_url,
        published_at=published_iso,
        llm_provider=cn.llm_provider,
        confidence=cn.classification.confidence,
    )


__all__ = ["run_pipeline"]
