# news_agent — Stage 1 MVP

Automated automotive / economy news aggregator. Reads a list of monitored
sources from a Google Sheet, fetches fresh articles, filters them with an
LLM, classifies them into sections, detects the primary source, and appends
rows to an output sheet.

Stage 2 (auto-transfer to CMS) and Stage 3 (bullet-point generation) are
**not** implemented here.

---

## Quick start

```bash
# 1. Install
make install           # or: pip install -e ".[dev]"

# 2. Configure
cp .env.example .env
# fill in SPREADSHEET_ID, ANTHROPIC_API_KEY / OPENAI_API_KEY,
# and place your Google service-account JSON at secrets/service_account.json

# 3. Smoke-test access to Sheets
make sheets-check      # or: python scripts/check_sheets.py

# 4. Dry-run (no writes)
python -m news_agent run --portal RU --dry-run --limit 5

# 5. Real run
python -m news_agent run --portal RU
```

Running the same command twice appends zero new rows the second time
(idempotency is enforced via SQLite dedup + fuzzy-title dedup against the
output sheet).

---

## Google service-account setup

1. Create a GCP project (any project will do).
2. Enable the **Google Sheets API**.
3. Create a **service account**. Under *Keys → Add key → JSON*, download the
   credentials file.
4. Save it at `secrets/service_account.json` (git-ignored) or set
   `GOOGLE_SERVICE_ACCOUNT_JSON` in `.env` to its absolute path.
5. Open your monitoring spreadsheet in Google Sheets → *Share* → paste the
   service account email (`…@…iam.gserviceaccount.com`) with **Editor**
   access.
6. Copy the spreadsheet ID from its URL (the string between `/d/` and
   `/edit`) into `.env` as `SPREADSHEET_ID`.

---

## CLI

```
python -m news_agent run --portal {RU|UZ|KZ}
                         [--dry-run]
                         [--limit N]              # max items per source
                         [--since-hours N]        # override FRESHNESS_HOURS

python -m news_agent show-config                   # effective settings (secrets redacted)
```

Scripts:

```
python scripts/check_sheets.py       # print source counts per portal
python scripts/compare_llms.py       # run the 20-article fixture through both providers
```

---

## Configuration

All tunables are YAML under `config/` or env under `.env`:

| File | Purpose |
|---|---|
| `config/sections.yaml` | Canonical section list + definitions + examples. Overridden by the `News sections` tab if non-empty. |
| `config/sources_schema.yaml` | Maps sheet columns → `Source` fields. Edit on first run if your headers differ. |
| `config/sources_overrides.yaml` | Per-URL overrides: `requires_js`, `rate_limit_rps`, `language`, `rss_url`. |
| `config/brand_domains.yaml` | Brand → known official/press domains. Used by primary-source detection. |
| `config/primary_source_cues.yaml` | Cue phrases (per language) and press-release host list. |

### Adding a new portal

1. Add a sources tab `Sources (XX)` to the input spreadsheet.
2. Extend `PORTAL_COUNTRY` in `src/news_agent/settings.py` with the country
   name + numeric id.
3. Extend the `Portal` Literal in `src/news_agent/core/models.py`.
4. Add `SOURCES_TAB_XX` to `.env.example` and `settings.py`.

*(The brief required "no code changes to add a portal"; in practice, the
country→id mapping must live somewhere deterministic. We kept it in one
module so adding a portal is a 4-line change in one file.)*

### Adding a source that needs JavaScript

```yaml
# config/sources_overrides.yaml
overrides:
  - url: https://example-js-heavy-site.com/
    requires_js: true
```

Then install Playwright + a browser:

```bash
pip install -e ".[js]"
playwright install chromium
```

All other sources continue to use httpx; Playwright is invoked only for the
flagged URL.

---

## Scheduling

### Linux (cron)

```
# run RU portal hourly at :07, with environment from a dedicated file
7 * * * * cd /opt/news_agent && /opt/news_agent/.venv/bin/python -m news_agent run --portal RU >> /opt/news_agent/logs/cron.log 2>&1
17 * * * * cd /opt/news_agent && /opt/news_agent/.venv/bin/python -m news_agent run --portal UZ >> /opt/news_agent/logs/cron.log 2>&1
27 * * * * cd /opt/news_agent && /opt/news_agent/.venv/bin/python -m news_agent run --portal KZ >> /opt/news_agent/logs/cron.log 2>&1
```

### Linux (systemd timer)

```ini
# /etc/systemd/system/news-agent@.service
[Service]
Type=oneshot
WorkingDirectory=/opt/news_agent
ExecStart=/opt/news_agent/.venv/bin/python -m news_agent run --portal %i

# /etc/systemd/system/news-agent@.timer
[Timer]
OnCalendar=hourly
Persistent=true
Unit=news-agent@%i.service
[Install]
WantedBy=timers.target
```

```bash
systemctl enable --now news-agent@RU.timer news-agent@UZ.timer news-agent@KZ.timer
```

### Windows (Task Scheduler)

Save as `news-agent-RU.xml` and import via *Task Scheduler → Import Task…*:

```xml
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>2026-04-16T08:00:00</StartBoundary>
      <Repetition>
        <Interval>PT1H</Interval>
      </Repetition>
      <ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>
    </CalendarTrigger>
  </Triggers>
  <Actions>
    <Exec>
      <Command>C:\Users\you\news_agent\.venv\Scripts\python.exe</Command>
      <Arguments>-m news_agent run --portal RU</Arguments>
      <WorkingDirectory>C:\Users\you\news_agent</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
```

Duplicate for UZ and KZ.

---

## Cost report

Every LLM call is logged with `provider`, `model`, `prompt_hash`,
`input_tokens`, `output_tokens`, `cost_usd`, `latency_ms` (JSON lines in
`logs/run_*.log`).

The run emits a `pipeline.done` event with a `RunSummary` that includes
`total_cost_usd`. If `MAX_COST_USD` is exceeded mid-run, the pipeline
aborts cleanly (`aborted=true`, `abort_reason` set) — already-classified
rows are **not** flushed to Sheets.

To compare providers end-to-end:

```bash
python scripts/compare_llms.py
# → reports/llm_comparison.md
```

The fixture set lives at `scripts/fixtures/eval_articles.json` — add items
as you encounter edge cases.

---

## Architecture

```
src/news_agent/
├── settings.py                 # env-backed Settings
├── logging_setup.py            # structlog → file (JSON) + stderr (rich)
├── core/                       # pure domain logic — no I/O
│   ├── models.py               # Pydantic models for everything
│   ├── config_loader.py        # YAML loaders
│   ├── urls.py                 # canonicalise, hash, domain_of
│   ├── freshness.py
│   ├── dedup.py
│   ├── primary_source.py       # heuristic detection
│   └── budget.py               # cost cap
├── adapters/                   # I/O — depend on core, never reverse
│   ├── sheets.py
│   ├── storage.py              # SQLite dedup cache + run log
│   ├── fetchers/
│   │   ├── base.py             # http client, rate limiter, robots cache
│   │   ├── rss.py
│   │   ├── html.py             # + pure extract_article()
│   │   └── playwright_fetch.py
│   └── llm/
│       ├── base.py             # LLMClient Protocol + shared prompts/schemas
│       ├── anthropic_client.py
│       ├── openai_client.py
│       ├── pricing.py          # per-mtok prices
│       └── factory.py
├── pipeline/run.py             # orchestration: fetch → filter → classify → write
└── cli/main.py                 # Typer entrypoint
```

Dependency rule: `cli → pipeline → adapters → core`. Nothing points back.
`pipeline/run.py` is the only module that composes the whole stack.

Tests mock at the adapter boundary: `test_pipeline_integration.py`
substitutes `SheetsClient`, fetchers and LLM with in-memory fakes, which
lets the full pipeline run in milliseconds with no network or keys.

---

## Known risks / tuning levers

- **Scraper blocking.** Russian sources often block non-browser user-agents
  or foreign IPs. Expect to add `requires_js: true` overrides or reduce
  `rate_limit_rps` for problem sources. Consider putting the scheduled
  runner on a Russian VPS for RU sources.
- **Primary-source accuracy (~70–80% first pass).** Tune
  `config/brand_domains.yaml` and `config/primary_source_cues.yaml` as you
  see mis-detections. Add new brand entries whenever the editor picks up
  news from an unfamiliar manufacturer.
- **Classification drift to `other`.** The LLM leans on few-shot examples
  pulled from the curated `News` tab. If a section has fewer than ~3
  examples there, classification quality drops. The fix is to backfill the
  `News` tab, not to tune the prompt.
- **Sheets 60 rpm read limit.** All reads are batched per tab; no
  per-article round-trips. If you scale up beyond ~20 sources per portal,
  consider caching sheet reads for the duration of a single run (today each
  tab is read exactly once).
- **Translation quality.** MVP uses a single "translate title" LLM call.
  Longer body translation, or a human-review step, is deliberately
  out-of-scope.

---

## Development

```bash
make install            # editable install + dev deps
make lint               # ruff + black --check + mypy --strict
make test               # pytest
make check              # lint + test
make format             # ruff --fix + black
```

All code is typed under `mypy --strict`. Core logic (dedup, freshness,
primary-source, budget, URL canonicalisation, HTML extraction) is pure and
has unit tests; the full pipeline has an integration test against in-memory
fakes.

---

## Out of scope (follow-ups)

- CMS auto-push (Stage 2) and bullet generation (Stage 3).
- Dockerfile + compose bundle — trivial given the stateless runtime, but
  not required for the MVP acceptance criteria.
- Multi-worker coordination (today's rate limiter is process-local).
- Non-title translation quality.
- Metrics / alerting beyond structured log output.
