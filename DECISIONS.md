# Pragmatic decisions

This file records non-obvious calls made during Stage 1 MVP implementation.
When you override a brief assumption, append the override here.

## Stack / tooling
- **Python 3.11**, managed with `pip` + `pyproject.toml` (not `uv`) to keep onboarding
  friction low for editors who do not have `uv` installed. `uv pip install -e .[dev]`
  works identically if preferred.
- **`hatchling`** as build backend (zero-config, PEP 517 compliant).
- **`typer`** for the CLI rather than hand-rolled `argparse` — small dep, readable help,
  supports `--dry-run`/`--limit` naturally.

## LLM providers
- Default model IDs in `.env.example` are `claude-sonnet-4-5` and `gpt-4o`. The brief
  says "or latest available" — these can be overridden via env without code changes.
- Token counting:
  - Anthropic → the SDK returns `usage.input_tokens`/`usage.output_tokens` on every call;
    we trust that.
  - OpenAI → `tiktoken` is used for *pre-flight* estimation (for budget cap); actual
    usage comes back on the response and is preferred for accounting.
- Structured output:
  - Anthropic → tool-use with a forced tool (`classify_news`).
  - OpenAI → `response_format={"type": "json_schema", ...}`.
  Both validate against the same Pydantic model.

## Storage
- SQLite is a **dedup cache only**. Sheets is the system of record. We commit SQLite
  rows only after a successful `Sheets.append` — so a crash mid-append never marks an
  article as "seen" without it actually being written.

## Primary-source detection
- Heuristic-only (no second LLM call) for cost reasons. If the primary source is not
  resolvable from outbound links + cue phrases + brand-domain map, we fall back to the
  article URL itself and set `primary_source_confidence=low`.
- We do **not** fetch the primary-source page to confirm — the brief explicitly permits
  URL-only detection, and a network round-trip per article would blow the budget.

## Locality (Local vs Global)
- Decided by the LLM (second call) rather than a keyword rule, because country mentions
  are often implicit ("президент подписал" in a RU source usually means Russia).
- The LLM prompt is given the portal's country and explicitly asked whether the news is
  *specifically about* that country.

## Freshness
- `published_at` is read from RSS first, then `<meta property="article:published_time">`,
  then JSON-LD, then the HTTP `Last-Modified` header. If none are present, we assume
  "now minus 1 minute" and let the dedup layer handle reruns — this avoids dropping
  fresh items on sources that hide their publication time.

## Rate limiting
- Per-domain token-bucket in-process (default 1 rps, tunable in
  `config/sources_overrides.yaml`). This is sufficient for a single cron run; if we ever
  run multiple workers, we'd need a shared limiter (Redis / filesystem lock).

## Out-of-scope confirmations
- No Dockerfile (brief says follow-up).
- No CMS push, no bullet generation, no test-drives section.
- No web UI.

## Semantic dedup между источниками (MVP: нет)

Один инфоповод у нескольких СМИ (пример: «Mustang GTD обогнал Corvette на
Нюрбургринге» появился у motortrend + thedrive + carbuzz) в MVP проходит
как **три отдельные строки** — одна на каждый источник. Это осознанный
выбор:

- **Бот не знает, какая формулировка «лучшая»** — выбор источника делает
  редактор или Stage 3 (написание собственного текста по инфоповоду).
- **URL-дедуп уже работает** (одна и та же финальная страница не будет
  записана дважды).
- **Fuzzy-title дедуп против `Новости опубликованные`** тоже работает —
  если история уже была опубликована, её не предложим ещё раз.

Семантическая группировка «один инфоповод → выбор одного источника» —
задача Stage 2/3, когда будет писаться собственный текст. На Stage 1
приоритет — **правильно определять, что это новость и про авто**, а не
решать, какая из трёх версий лучше.

## Overrides of the brief's assumptions
*(none so far — append here if any assumption is reversed)*
