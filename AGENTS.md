# Agent handoff — NewsMaker / news_agent

You are picking up the **Stage 1 MVP** of an automotive / economy news
aggregator for a company that manually monitors ~357 sources per day and
publishes selected news to three portals (RU / UZ / KZ, plus EN later).
Stage 1 = *collect → dedup → pre-filter → classify → write to Sheets*.
Stages 2 (CMS push) and 3 (bullet-point generation) are **out of scope**.

Read `README.md` and `DECISIONS.md` for the deep dive. This file is the
shortest path from "fresh clone" to "I know what to do next".

## Repository at a glance

```
config/                    YAML tunables (sections, brand domains, cue phrases, whitelist)
src/news_agent/            production package (core / adapters / pipeline / cli)
scripts/                   one-off runnables (smoke tests, batch runs, analyses)
tests/                     pytest — pure-core unit tests + one integration test
```

The pipeline is hexagonal: `cli → pipeline → adapters → core`. Core is pure,
unit-tested. Adapters (Sheets, fetchers, LLM) do all I/O.

## What has been built so far

- ✅ Core models (`core/models.py`) + config loaders (`core/config_loader.py`)
- ✅ Three fetcher types — **RSS**, **HTML**, **Telegram** (public `t.me/s/`)
- ✅ LLM adapter with **Anthropic + OpenAI** implementations (provider-agnostic)
- ✅ Heuristic `is_article` + `is_auto_or_economy` filters
- ✅ **Three-tier grading** (`core/heuristic_relevance.grade_article`):
  `certain_news` → directly to section classification,
  `possible_news` → cheap LLM relevance-check first,
  `off_topic` / `not_article` → rejected without any LLM call.
- ✅ SQLite dedup cache
- ✅ Google Sheets reader / writer with conditional formatting in test tabs
- ✅ `batch_fetch_test.py` — end-to-end offline batch run (no LLM) that
  writes versioned `ТЕСТ статьи v2/v3/…` tabs with coloured verdicts.
- ✅ `analyze_published_news.py` — derives a **whitelist of 28 domains**
  and a **few-shot corpus** (`config/few_shots.yaml`) from 2989 published
  rows.
- ✅ Tests: `pytest -q` → all green (13 heuristic + 3 telegram + others).

## Current open threads

1. **LLM classification is coded but not yet wired into the batch run.**
   The `batch_fetch_test.py` script stops at the three-tier grading and
   does **not** call an LLM. The production pipeline (`pipeline/run.py`)
   has the full flow, but the user has not yet provided an Anthropic API
   key, so we haven't done a live LLM run. See "Next steps" below.
2. **Only ~80 rows in `ТЕСТ статьи` are human-labelled** (the bare base
   tab). Everything compared against that is the gold set. V2 lowered
   the false-positive rate from 38 → 9 on the 74 common URLs. V3 adds
   the three-tier grading; not yet scored against the human labels.
3. **Real section names** (from the published-news corpus) are now in
   `config/sections.yaml`: `Confirmed`, `Local specifics`, `Other news`,
   `Rumors`, `Economics`, `LCV news`, `Test-drive`, `Dealer news / Promo`,
   `Motorshow`. **Do not rename them** — the editor's sheet uses these
   strings verbatim.
4. **Test-drive** — user said "leave it, but mark for editor review".
   When LLM wiring lands, output should include a "требует ручной
   проверки" flag for Test-drive rows.
5. **UZ / KZ / EN portals** will be added later. The code already has
   RU / UZ / KZ; add EN when the user provides its country_id.

## Run commands

First time on this machine:

```bash
python3.11 -m venv .venv
source .venv/bin/activate           # Windows: .venv\Scripts\activate
pip install -e .[dev]
cp .env.example .env
# Fill SPREADSHEET_ID in .env. Put the Google service-account JSON at
# secrets/service_account.json (it is gitignored — ask the user for it;
# do NOT commit).
```

Then:

```bash
make test                           # pytest
python scripts/smoke_sheets_access.py         # verify Google Sheets auth
python scripts/inspect_sheets.py              # print real tab structure
python scripts/analyze_published_news.py      # rebuild few_shots.yaml + АНАЛИЗ sheet
python scripts/batch_fetch_test.py            # offline batch run (no LLM) → ТЕСТ статьи vN
python scripts/apply_sheet_formatting.py      # re-colour the latest test tab
python scripts/analyze_manual_review.py       # score bot vs human labels
python scripts/compare_versions.py            # diff between test tabs
```

## User's sheet at a glance

| Tab in sheet              | What's there                          | Code reads as           |
|---                        |---                                    |---                      |
| `Источники (для РУ)`      | 357 URLs (col A = flag `1`/blank, col B = URL) | `SOURCES_TAB_RU` |
| `Новости`                 | 3118 rows — editor's working buffer   | `NEWS_TAB`              |
| `Новости опубликованные`  | 2989 rows — **gold standard**         | `PUBLISHED_NEWS_TAB`    |
| `Разделы новостей`        | 9 canonical sections                  | `SECTIONS_TAB`          |
| `ТЕСТ статьи`             | 102 rows, **human-labelled in col P** | analysis only           |
| `ТЕСТ статьи v2/v3/…`     | each batch run's output               | created by the script   |
| `АНАЛИЗ опубликованных`   | summary pivot over published news     | generated by script     |

**Golden rule:** never overwrite the bare `ТЕСТ статьи` tab — it contains
the human review. `batch_fetch_test.py` auto-allocates `v{N+1}` for each
run (see `_next_version` in that file).

## Next concrete steps (in order)

1. Ask the user for `ANTHROPIC_API_KEY` (begins with `sk-ant-api03-…`).
   Put in `.env`. **Never commit .env.**
2. Wire up the LLM path on the latest `ТЕСТ статьи v*` tab:
   - For rows with "Точно новость" → `classify_section` + `translate_title`.
   - For rows with "Возможно новость" → `is_automotive` first, then the above.
   - Write into a new sheet `ТЕСТ классификация v1` with: bot verdict,
     section, region, confidence, EN/RU title, primary-source URL,
     per-call cost, total cost at the bottom.
3. Have the user review the classification; calibrate section prompts
   and `CERTAIN_SCORE_THRESHOLD` / `POSSIBLE_SCORE_THRESHOLD` in
   `core/heuristic_relevance.py`.
4. Then wire the same two-tier logic into `pipeline/run.py` (real
   production pipeline, writes to the live `Новости` tab).
5. Only after steps 2-4 are stable → Stage 2 (CMS push) and Stage 3
   (bullet-point generation).

## Things to keep in mind

- **`.env` и `secrets/` gitignored** — не коммить. На Mac пользователь
  вручную создаст `.env` по образцу и положит `service_account.json`.
- Код **кроссплатформенный** — все пути через `pathlib.Path`; все
  encode/decode через `utf-8`. Windows-специфичный `io.TextIOWrapper`
  в некоторых скриптах — безопасен на Mac (ничего не ломает).
- Sheets API имеет лимит 60 RPM на чтение — читай каждую вкладку за
  один вызов, как сделано в адаптерах.
- Секции в `config/sections.yaml` должны совпадать с листом `Разделы
  новостей` **буквально** (строковое совпадение при записи в Sheets).

## Communication style

The user is non-technical. Respond in Russian. Explain *what* you
changed and *why* in plain language; leave implementation detail for the
code + DECISIONS.md. Confirm destructive actions before running.
