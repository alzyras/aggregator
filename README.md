---
title: Aggregator - Track Productivity and Health Data
description: Modular application for collecting data from Asana, Habitica, Toggl, and Google Fit. Store data in MySQL database for analysis.
keywords: wellness, productivity, health tracking, asana, habitica, toggl, google fit, data collection, mysql
---

# Aggregator Application

A modular application for collecting and storing data from various sources.

| Docs | Description |
| --- | --- |
| [Plugin details](README_PLUGINS.md) | Schemas, setup, and per-plugin notes |
| [LLM focus analysis](llm_focus.md) | Topic-focused cross-platform summaries |
| [LLM summary](llm_summary.md) | Ad-hoc, narrated summaries across all sources |
| [LLM progress](llm_progress.md) | Period-based progress reports |

## Quick start

```bash
# 1) Install deps (uv recommended)
uv sync

# 2) Configure environment
cp .env.example .env
# edit .env with DB + plugin credentials

# 3) Prepare tables
uv run python manage.py sync

# 4) Run the aggregator loop
uv run python manage.py run
```

## Commands (manage.py)

- `python manage.py run` — setup→fetch→write for each enabled plugin in a loop (respects `INTERVAL_SECONDS`)
- `python manage.py sync` — run setup only (create/prepare tables)
- `python manage.py debug` — show installed apps and enabled state
- `python manage.py llm_summary "your question"` — build context and ask the LLM for an answer
- `python manage.py llm_progress --period last_month` — generate a progress summary via the LLM
- `python manage.py llm_focus "topic" [last_month|last_90_days|last_12_months]` — topic-focused cross-platform summary (e.g., "learning Portuguese")

## Configuration

- `AGGREGATOR_SETTINGS_MODULE` (default `aggregator.settings.base`)
- `ENABLED_PLUGINS` (comma-separated, leave empty to enable all in `INSTALLED_APPS`)
- Database: `MYSQL_HOST`, `MYSQL_DB`, `MYSQL_USER`, `MYSQL_PASSWORD`
- Plugin creds: `ASANA_*`, `HABITICA_*`, `TOGGL_*`, `GOOGLE_FIT_*`
- LLM summary: `LLM_SUMMARY_*` (see `.env.example`)

See [README_PLUGINS.md](README_PLUGINS.md) for plugin-specific details.

## Docker

```bash
cp .env.example .env
# edit .env with DB + plugin credentials

docker compose up --build
# or use the helper
./start_docker.sh
```
