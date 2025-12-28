---
title: Aggregator - Track Productivity and Health Data
description: Modular application for collecting data from Asana, Habitica, Toggl, and Google Fit. Store data in MySQL database for analysis.
keywords: wellness, productivity, health tracking, asana, habitica, toggl, google fit, data collection, mysql
---

# Aggregator Application

A modular application for collecting and storing data from various sources.

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

## Configuration

- `AGGREGATOR_SETTINGS_MODULE` (default `aggregator.settings.base`)
- `ENABLED_PLUGINS` (comma-separated, leave empty to enable all in `INSTALLED_APPS`)
- Database: `MYSQL_HOST`, `MYSQL_DB`, `MYSQL_USER`, `MYSQL_PASSWORD`
- Plugin creds: `ASANA_*`, `HABITICA_*`, `TOGGL_*`, `GOOGLE_FIT_*`

See `README_PLUGINS.md` for plugin-specific details.
