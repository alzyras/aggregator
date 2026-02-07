# Aggregator (Django)

Personal data aggregator that ingests and normalizes data from Google Fit, Asana, Todoist, and Habitica.

## Features
- Clean Django architecture with `core`, `connectors`, `ingestion`, and `events` apps.
- PostgreSQL database with automatic database creation via `ensure_db`.
- Admin UI + simple Django templates for browsing events and triggering syncs.
- Service layer for connector clients, normalizers, and sync orchestration.

## Quickstart

1. Copy env file and edit secrets:

```bash
cp .env.example .env
```

2. Start the app (uses `uv` to create venv, installs deps, ensures DB, migrates, runs server):

```bash
./launch_server.sh
```

3. Visit:
- `http://localhost:8000/` for dashboard
- `http://localhost:8000/events/` for events
- `http://localhost:8000/sync/` to trigger syncs
- `http://localhost:8000/admin/` for admin

## Database

The management command `ensure_db` connects to the maintenance database (default `postgres`) and creates the target database (default `aggregator`) if missing. It uses these environment variables:

- `PGHOST` or `POSTGRES_HOST`
- `PGPORT` or `POSTGRES_PORT`
- `PGUSER` or `POSTGRES_USER`
- `PGPASSWORD` or `POSTGRES_PASSWORD`
- `PGDATABASE` or `POSTGRES_DB` (target DB, defaults to `aggregator`)
- `PGMAINTENANCE_DB` (optional override, defaults to `postgres`)

Run manually:

```bash
python aggregator_project/manage.py ensure_db
python aggregator_project/manage.py migrate
```

## Syncing

Run all sources:

```bash
python aggregator_project/manage.py sync_all
```

Run a single source:

```bash
python aggregator_project/manage.py sync_source --source=asana
```

Optionally pass `--since=2025-01-01T00:00:00Z` to limit scope.

## Notes

- Google Fit OAuth flow is stubbed; update `connectors/google_fit/client.py` with real OAuth/token refresh handling.
- Connector credentials can be stored in `ConnectorAccount.credentials` encrypted with `ENCRYPTION_KEY` (Fernet).

## Project Layout

- `aggregator_project/` Django project
- `aggregator_project/core/` settings & utilities
- `aggregator_project/connectors/` provider auth tokens (ConnectorAccount)
- `aggregator_project/ingestion/` sync orchestration, normalizers, jobs
- `aggregator_project/events/` normalized data model + UI
- `aggregator_project/providers/` per-provider apps (client + normalizer)

## Tests

```bash
python aggregator_project/manage.py test
```
