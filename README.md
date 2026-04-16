# Aggregator (Django)

Personal data aggregator that ingests and normalizes data from Google Fit, Asana, Todoist, and Habitica.

## Features
- Clean Django architecture with `core`, `connectors`, `ingestion`, and `events` apps.
- PostgreSQL database with automatic database creation via `ensure_db`.
- Admin UI + simple Django templates for browsing events and triggering syncs.
- Service layer for connector clients, normalizers, and sync orchestration.
- DB-backed background jobs for sync execution.

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
- `http://localhost:8000/sync/` to trigger sync jobs
- `http://localhost:8000/jobs/` to inspect job status
- `http://localhost:8000/admin/` for admin

## Docker / Portainer

The repository is now dockerized for a production-style deployment with two services:

- `web`: Django + Gunicorn
- `worker`: background job runner (`run_worker`)
- `postgres`: PostgreSQL (default; override with external DB via `PGHOST`)

Start locally with Docker Compose:

```bash
docker compose --env-file .env up -d --build
```

Portainer stack deployment:

1. Use this repository as the stack source.
2. Use [`/Users/tomas/Documents/projects/aggregator/docker-compose.yml`](/Users/tomas/Documents/projects/aggregator/docker-compose.yml) (Portainer default filename).
3. Set environment variables in Portainer stack settings:
- `DJANGO_SECRET_KEY`
- `DJANGO_ALLOWED_HOSTS`
- `ENCRYPTION_KEY` (required and must stay stable)
- `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
- `DB_DEPLOYMENT_MODE` (`built_in` for the compose `postgres` service, `external` for an existing database)
- `WEB_PORT` (host port, example `8000`)
- `APP_PORT` (container app port, default `8000`)
4. Expose port `8000` from the `web` service.

Optional startup flags:

- `AUTO_CREATE_DB=1` to run `ensure_db` on container start (only if DB user can create databases)
- `RUN_MIGRATIONS=1` to apply migrations at startup (default on `web`)
- `COLLECT_STATIC=1` to build static assets (default on `web`)
- Application URL is `http://<your-hostname>:<WEB_PORT>/`
- If you keep the built-in DB, set `DB_DEPLOYMENT_MODE=built_in` and `PGHOST=postgres` (or leave it unset).
- If you use an external DB, set `DB_DEPLOYMENT_MODE=external` and set `PGHOST` to that host explicitly. Do not leave `PGHOST=postgres`, or Portainer will point the app at the stack-created database service.
- `VALIDATE_RUNTIME_CONFIG=1` runs startup configuration checks for both `web` and `worker`; `STRICT_RUNTIME_CONFIG=1` turns warnings into startup failures.

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

Queue a sync for all sources:

```bash
python aggregator_project/manage.py sync_all --workspace-id <id>
```

Queue a single source:

```bash
python aggregator_project/manage.py sync_source --source=asana --workspace-id <id>
```

Run a worker (production / separate process):

```bash
python aggregator_project/manage.py run_worker
```

In development, a background worker thread auto-starts when `DEBUG=true`.
Queued jobs are durable in Postgres. A worker restart reclaims jobs that were
left `running` longer than `JOB_STALE_RUNNING_SECONDS`. General sync jobs use
`JOB_MAX_ATTEMPTS`; planner task status writeback uses
`PLANNER_STATUS_WRITEBACK_MAX_RETRIES` retries before surfacing a failed sync.
Jobs also use Postgres leases, so a stopped worker/container can be recovered
by another worker after the lease expires.

Local queue operations:

```bash
python aggregator_project/manage.py queue_health
python aggregator_project/manage.py recover_stale_jobs
python aggregator_project/manage.py retry_failed_writebacks --workspace-id <id>
```

Optionally pass `--since=2025-01-01T00:00:00Z` to limit scope.

## Notes

- Google Fit OAuth flow is stubbed; update `connectors/google_fit/client.py` with real OAuth/token refresh handling.
- Connector credentials can be stored in `ConnectorAccount.credentials` encrypted with `ENCRYPTION_KEY` (Fernet).

## Project Layout

- `aggregator_project/` Django project
- `aggregator_project/core/` settings & utilities
- `aggregator_project/connectors/` provider auth tokens (ConnectorAccount)
- `aggregator_project/ingestion/` sync orchestration, jobs, worker
- `aggregator_project/events/` normalized data model + UI
- `aggregator_project/providers/` per-provider apps (client + normalizer)

## Tests

```bash
python aggregator_project/manage.py test
```
