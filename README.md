# Aggregator

A unified task inbox for work spread across multiple platforms. Aggregator syncs
tasks into one responsive planner, keeps local actions immediate, and writes
status and description changes back to the source service in the background.

## Features

- One newest-first Inbox across Asana, Todoist, Jira, Habitica, GitHub Issues,
  Linear, ClickUp, and Trello, with Google Fit available for non-task activity data.
- Fast local filtering, search, sorting, status changes, pinning, ordering, and
  inline description editing.
- Optimistic UI updates with independent status and description writeback state,
  visible retry controls, and durable background jobs.
- Provider-owned connector contracts for forms, verification, normalization,
  source links, credentials, badges, and writeback behavior.
- Workspace-scoped unified tags, task summaries, a persistent data chat, and
  outcome-based work-pattern insights.
- Configurable OpenAI Responses or OpenAI-compatible local Qwen analysis.
- Workspace-scoped optional plugins discovered from self-contained folders.
- Built-in SQL Explorer, task-aware Data Chat, and Activity Pulse plugins.
- PostgreSQL persistence, Gunicorn web service, and a leased background worker.

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

- `http://localhost:8000/planner/` for the unified task inbox
- `http://localhost:8000/insights/` for unified tags and work-pattern insights
- `http://localhost:8000/insights/chat/` for workspace data chat
- `http://localhost:8000/connectors/` to add task platforms
- `http://localhost:8000/plugins/` to enable workspace tools
- `http://localhost:8000/jobs/` to inspect background jobs
- `http://localhost:8000/admin/` for Django administration

## Docker / Portainer

The production compose stack contains five services:

- `web`: Django + Gunicorn
- `worker`: background job runner (`run_worker`)
- `postgres`: PostgreSQL (default; override with external DB via `PGHOST`)
- `redis`: shared, disposable cache for versioned derived read models
- `cloudflared`: Cloudflare Tunnel connector when a tunnel token is configured

Start locally with Docker Compose:

```bash
docker compose --env-file .env up -d --build
```

Portainer stack deployment:

1. Use this repository as the stack source.
2. Use `docker-compose.yml` (Portainer's default compose filename).
3. Set environment variables in Portainer stack settings:
- `DJANGO_SECRET_KEY`
- `DJANGO_ALLOWED_HOSTS`
- `ENCRYPTION_KEY` (required and must stay stable)
- `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
- `DB_DEPLOYMENT_MODE` (`built_in` for the compose `postgres` service, `external` for an existing database)
- `WEB_PORT` (host port, example `8000`)
- `APP_PORT` (container app port, default `8000`)
- `ENABLED_CONNECTORS` (optional comma-separated provider allowlist)
- `CACHE_URL` (optional external Redis URL; defaults to the stack's `redis` service)
4. Expose port `8000` from the `web` service.

Optional startup flags:

- `AUTO_CREATE_DB=1` to run `ensure_db` on container start (only if DB user can create databases)
- `RUN_MIGRATIONS=1` to apply migrations at startup (default on `web`)
- `COLLECT_STATIC=1` to build static assets (default on `web`)
- Application URL is `http://<your-hostname>:<WEB_PORT>/`
- If you keep the built-in DB, set `DB_DEPLOYMENT_MODE=built_in` and `PGHOST=postgres` (or leave it unset).
- If you use an external DB, set `DB_DEPLOYMENT_MODE=external` and set `PGHOST` to that host explicitly. Do not leave `PGHOST=postgres`, or Portainer will point the app at the stack-created database service.
- `VALIDATE_RUNTIME_CONFIG=1` runs startup configuration checks for both `web` and `worker`; `STRICT_RUNTIME_CONFIG=1` turns warnings into startup failures.

## Cloudflare

For this stack, the cleanest production path is **Cloudflare Tunnel**, not a public origin behind an orange-cloud A record. The reason is simple: this app currently terminates HTTP at Gunicorn, not at Nginx/Caddy/Traefik with an origin certificate.

Recommended setup:

1. Push the version you actually want to deploy.
2. In Cloudflare, create a Tunnel and copy the tunnel token.
3. Set these env vars for the stack:
   - `DJANGO_ALLOWED_HOSTS=your.domain`
   - `DJANGO_CSRF_TRUSTED_ORIGINS=https://your.domain`
   - `DJANGO_USE_X_FORWARDED_HOST=1`
   - `DJANGO_TRUST_PROXY_SSL_HEADER=1`
   - `DJANGO_SECURE_SSL_REDIRECT=1`
   - `DJANGO_SESSION_COOKIE_SECURE=1`
   - `DJANGO_CSRF_COOKIE_SECURE=1`
   - `WEB_BIND=127.0.0.1`
   - `CLOUDFLARE_TUNNEL_TOKEN=<token>`
4. Start or redeploy the stack:

```bash
docker compose --env-file .env up -d --build
```

5. In the Cloudflare Tunnel dashboard, route your hostname to `http://web:8000` if you are using a locally-managed config, or use the remotely managed token flow and attach the public hostname there.

Cloudflare references:
- Proxied DNS records route HTTP/HTTPS through Cloudflare: [How Cloudflare DNS works](https://developers.cloudflare.com/fundamentals/concepts/how-cloudflare-works/)
- Cloudflare recommends proxying web-serving `A`, `AAAA`, and `CNAME` records: [Proxy status](https://developers.cloudflare.com/dns/proxy-status/)
- Tunnel uses outbound-only connections from your origin: [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/)
- Docker tunnel run pattern: [Set up Cloudflare Tunnel](https://developers.cloudflare.com/tunnel/setup/)

If you want a classic reverse-proxy origin instead of Tunnel, add Nginx/Caddy/Traefik in front of Gunicorn, install a Cloudflare Origin CA cert, and use `Full (strict)`:
- [Full (strict)](https://developers.cloudflare.com/ssl/origin-configuration/ssl-modes/full-strict/)
- [Cloudflare Origin CA](https://developers.cloudflare.com/ssl/origin-configuration/origin-ca/)

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
Description writeback uses `PLANNER_DESCRIPTION_WRITEBACK_MAX_RETRIES`. Imported
tasks receive deterministic tags immediately; configured AI enrichment runs as
durable `task_enrichment` jobs.
Jobs also use Postgres leases, so a stopped worker/container can be recovered
by another worker after the lease expires.

Each workspace controls its own automatic refresh rate from **Sync** (default:
12 checks per day). The worker schedules lightweight incremental syncs after
the first full import, with a configurable full refresh interval as a safety
net. `SYNC_INCREMENTAL_LOOKBACK_MINUTES` defaults to 5 to cover updates made
while a sync is running; `AUTO_REFRESH_SCHEDULER_TICK_SECONDS` defaults to 60
and only controls how often the worker evaluates due workspaces.

The cache is shared by the web and worker containers in Docker. Cache keys carry
the workspace cache version, which the worker advances after imported data
changes. This makes old derived views unreachable immediately without making a
cache restart or transient Redis outage block the planner or event views.

Local queue operations:

```bash
python aggregator_project/manage.py queue_health
python aggregator_project/manage.py recover_stale_jobs
python aggregator_project/manage.py retry_failed_writebacks --workspace-id <id>
```

Optionally pass `--since=2025-01-01T00:00:00Z` to limit scope.

## Notes

- Google Fit currently requires client credentials and a refresh token entered in
  its connector form; it is an activity source rather than a task writeback source.
- Connector tokens are encrypted at rest with the stable `ENCRYPTION_KEY` (Fernet).
- Workspace AI settings support either the OpenAI Responses API (`OPENAI_API_KEY`,
  `OPENAI_CHAT_MODEL`) or a reachable OpenAI-compatible Qwen endpoint
  (`AI_QWEN_BASE_URL`, `AI_QWEN_MODEL`, optional `AI_QWEN_API_KEY`). Responses
  requests use `store=false`.
- See `README_PLUGINS.md` for the provider and plugin extension contracts.

## Plugins

Connectors ingest and write back provider data. Plugins are optional workspace tools that operate on that data. Each built-in plugin is discovered from its own folder and can be enabled per workspace from `/plugins/`.

Built-in plugins:

- `SQL Explorer`: runs read-only queries against an isolated SQLite snapshot of the current workspace.
- `Data Chat`: sends a capped workspace task snapshot to the OpenAI Responses API only after a user asks a question; requests use `store=false`.
- `Activity Pulse`: shows cross-provider status, workload, and task-aging views.

A plugin folder owns its manifest, app config, URL routes, views/services, templates, static assets, and tests:

```text
aggregator_project/plugins/example/
  plugin.json
  apps.py
  urls.py
  views.py
  templates/plugins/example/
  static/plugins/
  tests/
```

`plugin.json` declares the Django `AppConfig`. The app config exposes a
`PluginSpec`, and the platform registers routes and navigation only through that
contract. A plugin is enabled independently for each workspace.

## Project Layout

- `aggregator_project/` Django project
- `aggregator_project/core/` settings & utilities
- `aggregator_project/connectors/` provider auth tokens (ConnectorAccount)
- `aggregator_project/ingestion/` sync orchestration, jobs, worker
- `aggregator_project/events/` normalized data model + UI
- `aggregator_project/intelligence/` unified taxonomy, AI backends, chat, and insights
- `aggregator_project/providers/` per-provider apps (client + normalizer)
- `aggregator_project/plugins/` self-contained optional workspace tools
- `aggregator_project/plugin_system/` plugin discovery, activation, and navigation contract

## Tests

```bash
python aggregator_project/manage.py test
```
