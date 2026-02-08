# Operations Runbook

## Local dev
- Copy environment:
  - `cp .env.example .env`
- Boot the app (creates venv, installs deps, ensures DB, migrates, runs server):
  - `./launch_server.sh`

## Database setup
- Ensure DB and run migrations:
  - `python aggregator_project/manage.py ensure_db` (or `python3` if `python` is missing)
  - `python aggregator_project/manage.py migrate` (or `python3` if `python` is missing)

## Syncing
- Sync all sources:
  - `python aggregator_project/manage.py sync_all` (or `python3` if `python` is missing)
- Sync a single source:
  - `python aggregator_project/manage.py sync_source --source=asana` (or `python3` if `python` is missing)
- Limit scope:
  - `python aggregator_project/manage.py sync_source --source=asana --since=2025-01-01T00:00:00Z` (or `python3` if `python` is missing)

## Connector credentials
- Stored in `ConnectorAccount` per workspace.
- For secure storage, set `ENCRYPTION_KEY` before saving credentials.

## Adding a provider
1. Create a provider app under `providers/<name>/`.
2. Implement client + normalizer.
3. Expose a `provider_spec` on the AppConfig.
4. Add the app to `INSTALLED_APPS`.
5. Ensure normalizer emits required `events.Event` fields.
